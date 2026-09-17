/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.contract.Assignment;
import org.opensearch.clusterless.contract.IngestionReset;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Carrying out the consumer-pointer resets an assignment asks for, exactly once each.
 *
 * <p>This is the only thing in this plugin that acts on an <em>instruction</em> rather than converging towards
 * desired state, and everything awkward about it follows from that. {@code DESIGN.md} §21.2 and
 * {@code NODE_CONTRACT.md} carry the agreed contract; what matters here is the three obligations it puts on this
 * side, each of which has a plausible wrong answer.
 *
 * <p><b>1. Pause the poller first, and only then land on the document's pause state.</b> Upstream's
 * {@code IngestionEngine.resetStreamPoller} throws {@code IllegalStateException} on a poller that is not paused,
 * and a node cannot assume the pause has already been applied — quite the opposite. The resume and the reset are
 * two halves of one operator action that travel paths of very different lengths:
 *
 * <ul>
 *   <li>{@code ingestion_status.is_paused} is desired state, in a document this node already reads every pass.
 *       It converges on the next one, unconditionally.
 *   <li>the reset is an instruction: observed by the supervisor, planned into an assignment, written, read.
 *       Several passes at best.
 * </ul>
 *
 * <p>The fast path is the wrong one, so <b>the ordinary case is a poller that has already resumed</b> and a reset
 * that would then throw. That is the normal ordering rather than a race to narrow, and it is why this applies the
 * reset in two calls: pause, then reset-and-land-on-the-desired-state. Upstream's own
 * {@code updateIngestionSettings} applies a reset <em>before</em> its pause flag, so one call carrying both would
 * still run the reset against a live poller. A node that trusted the ordering would work in every test where the
 * reset happened to arrive first.
 *
 * <p><b>2. {@code applied} has to mean the reset too.</b> The supervisor completes the task when the version this
 * node reports equals the version in its assignment, so that report is the entire acknowledgement. A reset that is
 * outstanding therefore has to hold the ack at {@code applying}, and one that cannot be carried out has to make it
 * {@code failed} — see {@link Outcome}.
 *
 * <p><b>3. Idempotence keys on the reset's id, never on the assignment version</b>, which moves whenever anything
 * else in the document does. {@link #applied} is that memory, and it is in memory deliberately: after a reset the
 * new start pointer is force-flushed into the Lucene commit, so a copy that moves resumes from the reset point
 * with no instruction surviving anywhere. Nothing needs a durable ledger.
 *
 * <p><b>The one hole, stated because it is real:</b> a restart between applying a reset and the supervisor
 * observing the acknowledgement loses this memory, and the reset would be applied a second time — rewinding the
 * consumer by whatever it had consumed in between. It is bounded by one supervisor pass and it re-consumes rather
 * than loses, because every ingested message must carry {@code _id} ({@code MessageProcessorRunnable} rejects one
 * that does not), so a replay re-indexes the same documents rather than duplicating them. The exception worth
 * knowing is a stream of {@code _op_type: create} messages, where a replay is a version conflict per document and
 * the error policy decides whether that drops or blocks.
 */
public final class IngestionResets {

    /**
     * The seam. Everything above it is a pure function of (assignment, cluster state, what has been applied); this
     * is the part that touches a running shard, and it cannot be exercised in a unit test because it needs an
     * {@code IngestionEngine} behind a started primary.
     */
    public interface Pollers {

        /** Whether a shard is in a state where a reset could be applied at all. */
        enum Readiness {

            /** Started, primary, and fed by an ingestion source. */
            READY,

            /** Not started yet, or gone. Nothing is wrong; the next pass tries again. */
            NOT_YET,

            /** Started, but this index has no ingestion source, so there is no consumer to move. */
            NOT_INGESTING
        }

        Readiness readinessOf(ShardId shard);

        /**
         * Pause this shard's poller, move its consumer, then leave it paused or running as asked.
         *
         * @param landPaused what {@code ingestion_status.is_paused} says the index should end up as, which is
         *     <em>not</em> the state the poller was in when this was called
         * @throws Exception if the reset could not be applied, in which case nothing is recorded and the next
         *     pass tries again
         */
        void pauseThenReset(ShardId shard, IngestionReset.Mode mode, String value, boolean landPaused) throws Exception;

        /** A node that holds no shards, which is what a fabrication test wants. */
        Pollers NONE = new Pollers() {
            @Override
            public Readiness readinessOf(ShardId shard) {
                return Readiness.NOT_YET;
            }

            @Override
            public void pauseThenReset(ShardId shard, IngestionReset.Mode mode, String value, boolean landPaused) {
                throw new IllegalStateException("this node holds no pollers");
            }
        };
    }

    /** One reset of one shard, resolved against the cluster state and ready to carry out. */
    public record Action(ShardId shard, String resetId, IngestionReset.Mode mode, String value, boolean landPaused) {}

    /**
     * What this pass found, in the two forms the heartbeat needs.
     *
     * @param actions resets to carry out now
     * @param outstanding resets that could not be carried out <em>yet</em>, which hold the ack at
     *     {@code applying} — a shard still recovering is the ordinary case
     * @param refused resets this node will never carry out, which make the ack {@code failed}. A reset for a
     *     shard this node is not the primary of, or in a mode it does not know: both are the supervisor or the
     *     control plane saying something this node cannot act on, and silence would be reported upstream as a
     *     pointer that moved when nothing moved it.
     */
    public record Outcome(List<Action> actions, List<String> outstanding, List<String> refused) {

        public Outcome {
            actions = actions == null ? List.of() : List.copyOf(actions);
            outstanding = outstanding == null ? List.of() : List.copyOf(outstanding);
            refused = refused == null ? List.of() : List.copyOf(refused);
        }

        static final Outcome NOTHING = new Outcome(List.of(), List.of(), List.of());

        /** Whether the ack may say {@code applied}, which it may only when no reset is unaccounted for. */
        public boolean isSettled() {
            return outstanding.isEmpty() && refused.isEmpty();
        }
    }

    private final Pollers pollers;

    /**
     * The last reset id applied per shard, which is what makes this idempotent.
     *
     * <p>Keyed on {@link ShardId}, whose equality includes the index uuid — so an index deleted and recreated
     * under the same name does not inherit its predecessor's applied resets, which would be the torn-read
     * problem arriving in a new place.
     */
    private final Map<ShardId, String> applied = new LinkedHashMap<>();

    private final List<String> lastFailures = new ArrayList<>();

    public IngestionResets(Pollers pollers) {
        this.pollers = Objects.requireNonNull(pollers, "pollers");
    }

    /**
     * Decide what to do, without doing any of it.
     *
     * <p>Separated from {@link #apply} so that every rule above is testable without a running shard: which resets
     * are outstanding, which are refused, and what the pause state to land on is.
     */
    public Outcome plan(Assignment assignment, ClusterState state) {
        List<Action> actions = new ArrayList<>();
        List<String> outstanding = new ArrayList<>();
        List<String> refused = new ArrayList<>();

        assignment.tenants().forEach((tenant, tenantAssignment) -> {
            tenantAssignment.indices().forEach((uuid, index) -> {
                IngestionReset reset = index.reset();
                if (reset == null) {
                    return;
                }
                reset.shards().forEach((shard, pointer) -> {
                    String what = describe(tenant, uuid, shard, reset.id());

                    // The supervisor writes a reset only into the assignment of the node holding that shard's
                    // primary, so this is that promise checked rather than assumed. A search replica has no
                    // consumer to move, and applying to the wrong copy would report a pointer moved on a shard
                    // whose primary never heard of it.
                    if (index.shards().get(shard) != ShardRole.PRIMARY) {
                        refused.add(what + ": this node is not assigned that shard's primary");
                        return;
                    }
                    if (pointer.mode() == IngestionReset.Mode.UNKNOWN) {
                        // Reported rather than skipped. Nothing else would make it visible: the supervisor
                        // completes the task on this node's applied, so a mode dropped quietly is a pointer
                        // reported as moved by something that could not move it.
                        refused.add(what + ": mode is not one this node understands, so the pointer was not moved");
                        return;
                    }

                    IndexMetadata metadata = metadataOf(state, uuid);
                    if (metadata == null) {
                        outstanding.add(what + ": the index is not in this node's state yet");
                        return;
                    }
                    ShardId shardId = new ShardId(metadata.getIndex(), shard);

                    if (reset.id().equals(applied.get(shardId))) {
                        // Already carried out. Still nothing to report: the ack says applied, which is what
                        // lets the supervisor complete the task and stop writing this.
                        return;
                    }

                    switch (pollers.readinessOf(shardId)) {
                        case NOT_YET -> outstanding.add(what + ": the shard is not started here yet");
                        case NOT_INGESTING -> refused.add(
                            what + ": the index has no ingestion source, so there is no consumer to move"
                        );
                        case READY -> actions.add(
                            new Action(
                                shardId,
                                reset.id(),
                                pointer.mode(),
                                pointer.value(),
                                // What the index document asks for, not what the poller is doing now. The
                                // resume very likely arrived several passes ago.
                                metadata.getIngestionStatus().isPaused()
                            )
                        );
                    }
                });
            });
        });

        if (actions.isEmpty() && outstanding.isEmpty() && refused.isEmpty()) {
            return Outcome.NOTHING;
        }
        return new Outcome(actions, outstanding, refused);
    }

    /**
     * Carry out a plan, and report what is still not done.
     *
     * <p>A failure is recorded and retried rather than remembered as done, which is the safe direction: an
     * unapplied reset holds the supervisor's task open, where a wrongly-recorded one would let it complete
     * having moved nothing.
     */
    public Outcome apply(Assignment assignment, ClusterState state) {
        Outcome planned = plan(assignment, state);
        if (planned.actions().isEmpty()) {
            return planned;
        }

        List<String> failures = new ArrayList<>();
        for (Action action : planned.actions()) {
            try {
                pollers.pauseThenReset(action.shard(), action.mode(), action.value(), action.landPaused());
                applied.put(action.shard(), action.resetId());
            } catch (Exception e) {
                failures.add(
                    action.shard() + " reset [" + action.resetId() + "]: " + e.getClass().getSimpleName() + ": " + e.getMessage()
                );
            }
        }

        lastFailures.clear();
        lastFailures.addAll(failures);
        if (failures.isEmpty()) {
            // Everything planned was carried out, so what remains is whatever could not be planned.
            return new Outcome(List.of(), planned.outstanding(), planned.refused());
        }
        List<String> stillOutstanding = new ArrayList<>(planned.outstanding());
        stillOutstanding.addAll(failures);
        return new Outcome(List.of(), stillOutstanding, planned.refused());
    }

    /** Reset failures from the last pass, for the status endpoint. */
    public List<String> lastFailures() {
        return List.copyOf(lastFailures);
    }

    /** How many resets this node has carried out, for the status endpoint. */
    public int appliedCount() {
        return applied.size();
    }

    private static IndexMetadata metadataOf(ClusterState state, String uuid) {
        for (IndexMetadata index : state.metadata()) {
            if (index.getIndexUUID().equals(uuid)) {
                return index;
            }
        }
        return null;
    }

    private static String describe(TenantId tenant, String uuid, int shard, String resetId) {
        return "reset [" + resetId + "] of " + tenant + "/" + uuid + "/" + shard;
    }

    /**
     * Whether a routing entry is a started primary, which is the only copy a reset may be applied to.
     *
     * <p>Used by the real {@link Pollers} rather than here, and kept in this class because the rule belongs with
     * everything else about resets: {@link ShardRole#of} reads the search-only flag first, so a search replica is
     * never mistaken for a primary.
     */
    public static boolean isResettableCopy(ShardRouting routing) {
        return routing != null && routing.started() && ShardRole.of(routing) == ShardRole.PRIMARY;
    }

    /** The index this node holds under a uuid, or null. Shared with the real {@link Pollers}. */
    public static Index indexOf(ClusterState state, String uuid) {
        IndexMetadata metadata = metadataOf(state, uuid);
        return metadata == null ? null : metadata.getIndex();
    }
}
