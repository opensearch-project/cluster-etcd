/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.clusterless.contract.AppliedOutcome;
import org.opensearch.clusterless.contract.Assignment;
import org.opensearch.clusterless.contract.Heartbeat;
import org.opensearch.clusterless.contract.Keys;
import org.opensearch.clusterless.contract.MalformedDocumentException;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.publish.NodePublisher;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.controlplane.store.KeyValueStore;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Reads this node's assignment, works out what to do, and does it — or, with {@code clusterless.apply} false,
 * says what it would have done.
 *
 * <p>Read-only is the default, mirroring the supervisor's own and for the same reason: this is the component that
 * can move data, and the cheapest way to introduce one of those is to have it explain itself for a while where
 * somebody can check the explanation.
 *
 * <h2>Level-triggered, and what that costs</h2>
 *
 * <p>Every pass re-reads the assignment and every index document it names, and rebuilds the intended state from
 * what it read. Nothing here remembers what changed, because a decision derived from a change is a decision
 * that is wrong after a missed one. The only state kept between passes is a copy of the last intended state, and
 * it is used for exactly one thing: deciding whether to log. Convergence does not consult it.
 *
 * <h2>Absent is not empty</h2>
 *
 * <p>The single most destructive misreading available to this class is treating a missing assignment as an empty
 * one, because an empty assignment means "drop every shard you hold". A supervisor that has not written one yet,
 * a pool between owners and a lost key all look exactly like that. So an absent key leaves everything alone and
 * does not even touch the applied ack, and an unparseable one is reported as a failure rather than obeyed.
 *
 * <h2>An assignment version is not ordered across a deletion</h2>
 *
 * <p>Every pass applies what it reads, and <b>nothing here compares an assignment's version with the last one
 * applied.</b> That is deliberate and worth defending, because "skip an assignment that is not newer than what I
 * already applied" is an obvious-looking optimisation that would freeze this node for ever.
 *
 * <p>An assignment's {@code version} counts up from the document already in the store, so deleting the document
 * restarts it at 1 — and the supervisor now reaps a departed node's documents, {@code nodes/assignment/<id>}
 * included. A node reaped while merely unreachable therefore comes back reporting version 11 against a fresh
 * version 1, which under an ordered reading looks like a stale read or a second supervisor writing, and under this
 * one is simply the next instruction. It is the same trap as a store revision restarting after a key is deleted
 * — see {@code WORKING_NOTES.md} — one level up, and the supervisor's own contract had an ordered reading of
 * {@code applied.assignment_version} until it was corrected for exactly this reason.
 */
public final class AssignmentPoller {

    private static final Logger logger = LogManager.getLogger(AssignmentPoller.class);

    private final KeyValueStore store;
    private final NodeId nodeId;
    private final DesiredStateResolver resolver;
    private final Supplier<ClusterState> currentState;
    private final NodeStateFactory nodeStates;
    private final NodePublisher publisher;
    private final ClusterStateApplier applier;
    private final RetainedIndexReaper reaper;
    private final RemoteRecovery remoteRecovery;
    private final CoordinatorViewSource coordinatedShards;
    private final IngestionResets ingestionResets;
    private final StaleReplicas staleReplicas;
    private final boolean applyEnabled;

    /** What the poller needs of the fabricator: everything a pass decides, and nothing it has to decide itself. */
    @FunctionalInterface
    public interface NodeStateFactory {
        NodeState create(
            DesiredState desired,
            Set<Index> stopRetaining,
            Map<ShardId, RecoverySource> recoverySources,
            CoordinatorView coordinated,
            Set<String> rebuild
        );
    }

    private DesiredState lastResolved = DesiredState.nothing();
    private CoordinatorView lastCoordinated = CoordinatorView.nothing();
    private IngestionResets.Outcome lastResets = IngestionResets.Outcome.NOTHING;
    private String lastReport;
    private long assignmentsRead;
    private long statesApplied;
    private String lastFailure;
    private String lastApplyFailure;

    /**
     * @param applier where a fabricated state goes when applying is enabled. May be {@code null} in read-only
     *     mode, which is what makes the read-only path runnable without a node behind it.
     */
    public AssignmentPoller(
        KeyValueStore store,
        NodeId nodeId,
        DesiredStateResolver resolver,
        Supplier<ClusterState> currentState,
        NodeStateFactory nodeStates,
        NodePublisher publisher,
        ClusterStateApplier applier,
        RetainedIndexReaper reaper,
        boolean applyEnabled
    ) {
        this(store, nodeId, resolver, currentState, nodeStates, publisher, applier, reaper, null, applyEnabled);
    }

    /**
     * @param remoteRecovery looks up where a remote-backed primary should recover from, once per pass. May be
     *     {@code null}, which is the same as a node with no remote store: nothing is looked up and the fabricator
     *     keeps its own answer.
     */
    public AssignmentPoller(
        KeyValueStore store,
        NodeId nodeId,
        DesiredStateResolver resolver,
        Supplier<ClusterState> currentState,
        NodeStateFactory nodeStates,
        NodePublisher publisher,
        ClusterStateApplier applier,
        RetainedIndexReaper reaper,
        RemoteRecovery remoteRecovery,
        boolean applyEnabled
    ) {
        this(store, nodeId, resolver, currentState, nodeStates, publisher, applier, reaper, remoteRecovery,
            CoordinatorViewSource.NONE, applyEnabled);
    }

    /**
     * @param coordinatedShards where the shards of coordinated tenants are. Read once per pass, like everything
     *     else the fabrication is given: it is several reads of the store, and the fabricator runs twice on a pass
     *     that applies.
     */
    public AssignmentPoller(
        KeyValueStore store,
        NodeId nodeId,
        DesiredStateResolver resolver,
        Supplier<ClusterState> currentState,
        NodeStateFactory nodeStates,
        NodePublisher publisher,
        ClusterStateApplier applier,
        RetainedIndexReaper reaper,
        RemoteRecovery remoteRecovery,
        CoordinatorViewSource coordinatedShards,
        boolean applyEnabled
    ) {
        this(store, nodeId, resolver, currentState, nodeStates, publisher, applier, reaper, remoteRecovery,
            coordinatedShards, null, applyEnabled);
    }

    /**
     * @param ingestionResets carries out the consumer-pointer resets an assignment asks for. May be {@code null},
     *     which is a node that acts on none — the right default, since a reset it cannot carry out is one it must
     *     not report as done.
     */
    public AssignmentPoller(
        KeyValueStore store,
        NodeId nodeId,
        DesiredStateResolver resolver,
        Supplier<ClusterState> currentState,
        NodeStateFactory nodeStates,
        NodePublisher publisher,
        ClusterStateApplier applier,
        RetainedIndexReaper reaper,
        RemoteRecovery remoteRecovery,
        CoordinatorViewSource coordinatedShards,
        IngestionResets ingestionResets,
        boolean applyEnabled
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.currentState = Objects.requireNonNull(currentState, "currentState");
        this.nodeStates = Objects.requireNonNull(nodeStates, "nodeStates");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.applier = applier;
        this.reaper = Objects.requireNonNull(reaper, "reaper");
        this.ingestionResets = ingestionResets;
        this.staleReplicas = new StaleReplicas(nodeId.value());
        this.remoteRecovery = remoteRecovery;
        this.coordinatedShards = coordinatedShards == null ? CoordinatorViewSource.NONE : coordinatedShards;
        this.applyEnabled = applyEnabled;
        if (applyEnabled && applier == null) {
            throw new IllegalArgumentException("applying is enabled but no applier was given");
        }
    }

    /**
     * One pass. Never throws, for the reason the publishing pass does not: an exception out of a scheduled task
     * stops it being rescheduled on some executors, and a node that quietly stopped reading its assignment would
     * look healthy while ignoring every instruction it was given.
     */
    public synchronized void poll() {
        try {
            Optional<KeyValueStore.Versioned> stored = store.get(Keys.nodeAssignment(nodeId));
            if (stored.isEmpty()) {
                // No instruction. Not an empty desired state -- see the class comment -- so nothing is
                // resolved, nothing is reported, and the last applied ack stands.
                logger.debug("clusterless: no assignment for this node yet");
                lastFailure = null;
                return;
            }

            Assignment assignment;
            try {
                assignment = Assignment.read(stored.get().value());
            } catch (MalformedDocumentException e) {
                // Reported, and changes nothing. Reading a parse failure as an empty assignment would let one
                // corrupt write empty a node.
                lastFailure = e.getMessage();
                logger.error("clusterless: {}; ignoring it and keeping the last assignment", e.getMessage());
                publisher.reportApplied(
                    new Heartbeat.Applied(lastResolved.assignmentVersion(), AppliedOutcome.FAILED, e.getMessage())
                );
                return;
            }

            assignmentsRead++;
            lastFailure = null;
            DesiredState desired = resolver.resolve(assignment);
            lastResolved = desired;

            ClusterState live = currentState.get();
            // Once per pass, before anything is fabricated. The fabricator is called twice on a pass that applies,
            // so a counter advanced from inside it would reach its threshold in half the intended time.
            Set<Index> stopRetaining = reaper.reapable(live, desired);

            // Also once per pass, and for the same reason plus a stronger one: this one reads a repository over a
            // network. It can reduce the desired state -- a copy whose recovery source cannot be established is
            // declined rather than built on a guess -- so what it returns is what everything downstream uses.
            RemoteRecovery.Plan recovery = remoteRecovery == null
                ? RemoteRecovery.noOpinion(desired)
                : remoteRecovery.planFor(desired, live);
            desired = recovery.desired();
            lastResolved = desired;

            // Where other nodes' copies are: every shard of a coordinated tenant's indices, and the peers of this
            // node's own shards. Read here for the same reason as everything above -- the fabrication is a pure
            // function and this is several reads of the store -- and read once for both, since it is one scan per
            // tenant either way.
            CoordinatorView coordinated = coordinatedShards.read(desired);
            lastCoordinated = coordinated;
            // The second step that can reduce the desired state, and it has to come after the view: a
            // document-replication replica cannot be built unless its primary will be in the routing table, because
            // RoutingNodes asserts a primary exists for every shard and an assertion on the applier thread exits the
            // node rather than declining a copy. See PeerRecovery.
            desired = PeerRecovery.reduce(desired, coordinated, live);
            // And a term that went backwards, which also needs the live state: a copy already running at a higher
            // term is not rebuilt at a lower one, it is declined with a reason. See PrimaryTerms for why saying so
            // matters more than the server's own refusal would.
            desired = PrimaryTerms.reduce(desired, live);
            lastResolved = desired;
            // Once per pass, because it counts passes: which started docrep replicas here their primary no longer holds
            // in sync, and so are rebuilt. See StaleReplicas.
            Set<String> rebuild = staleReplicas.toRebuild(desired, live, coordinated);
            // Fabricated against the live state to decide whether anything needs doing, and fabricated again
            // inside the applier against whatever the server hands over. The two can differ -- a shard may have
            // finished recovering in between -- and the applier's copy is the one that counts.
            ClusterState intended = nodeStates.create(desired, stopRetaining, recovery.sources(), coordinated, rebuild)
                .buildClusterState(live);

            if (StateEquivalence.sameContent(live, intended)) {
                // The fixed point. Applying here would publish a state identical to the running one except for
                // its version, which costs every applier and listener in the server a pass and says nothing.
                logger.debug(
                    "clusterless: already holding what assignment version {} asks for; nothing to apply",
                    desired.assignmentVersion()
                );
            } else {
                report(intended, desired);
                DesiredState finalDesired = desired;
                if (applyEnabled) {
                    applier.apply(
                        "clusterless assignment version " + desired.assignmentVersion(),
                        previous -> nodeStates.create(finalDesired, stopRetaining, recovery.sources(), coordinated, rebuild)
                            .buildClusterState(previous),
                        this::applied
                    );
                }
            }

            // After the state, and against the live one rather than the intended one. A reset needs a started
            // primary with an ingestion engine behind it, which the state applied a moment ago does not yet have
            // -- so the ordinary first pass finds the shard not ready, reports applying, and the pass after does
            // it. Level-triggered, like everything else: the reset stays in the assignment until acknowledged.
            //
            // The desired state goes in too, and it is not decoration: applying is asynchronous, so the live
            // state here can still be the one from before this pass's publication. The pause state a reset lands
            // on has to come from the document, or a resume carrying a reset leaves the poller stopped.
            lastResets = ingestionResets == null || applyEnabled == false
                ? IngestionResets.Outcome.NOTHING
                : ingestionResets.apply(assignment, desired, currentState.get());

            publisher.reportApplied(ackFor(desired));
        } catch (Exception e) {
            lastFailure = e.getClass().getSimpleName() + ": " + e.getMessage();
            logger.warn("clusterless: could not read this node's assignment; keeping the last one", e);
        }
    }

    /**
     * What to tell the supervisor.
     *
     * <p>In read-only mode the honest answer is {@code failed}, and the alternatives are both worse.
     * {@code applied} would be a lie the supervisor acts on — it would stop expecting anything to change while
     * this node did nothing. A permanent {@code applying} would imply progress that is not coming. {@code failed}
     * with a reason produces a finding, which is exactly what a node that has been told to hold shards and is
     * configured not to should produce.
     *
     * <p>The version is still echoed, because "which assignment this refers to" is useful in every case.
     */
    private Heartbeat.Applied ackFor(DesiredState desired) {
        if (applyEnabled == false) {
            String detail = "read-only: [clusterless.apply] is false, so nothing was applied"
                + (desired.deferred().isEmpty() ? "" : "; " + desired.deferred().size() + " copy(ies) would also have been deferred");
            return new Heartbeat.Applied(desired.assignmentVersion(), AppliedOutcome.FAILED, detail);
        }
        if (lastApplyFailure != null) {
            return new Heartbeat.Applied(desired.assignmentVersion(), AppliedOutcome.FAILED, lastApplyFailure);
        }

        // Measured from the live state rather than from the fact that an update was submitted. Every new copy
        // is INITIALIZING at the moment it is applied, so "the state went in" and "the node holds what it was
        // told to" are different claims and only the second one is worth making.
        Convergence convergence = Convergence.assess(desired, currentState.get(), nodeId.value());
        List<String> notBuilt = desired.deferred();

        // A reset the supervisor is waiting on is part of what "applied" means, and this is where that is
        // enforced. The supervisor completes the task on the version this node reports, so an outstanding reset
        // reported as applied would tell it a pointer had moved when nothing had moved it -- and in that one case
        // nothing downstream can ever contradict the claim. Refusals come first: they are not going to resolve.
        if (lastResets.refused().isEmpty() == false) {
            String refused = lastResets.refused().size() + " reset(s) not carried out: " + lastResets.refused().get(0);
            return new Heartbeat.Applied(desired.assignmentVersion(), AppliedOutcome.FAILED, refused);
        }

        if (convergence.isComplete() && notBuilt.isEmpty() && lastResets.outstanding().isEmpty()) {
            return new Heartbeat.Applied(desired.assignmentVersion(), AppliedOutcome.APPLIED, null);
        }
        if (convergence.isComplete() && notBuilt.isEmpty() && lastResets.outstanding().isEmpty() == false) {
            // Every copy is where it should be and a reset has not landed yet -- the ordinary case on the pass a
            // reset arrives, since the shard has to be started before its consumer can be moved.
            return new Heartbeat.Applied(
                desired.assignmentVersion(),
                AppliedOutcome.APPLYING,
                lastResets.outstanding().size() + " reset(s) outstanding: " + lastResets.outstanding().get(0)
            );
        }

        List<String> nothingServing = lastCoordinated.indicesWithNothingServing();
        if (convergence.isComplete() && notBuilt.isEmpty() && nothingServing.isEmpty() == false) {
            // Every copy this node was told to hold is serving, nothing was declined, and it still cannot answer a
            // query for an index it coordinates -- because every copy of it is on a node this node cannot reach or
            // has not started. APPLYING rather than FAILED: the copies exist somewhere and the projection is
            // catching up, so this is the fleet converging rather than this node refusing. Reported at all because
            // the alternative is a coordinator that looks perfectly healthy and answers with nothing.
            return new Heartbeat.Applied(
                desired.assignmentVersion(),
                AppliedOutcome.APPLYING,
                "coordinating " + nothingServing.size() + " index(es) with no serving copy reachable: " + nothingServing.get(0)
            );
        }

        String detail = convergence.detail();
        if (notBuilt.isEmpty() == false) {
            // Copies this node declined are not something more time will fix, so they are reported as a failure
            // even while the rest is still converging. The supervisor should see that part as stuck.
            String declined = notBuilt.size() + " copy(ies) not built: " + notBuilt.get(0);
            return new Heartbeat.Applied(
                desired.assignmentVersion(),
                AppliedOutcome.FAILED,
                detail == null ? declined : detail + "; " + declined
            );
        }
        return new Heartbeat.Applied(desired.assignmentVersion(), AppliedOutcome.APPLYING, detail);
    }

    /**
     * The applier's callback, which arrives on the server's thread rather than a pass's.
     *
     * <p>Only records. The next pass computes the ack from the live state, so there is nothing to publish from
     * here — and publishing from here would write a heartbeat from an unexpected thread, out of order with the
     * pass that owns that document.
     */
    private synchronized void applied(String source, Exception failure) {
        if (failure == null) {
            statesApplied++;
            lastApplyFailure = null;
            logger.info("clusterless: applied {}", source);
        } else {
            lastApplyFailure = "could not apply " + source + ": " + failure;
            logger.error("clusterless: could not apply {}", source, failure);
        }
    }

    /**
     * Logs the intended state, at {@code INFO}, when there is something to say.
     *
     * <p>Only reached when the intended state differs from what is applied, which is what makes the line worth
     * reading: it appears when this node is about to change something. It used to be guarded by comparing the
     * rendered report with the last one, which did not work — the report names the cluster state version, so it
     * differed every pass and logged several hundred bytes per index per second. Comparing states rather than
     * their descriptions is both correct and the thing that decides whether to apply, so there is now one
     * decision instead of two that could disagree.
     *
     * <p>The last report is still remembered, for the one case the state comparison cannot see: in read-only
     * mode nothing is ever applied, so every pass differs from the live state for ever. Without this the
     * read-only node would be the noisy one.
     */
    private void report(ClusterState intended, DesiredState desired) {
        String report = ClusterStateReport.describe(intended, desired);
        String withoutVersion = report.substring(report.indexOf(','));
        if (withoutVersion.equals(lastReport)) {
            logger.debug("clusterless: intended state unchanged at assignment version {}", desired.assignmentVersion());
            return;
        }
        lastReport = withoutVersion;
        if (applyEnabled) {
            logger.info("clusterless: applying\n{}", report);
        } else {
            logger.info("clusterless: read-only, so the following was NOT applied\n{}", report);
        }
    }

    /** What this node last read, for the local diagnostic D7.3 asks for. */
    public synchronized PollStatus status() {
        return new PollStatus(
            applyEnabled,
            assignmentsRead,
            statesApplied,
            lastResolved.assignmentVersion(),
            lastResolved.indices().size(),
            lastResolved.copyCount(),
            lastResolved.coordinatorTenants().stream().map(Object::toString).toList(),
            join(lastResolved.deferred(), lastCoordinated.deferred()),
            lastCoordinated.indices().size(),
            lastCoordinated.servingCopies(),
            lastFailure,
            lastApplyFailure
        );
    }

    private static List<String> join(List<String> first, List<String> second) {
        if (second.isEmpty()) {
            return first;
        }
        List<String> both = new java.util.ArrayList<>(first);
        both.addAll(second);
        return List.copyOf(both);
    }

    /**
     * A snapshot of the reading half of a pass.
     *
     * <p>{@code deferred} is the field worth having: "the supervisor asked for a shard and this node did not
     * build it" is otherwise invisible from both ends — the store shows an unassigned shard and the node shows
     * nothing at all.
     */
    public record PollStatus(
        boolean applyEnabled,
        long assignmentsRead,
        long statesApplied,
        long assignmentVersion,
        int indices,
        int copies,
        List<String> coordinatorTenants,
        List<String> deferred,
        int coordinatedIndices,
        int coordinatedServingCopies,
        String lastFailure,
        String lastApplyFailure
    ) {}
}
