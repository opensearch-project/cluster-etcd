package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.OutcomeUnknownException;
import org.opensearch.controlplane.store.RevisionConflictException;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.projection.ReapingStatus;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Finding;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Liveness;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Deletes the four documents a departed node leaves behind — the thing {@code DESIGN.md} §20.2 declined to do.
 *
 * <p>It declined for a good reason, and the reason is gone rather than forgotten. Three of the four keys are
 * written by the node, and the whole key layout rests on one writer per key, so reaping means deleting keys whose
 * writer is <em>presumed</em> gone rather than known to be. The specific hazard: a node partitioned from the store
 * for longer than any grace period is indistinguishable from a dead one, and deleting its registration while it is
 * merely unreachable leaves it heartbeating with no registration — {@code NODE_UNREGISTERED}, placeable for
 * nothing, needing a restart. Trading a slow leak for a node that strands itself is not an improvement.
 *
 * <p>What changed is the contract: <b>a node now rewrites its registration when it observes the document
 * missing</b>, with the same {@code ephemeral_id} and the same {@code registered_at}, so a wrong reap costs one
 * pass and heals itself. That makes reaping safe by construction.
 *
 * <p><b>But not safe to assume.</b> A node that does not implement the rewrite rule is still conformant with
 * everything else in the contract, so this cannot be built on the assumption that every node self-heals — which
 * is exactly why the reap waits for an aged {@code NODE_LOST} rather than acting the moment a node looks gone.
 * The ageing is the protection for the node that does not heal.
 */
public final class NodeReaper {

    /**
     * How long {@code NODE_LOST} must have held, on top of the silence that raised it.
     *
     * <p>The same figure as {@code OrphanReaper}'s patience, and for the same reason: both delete something on
     * the strength of an inference, and both are calibrated not for a torn read — which one more pass settles —
     * but for a human noticing that something is systematically wrong before it becomes irreversible. A node is
     * already twenty heartbeat intervals silent before the finding exists at all.
     *
     * <p>It is also sized so that an ordinary restart or deployment does not lose a node its documents, which is
     * the wrong job for it to be doing: a restart is a <em>declared</em> few minutes and being dead is an
     * undeclared forever, and one number cannot be right for both. {@code DESIGN.md} §24 is what would separate
     * them, and this figure could drop a long way once it did.
     */
    public static final long REAP_AFTER_LOST_PASSES = 30;

    /** A node whose documents are gone, and which keys went. */
    public record Reaped(NodeId node, List<String> keys, String because) {}

    /** A node that looks gone and was kept anyway, with the reason an operator will want. */
    public record Kept(NodeId node, String because) {}

    public record Report(List<Reaped> reaped, List<Kept> kept) {

        public static final Report NOTHING = new Report(List.of(), List.of());

        public boolean didAnything() {
            return reaped.isEmpty() == false;
        }
    }

    /** One copy, as either of the two records of it names it. */
    record Copy(TenantId tenant, String indexUuid, int shard) {}

    /** A copy only this node can supply, and why. */
    record Owed(Copy copy, String because) {}

    private final KeyValueStore store;
    private final LivenessTracker liveness;

    public NodeReaper(KeyValueStore store, LivenessTracker liveness) {
        this.store = store;
        this.liveness = liveness;
    }

    /**
     * Projects what a reap did, so the most destructive action here can be reviewed after the fact.
     *
     * <p>The {@code kept} half is the more useful of the two and the reason this is not merely an audit log: a node
     * that keeps being declined means something is pinned that should not be — most often a shard whose only
     * started copy is on a machine nobody can reach — and declining to act produces no finding, so that state is
     * otherwise entirely silent.
     *
     * <p>Written only when the content changes, like every other projection, so a pool that reaps nothing writes
     * nothing. A pool that has never reaped anything has no document at all, which is the honest difference between
     * "nothing to say" and "not running".
     */
    public ReapingStatus statusOf(Report report, PoolId pool, String holder, long pass, long observedAt) {
        return new ReapingStatus(
            pass, holder, observedAt,
            report.reaped().size(), report.kept().size(),
            report.reaped().stream().limit(ReapingStatus.MAX_SHOWN)
                .map(entry -> new ReapingStatus.Reaped(entry.node().value(), entry.keys(), entry.because()))
                .toList(),
            report.kept().stream().limit(ReapingStatus.MAX_SHOWN)
                .map(entry -> new ReapingStatus.Kept(entry.node().value(), entry.because()))
                .toList()
        );
    }

    public Report reap(Diff diff, Observation observation) {
        List<Reaped> reaped = new ArrayList<>();
        List<Kept> kept = new ArrayList<>();

        for (Finding finding : diff.of(FindingKind.NODE_LOST)) {
            NodeId nodeId = finding.subject().node();
            if (nodeId == null || finding.passes() < REAP_AFTER_LOST_PASSES) {
                continue;
            }
            Observation.NodeObservation node = observation.nodes().get(nodeId);
            if (node == null || node.liveness() != Liveness.LOST) {
                continue;
            }

            // A deployment's say first. A frozen node is left exactly as it is, and a node under a returning drain is
            // expected back: its documents are what it comes back to (DESIGN.md §29.3). A node being removed is
            // reapable, once it owes nothing.
            var covered = org.opensearch.supervisor.model.deploy.DeploymentCoverage.ofNode(observation, nodeId);
            if (covered != null && covered.frozen()) {
                kept.add(new Kept(nodeId, "frozen by a deployment that cannot be acted on as written, and a frozen node"
                    + " is not reaped"));
                continue;
            }
            if (covered != null && covered.draining() && covered.removing() == false) {
                kept.add(new Kept(nodeId, "under drain [" + covered.id() + "], whose nodes are expected back by its"
                    + " until; it is reaped only once the drain has expired and the node is still gone"));
                continue;
            }

            // Everything the node still owes, which is the one thing that makes this destructive rather than
            // tidy. A reaped node that comes back has no assignment, and an assignment's absence means "hold
            // nothing" -- so reaping a node whose copy the pool never managed to replace would turn the operator
            // action of bringing a machine back into the deletion of the last copy of a shard.
            Owed owed = copyOnlyThisNodeHas(node, observation);
            if (owed != null) {
                kept.add(new Kept(nodeId, owed.because()));
                continue;
            }

            Long witness = liveness.heartbeatRevisionOf(nodeId);
            List<String> deleted = new ArrayList<>();

            if (witness != null) {
                // The heartbeat goes first, under compare-and-set, and it is the gate for everything after it.
                // A node that wrote a heartbeat between the observation this decision came from and this moment
                // fails the exchange and keeps every one of its documents.
                //
                // The conflict arrives as an exception rather than as `false`, which is the store's convention
                // everywhere else here -- and getting that wrong is not a cosmetic mistake: it would throw out of
                // the pass, so the one event this gate exists to handle would break reconciliation for the whole
                // pool instead of being handled.
                try {
                    store.delete(Keys.nodeHeartbeat(nodeId), witness);
                } catch (RevisionConflictException e) {
                    kept.add(new Kept(nodeId, "its heartbeat changed since it was observed, so it is not gone"
                        + " after all and nothing was deleted"));
                    continue;
                } catch (OutcomeUnknownException e) {
                    // The gate is not known to have opened, so nothing behind it is deleted. If the heartbeat did
                    // go, the node reads as registered and silent, and is judged again from there.
                    kept.add(new Kept(nodeId, "the delete of its heartbeat lost its answer, so nothing else was"
                        + " deleted; it is decided again from what the next pass observes"));
                    continue;
                }
                deleted.add(Keys.nodeHeartbeat(nodeId));
            }

            // Then the rest, registration last. A crash part-way through leaves a registration with no
            // heartbeat, which the observer already handles as "registered and has never spoken" -- a grace
            // period and then LOST again. The reverse order would leave a heartbeat with no registration, which
            // is the exact state this whole design is trying not to create.
            for (String key : List.of(Keys.nodeShards(nodeId), Keys.nodeAssignment(nodeId),
                Keys.nodeRegistration(nodeId))) {
                if (store.delete(key)) {
                    deleted.add(key);
                }
            }

            reaped.add(new Reaped(nodeId, List.copyOf(deleted),
                "lost for " + finding.passes() + " passes and holding no copy this pool needs, so its"
                    + " documents were deleted; if it is merely unreachable it rewrites its registration when it"
                    + " sees it missing"));
        }

        return reaped.isEmpty() && kept.isEmpty() ? Report.NOTHING : new Report(reaped, kept);
    }

    /**
     * A copy this node has and nobody else can supply, or null if the pool can spare it entirely.
     *
     * <p>Reads both records of what a node has — what it last reported holding, and what it was last told to
     * hold — because either on its own is a way to be wrong here. A node that was assigned a copy and died before
     * reporting it has no inventory entry for it; a node holding a copy the supervisor has already withdrawn has
     * no assignment entry for it.
     */
    static Owed copyOnlyThisNodeHas(Observation.NodeObservation node, Observation observation) {
        for (Copy copy : copiesOf(node)) {
            Observation.DesiredState desired = observation.tenants().get(copy.tenant());
            if (desired == null) {
                // Not this pool's tenant any more, so not this pool's judgement to make. Whoever owns it now
                // will have placed what it needs.
                continue;
            }
            String shard = "index uuid " + copy.indexUuid() + " shard " + copy.shard() + " for tenant ["
                + copy.tenant().value() + "]";
            IndexConfig index = desired.indicesByUuid().get(copy.indexUuid());
            if (index == null) {
                List<String> unreadable = observation.unreadableIndicesOf(copy.tenant());
                if (unreadable.isEmpty() == false && startedElsewhere(copy, node.nodeId(), observation) == false) {
                    // Absent, but perhaps only unparseable: the document is keyed by name, so nothing says which
                    // index an unreadable one is.
                    return new Owed(copy, "holds " + shard + ", absent from desired state while " + unreadable
                        + " cannot be read, and no other node has a started copy; it may be one of them");
                }
                // Nobody wants this copy, so losing it costs nothing. A deleted index leaves exactly this, and
                // holding a node's documents for ever on account of a copy of something that does not exist
                // would make the graveyard permanent.
                continue;
            }
            if (index.open() == false) {
                // Dormant, not unwanted: a closed index's data must survive its reopening. A node reports no copies
                // of a closed index, so its assignment entries are the only record of where that data is, and only
                // another node's entry can stand in for this one's.
                if (assignedElsewhere(copy, node.nodeId(), observation) == false) {
                    return new Owed(copy, "the only node assigned " + shard + ", whose index is closed; reaping it"
                        + " would lose that copy's data when the index is reopened");
                }
                continue;
            }
            if (startedElsewhere(copy, node.nodeId(), observation) == false) {
                return new Owed(copy, "still the only node with a started copy of " + shard + "; reaping it would"
                    + " drop that copy if the node ever came back");
            }
        }
        return null;
    }

    /** Whether another node whose copies count is still assigned this shard. */
    private static boolean assignedElsewhere(Copy copy, NodeId exclude, Observation observation) {
        for (Observation.NodeObservation other : observation.nodes().values()) {
            if (other.nodeId().equals(exclude) == false && other.copiesCount()
                && other.isAssigned(copy.tenant(), copy.indexUuid(), copy.shard())) {
                return true;
            }
        }
        return false;
    }

    private static Set<Copy> copiesOf(Observation.NodeObservation node) {
        Set<Copy> copies = new LinkedHashSet<>();
        ShardInventory inventory = node.inventory();
        if (inventory != null) {
            inventory.tenants().forEach((tenant, tenantShards) ->
                tenantShards.indices().forEach((uuid, indexShards) ->
                    indexShards.shards().keySet().forEach(shard -> copies.add(new Copy(tenant, uuid, shard)))));
        }
        Assignment assignment = node.assignment();
        if (assignment != null) {
            assignment.tenants().forEach((tenant, tenantAssignment) ->
                tenantAssignment.indices().forEach((uuid, indexAssignment) ->
                    indexAssignment.shards().keySet().forEach(shard -> copies.add(new Copy(tenant, uuid, shard)))));
        }
        return copies;
    }

    /**
     * Whether another node reports a <b>started</b> copy of this shard.
     *
     * <p>Started, not merely present: a replacement that is still initializing has not finished reading the data
     * yet, and treating it as cover would be the same mistake {@code SurplusReaper} avoids by only removing a
     * copy when the ones remaining are already started.
     */
    private static boolean startedElsewhere(Copy copy, NodeId exclude, Observation observation) {
        for (Observation.NodeObservation other : observation.nodes().values()) {
            if (other.nodeId().equals(exclude) || other.copiesCount() == false || other.inventory() == null) {
                continue;
            }
            ShardInventory.TenantShards tenantShards = other.inventory().tenants().get(copy.tenant());
            if (tenantShards == null) {
                continue;
            }
            ShardInventory.IndexShards indexShards = tenantShards.indices().get(copy.indexUuid());
            if (indexShards == null) {
                continue;
            }
            ShardInventory.ShardCopy held = indexShards.shards().get(copy.shard());
            if (held != null && held.state() == ShardState.STARTED) {
                return true;
            }
        }
        return false;
    }
}
