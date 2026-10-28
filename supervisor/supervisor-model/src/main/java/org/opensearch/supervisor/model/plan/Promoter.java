package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ContractFeatures;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Replication;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Chooses a document-replication replica to promote when a shard has lost its primary (finding 11).
 *
 * <p><b>Promotion is not placement, and treating it as placement lost data.</b> The allocator refuses the replica's
 * node — it already holds the shard — so a lost primary used to be replaced by a fresh, empty primary on some other
 * node, from which the surviving replica then recovered: every acknowledged write the replica held was discarded to
 * match an empty copy. Under document replication the replica <em>is</em> the data, so the only non-lossy move is to
 * make it the primary where it stands. That is not an allocation decision — no disk, awareness or shard-limit rule
 * has anything to say about a copy that is already there — so it does not go through the allocator, and it is not
 * braked by {@code allocation.enable}: an operator who stopped allocation for maintenance still wants writes to
 * survive losing a node, and OpenSearch promotes under the same setting for the same reason.
 *
 * <p><b>Only onto a node that declares {@code primary_terms}.</b> Promotion in place needs the term to go up —
 * OpenSearch's promotion path is unreachable at an unchanged term — and a node that ignores the field would fabricate
 * term 1 and recreate the shard instead, which is two primaries at one term. When replicas exist and none can be
 * promoted, this refuses rather than letting the allocator place an empty primary: a shard that cannot take writes is
 * recoverable, and one whose replica was wiped is not.
 *
 * <p><b>Every node whose copies still count is searched, not only the placeable ones.</b> A replica on a {@code STALE}
 * node is still counted by the differ, so a shard whose primary is lost and whose replica's node has merely gone quiet
 * looks primary-less — and returning nothing here would let the allocator place an empty primary on a live node, which
 * the replica would recover from when its node came back. Such a replica is not promoted, because a node that is not
 * reporting cannot be shown to have applied the promotion; the shard waits, with a reason, until the node is live again
 * or lost.
 *
 * <p><b>Only a replica the lost primary last called in sync</b> (finding 12). A replica outside that set may be missing
 * acknowledged writes, and promoting it would lose them silently; OpenSearch's allocator has the same rule and makes an
 * operator ask explicitly to break it. The set is read from the lost primary's own inventory, which stays in the store
 * and in the observation after the node is lost, and promotion happens long before that node is reaped, so no memory
 * is needed here. A replica is in the set when the allocation id it publishes is listed. When the lost primary
 * published a set and no started replica is in it, nothing is promoted and nothing is placed: the shard is reported,
 * because an empty primary would make every replica recover from it. When it never published one, any started replica
 * may be promoted, as before.
 */
final class Promoter {

    /** Either a node to promote on, or the reason nothing may be placed, or neither (no replica to consider). */
    record Outcome(NodeId promote, String refusal) {

        static Outcome none() {
            return new Outcome(null, null);
        }

        boolean promoting() {
            return promote != null;
        }

        boolean refusing() {
            return refusal != null;
        }
    }

    private Promoter() {}

    static Outcome propose(Observation observation, TenantId tenant, IndexConfig index, int shard) {
        if (index.replication() != Replication.DOCUMENT) {
            // A segment-replication primary is replaced from the remote store, and its read-only copies can never
            // be promoted; the allocator's fresh primary is the right answer there.
            return Outcome.none();
        }

        // A promotion already written and not yet reported. The node reports what it holds, and what it holds beats
        // what it was told, so until it applies the change its copy still reads as a replica -- the shard still looks
        // primary-less, and without this the planner would promote again every pass, advancing the term each time
        // and rewriting every copy's assignment with it. A node gone stale since keeps its claim, as any assigned
        // copy does through the stale window.
        for (Map.Entry<NodeId, Observation.NodeObservation> entry : observation.nodes().entrySet()) {
            Observation.NodeObservation node = entry.getValue();
            if (node.actsOnItsAssignment() && assignedPrimary(node, tenant, index, shard)) {
                return new Outcome(null, "a promotion to [" + entry.getKey() + "] has been written and the node has"
                    + " not reported it yet; nothing else is placed for this shard until it does");
            }
        }

        Set<String> lastInSync = lastInSyncOfLostPrimary(observation, tenant, index, shard);
        List<NodeId> capable = new ArrayList<>();
        List<NodeId> incapable = new ArrayList<>();
        List<NodeId> unplaceable = new ArrayList<>();
        List<NodeId> outOfSync = new ArrayList<>();
        for (Map.Entry<NodeId, Observation.NodeObservation> entry : observation.nodes().entrySet()) {
            Observation.NodeObservation node = entry.getValue();
            ShardInventory.ShardCopy replica = startedReplica(node, tenant, index, shard);
            if (node.actsOnItsAssignment() == false || replica == null) {
                continue;
            }
            if (lastInSync != null && lastInSync.contains(replica.allocationId()) == false) {
                outOfSync.add(entry.getKey());
            } else if (node.isPlaceable() == false) {
                unplaceable.add(entry.getKey());
            } else if (node.registration().declares(ContractFeatures.PRIMARY_TERMS)) {
                capable.add(entry.getKey());
            } else {
                incapable.add(entry.getKey());
            }
        }

        if (capable.isEmpty() == false) {
            return new Outcome(capable.stream().min(Comparator.comparing(NodeId::value)).orElseThrow(), null);
        }
        if (incapable.isEmpty() == false) {
            return new Outcome(null, "a started replica of this shard survives on " + sorted(incapable)
                + ", and promoting it needs the node to declare [" + ContractFeatures.PRIMARY_TERMS + "]; placing an"
                + " empty primary elsewhere instead would make that replica recover from it and discard its data, so"
                + " this shard waits for a node that can be promoted");
        }
        if (unplaceable.isEmpty() == false) {
            return new Outcome(null, "a started replica of this shard survives on " + sorted(unplaceable)
                + ", which is not live, so it cannot be promoted; placing an empty primary elsewhere instead would make"
                + " that replica recover from it and discard its data, so this shard waits until the node is live"
                + " again or is lost");
        }
        if (outOfSync.isEmpty() == false) {
            return new Outcome(null, "the lost primary last reported " + new java.util.TreeSet<>(lastInSync)
                + " in sync, and no started replica of this shard is among them: " + sorted(outOfSync) + " may be"
                + " missing acknowledged writes, so promoting one could lose them silently, and placing an empty primary"
                + " instead would make them recover from it and discard their data; this shard stays without a primary"
                + " until the lost primary returns or an in-sync replica does");
        }
        return Outcome.none();
    }

    /**
     * The in-sync set the lost primary last published, or null when no lost primary published one.
     *
     * <p>A lost primary is a node that is no longer counted and still reports this shard as {@code PRIMARY}: its last
     * inventory, which nothing has deleted. There can be more than one when a promoted primary was lost in turn, and
     * the earlier one's set is out of date, so the set comes from whichever was told the highest primary term. A lost
     * node's assignment is not rewritten, so it still records that term. Two at the same term would be a split brain;
     * a replica then has to be in both sets.
     */
    private static Set<String> lastInSyncOfLostPrimary(
        Observation observation,
        TenantId tenant,
        IndexConfig index,
        int shard
    ) {
        Set<String> inSync = null;
        long highestTerm = 0;
        for (Observation.NodeObservation node : observation.nodes().values()) {
            if (node.copiesCount()) {
                continue;
            }
            ShardInventory.ShardCopy copy = copyOf(node, tenant, index, shard);
            if (copy == null || copy.role() != ShardRole.PRIMARY || copy.inSync() == null) {
                continue;
            }
            long term = toldTerm(node, tenant, index, shard);
            if (term > highestTerm) {
                highestTerm = term;
                inSync = new java.util.HashSet<>(copy.inSync());
            } else if (term == highestTerm) {
                inSync.retainAll(copy.inSync());
            }
        }
        return inSync;
    }

    /** The primary term this node was last told for the shard, or 1 when its assignment gives none. */
    private static long toldTerm(Observation.NodeObservation node, TenantId tenant, IndexConfig index, int shard) {
        if (node.assignment() == null) {
            return 1L;
        }
        var tenantAssignment = node.assignment().tenants().get(tenant);
        var entry = tenantAssignment == null ? null : tenantAssignment.indices().get(index.uuid());
        return entry == null ? 1L : entry.primaryTermOf(shard);
    }

    private static List<NodeId> sorted(List<NodeId> nodes) {
        return nodes.stream().sorted(Comparator.comparing(NodeId::value)).toList();
    }

    private static boolean assignedPrimary(
        Observation.NodeObservation node,
        TenantId tenant,
        IndexConfig index,
        int shard
    ) {
        if (node.assignment() == null) {
            return false;
        }
        var tenantAssignment = node.assignment().tenants().get(tenant);
        if (tenantAssignment == null) {
            return false;
        }
        var indexAssignment = tenantAssignment.indices().get(index.uuid());
        return indexAssignment != null && indexAssignment.shards().get(shard) == ShardRole.PRIMARY;
    }

    /** This node's started replica of the shard, or null. */
    private static ShardInventory.ShardCopy startedReplica(
        Observation.NodeObservation node,
        TenantId tenant,
        IndexConfig index,
        int shard
    ) {
        ShardInventory.ShardCopy copy = copyOf(node, tenant, index, shard);
        return copy != null && copy.role() == ShardRole.REPLICA && copy.state() == ShardState.STARTED ? copy : null;
    }

    private static ShardInventory.ShardCopy copyOf(
        Observation.NodeObservation node,
        TenantId tenant,
        IndexConfig index,
        int shard
    ) {
        ShardInventory inventory = node.inventory();
        if (inventory == null) {
            return null;
        }
        ShardInventory.TenantShards tenantShards = inventory.tenants().get(tenant);
        if (tenantShards == null) {
            return null;
        }
        ShardInventory.IndexShards held = tenantShards.indices().get(index.uuid());
        return held == null ? null : held.shards().get(shard);
    }
}
