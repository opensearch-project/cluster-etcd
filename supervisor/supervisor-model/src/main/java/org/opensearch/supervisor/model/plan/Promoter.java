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
 * <p><b>Not tracked: the in-sync set.</b> OpenSearch promotes only a replica in the primary's in-sync set; this chooses
 * any started replica on a node still heartbeating, by node id. A replica that fell behind before the primary was lost
 * can therefore be promoted and lose what it missed. The data plane keeps an in-sync set locally and this service
 * does not observe it — worth closing if document replication becomes a first-class mode here.
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
        // and rewriting every copy's assignment with it.
        for (Map.Entry<NodeId, Observation.NodeObservation> entry : observation.placeableNodes().entrySet()) {
            Observation.NodeObservation node = entry.getValue();
            if (node.actsOnItsAssignment() && assignedPrimary(node, tenant, index, shard)) {
                return new Outcome(null, "a promotion to [" + entry.getKey() + "] has been written and the node has"
                    + " not reported it yet; nothing else is placed for this shard until it does");
            }
        }

        List<NodeId> capable = new ArrayList<>();
        List<NodeId> incapable = new ArrayList<>();
        for (Map.Entry<NodeId, Observation.NodeObservation> entry : observation.placeableNodes().entrySet()) {
            Observation.NodeObservation node = entry.getValue();
            if (node.actsOnItsAssignment() == false || holdsStartedReplica(node, tenant, index, shard) == false) {
                continue;
            }
            if (node.registration() != null && node.registration().declares(ContractFeatures.PRIMARY_TERMS)) {
                capable.add(entry.getKey());
            } else {
                incapable.add(entry.getKey());
            }
        }

        if (capable.isEmpty() == false) {
            return new Outcome(capable.stream().min(Comparator.comparing(NodeId::value)).orElseThrow(), null);
        }
        if (incapable.isEmpty() == false) {
            return new Outcome(null, "a started replica of this shard survives on " + incapable.stream()
                .sorted(Comparator.comparing(NodeId::value)).toList()
                + ", and promoting it needs the node to declare [" + ContractFeatures.PRIMARY_TERMS + "]; placing an"
                + " empty primary elsewhere instead would make that replica recover from it and discard its data, so"
                + " this shard waits for a node that can be promoted");
        }
        return Outcome.none();
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

    private static boolean holdsStartedReplica(
        Observation.NodeObservation node,
        TenantId tenant,
        IndexConfig index,
        int shard
    ) {
        ShardInventory inventory = node.inventory();
        if (inventory == null) {
            return false;
        }
        ShardInventory.TenantShards tenantShards = inventory.tenants().get(tenant);
        if (tenantShards == null) {
            return false;
        }
        ShardInventory.IndexShards held = tenantShards.indices().get(index.uuid());
        if (held == null) {
            return false;
        }
        ShardInventory.ShardCopy copy = held.shards().get(shard);
        return copy != null && copy.role() == ShardRole.REPLICA && copy.state() == ShardState.STARTED;
    }
}
