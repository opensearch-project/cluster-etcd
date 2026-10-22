package org.opensearch.supervisor.model.observe;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * A shard's copies as reported, with any relocating pair linked ({@code DESIGN.md} §29.9, {@code NODE_CONTRACT.md},
 * <i>Proposed: relocating a document-replication primary</i>). One answer, read by the index projection, which shows a
 * pair as one primary throughout, and by the differ, which does not call a pair a {@code PRIMARY_CONFLICT}. Two
 * pieces of code each deciding which copies are a pair would eventually decide differently (§19).
 *
 * <p>Until the target has started, the source is the primary and the target is not counted. From then on the target is
 * the primary and the source is not, even while the source's node still reports its copy because it has not applied its
 * withdrawal. The pair is linked by the id the source's node publishes, which it keeps publishing until that withdrawal,
 * because the completion pass rewrites the assignments first; and, before that id is minted, by the two assignments. A
 * copy assigned as a relocation's target is never a primary of its own before it has started, whatever became of its
 * source.
 *
 * @param reported every copy of the shard on a node whose copies count, by node
 * @param hidden the nodes whose reported copy is one end of a pair and not the shard's primary now
 * @param sources for a source still primary, where it is relocating to
 */
public record RelocatingPairs(
    Map<NodeId, ShardInventory.ShardCopy> reported,
    Set<NodeId> hidden,
    Map<NodeId, Source> sources
) {

    public record Source(NodeId target, String targetAllocationId) {}

    /** These copies, with nothing linked: for an index that does not relocate. */
    public static RelocatingPairs none(Map<NodeId, ShardInventory.ShardCopy> reported) {
        return new RelocatingPairs(reported, Set.of(), Map.of());
    }

    /** Whether this node's reported copy counts as one of the shard's copies now. */
    public boolean counts(NodeId node) {
        return reported.containsKey(node) && hidden.contains(node) == false;
    }

    public static RelocatingPairs of(TenantId tenant, String indexUuid, int shard, Observation observation) {
        Map<NodeId, ShardInventory.ShardCopy> reported = new TreeMap<>(java.util.Comparator.comparing(NodeId::value));
        observation.nodes().forEach((nodeId, node) -> {
            if (node.copiesCount() == false || node.inventory() == null) {
                return;
            }
            ShardInventory.TenantShards tenantShards = node.inventory().tenants().get(tenant);
            if (tenantShards == null) {
                return;
            }
            ShardInventory.IndexShards held = tenantShards.indices().get(indexUuid);
            if (held == null) {
                return;
            }
            ShardInventory.ShardCopy copy = held.shards().get(shard);
            if (copy != null) {
                reported.put(nodeId, copy);
            }
        });

        Set<NodeId> hidden = new java.util.HashSet<>();
        Map<NodeId, Source> sources = new LinkedHashMap<>();
        reported.forEach((sourceNode, copy) -> {
            if (copy.role() != ShardRole.PRIMARY) {
                return;
            }
            String minted = copy.relocationTargetAllocationId();
            NodeId assignedTarget = relocating(observation, sourceNode, tenant, indexUuid, shard, true);
            if (minted == null && assignedTarget == null) {
                return;
            }
            NodeId targetNode = null;
            for (Map.Entry<NodeId, ShardInventory.ShardCopy> candidate : reported.entrySet()) {
                ShardInventory.ShardCopy other = candidate.getValue();
                if (candidate.getKey().equals(sourceNode) || other.role() != ShardRole.PRIMARY) {
                    continue;
                }
                boolean byId = minted != null && minted.equals(other.allocationId());
                boolean byAssignment = candidate.getKey().equals(assignedTarget)
                    && sourceNode.equals(relocating(observation, candidate.getKey(), tenant, indexUuid, shard, false));
                if (byId || byAssignment) {
                    targetNode = candidate.getKey();
                }
            }
            if (targetNode != null && reported.get(targetNode).state() == ShardState.STARTED) {
                // Handed off: the target is the primary, and the source serves nothing.
                hidden.add(sourceNode);
                return;
            }
            if (targetNode != null) {
                hidden.add(targetNode);
            }
            sources.put(sourceNode, new Source(targetNode != null ? targetNode : assignedTarget, minted));
        });
        // A copy assigned as a relocation's target is never a primary of its own before it has started, whatever
        // became of its source: if the source is gone, the relocation is cancelled and the copy dropped.
        reported.forEach((node, copy) -> {
            if (copy.role() == ShardRole.PRIMARY && copy.state() != ShardState.STARTED
                && relocating(observation, node, tenant, indexUuid, shard, false) != null) {
                hidden.add(node);
            }
        });
        return new RelocatingPairs(java.util.Collections.unmodifiableMap(reported), Set.copyOf(hidden),
            Map.copyOf(sources));
    }

    /** The other end this node's assignment names for the shard: where it relocates to, or from. */
    private static NodeId relocating(
        Observation observation,
        NodeId node,
        TenantId tenant,
        String indexUuid,
        int shard,
        boolean to
    ) {
        Observation.NodeObservation observed = observation.nodes().get(node);
        if (observed == null || observed.assignment() == null) {
            return null;
        }
        var tenantAssignment = observed.assignment().tenants().get(tenant);
        var entry = tenantAssignment == null ? null : tenantAssignment.indices().get(indexUuid);
        if (entry == null) {
            return null;
        }
        Map<Integer, NodeId> relocating = to ? entry.relocatingTo() : entry.relocatingFrom();
        return relocating == null ? null : relocating.get(shard);
    }
}
