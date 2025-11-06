package io.clustercontroller.allocation;

import io.clustercontroller.allocation.deciders.AllocationDecider;
import io.clustercontroller.allocation.deciders.HealthDecider;
import io.clustercontroller.allocation.deciders.ZoneAntiAffinityDecider;
import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.NodesGroup;
import io.clustercontroller.models.SearchUnit;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Selector for picking ONE ingester node from a group.
 * 
 * Responsibilities:
 * 1. Apply deciders to filter eligible nodes (HealthDecider, ZoneAntiAffinityDecider)
 * 2. Use IngesterNodeSelectionStrategy to pick one node from eligible nodes
 * 
 * This is specific to PRIMARY (ingester) allocation.
 */
@Slf4j
public class IngesterNodeSelector {
    
    private final IngesterNodeSelectionStrategy selectionStrategy;
    
    public IngesterNodeSelector(IngesterNodeSelectionStrategy selectionStrategy) {
        this.selectionStrategy = selectionStrategy;
    }
    
    /**
     * Select ONE ingester node from a group.
     * 
     * Process:
     * 1. Apply HealthDecider to filter healthy nodes
     * 2. Apply ZoneAntiAffinityDecider to prefer different zones (best-effort)
     *    - Automatically handles single-writer case (usedZones empty → allows all zones)
     * 3. Use selection strategy to pick one node
     * 
     * @param group The group to select from
     * @param shardId Shard ID
     * @param indexName Index name
     * @param usedZones Zones already used by other selected nodes (for zone anti-affinity)
     * @return Selected node, or null if no eligible node found
     */
    public SearchUnit selectNodeFromGroup(
        NodesGroup group,
        String shardId,
        String indexName,
        Set<String> usedZones
    ) {
        List<SearchUnit> nodesInGroup = group.getNodes();
        
        if (nodesInGroup == null || nodesInGroup.isEmpty()) {
            log.warn("IngesterNodeSelector: Group {} has no nodes for shard {}/{}", 
                    group.getGroupId(), indexName, shardId);
            return null;
        }
        
        // Step 1: Apply HealthDecider
        HealthDecider healthDecider = new HealthDecider();
        List<SearchUnit> healthyNodes = filterNodesByDecider(
            nodesInGroup, shardId, indexName, healthDecider
        );
        
        if (healthyNodes.isEmpty()) {
            log.warn("IngesterNodeSelector: No healthy nodes in group {} for shard {}/{}", 
                    group.getGroupId(), indexName, shardId);
            return null;
        }
        
        log.debug("IngesterNodeSelector: {}/{} nodes are healthy in group {}", 
                 healthyNodes.size(), nodesInGroup.size(), group.getGroupId());
        
        // Step 2: Apply ZoneAntiAffinityDecider (best-effort, always applied)
        // If usedZones is empty (single-writer), decider automatically allows all zones
        List<SearchUnit> eligibleNodes = healthyNodes;
        
        ZoneAntiAffinityDecider zoneDecider = new ZoneAntiAffinityDecider(usedZones);
        List<SearchUnit> nodesInDifferentZone = filterNodesByDecider(
            healthyNodes, shardId, indexName, zoneDecider
        );
        
        if (!nodesInDifferentZone.isEmpty()) {
            // Great! We have nodes in different zones (or usedZones was empty)
            eligibleNodes = nodesInDifferentZone;
            if (usedZones != null && !usedZones.isEmpty()) {
                log.debug("IngesterNodeSelector: Zone anti-affinity succeeded - {}/{} nodes in different zones", 
                         eligibleNodes.size(), healthyNodes.size());
            }
        } else if (usedZones != null && !usedZones.isEmpty()) {
            // No nodes in different zones - fall back to all healthy nodes
            log.warn("IngesterNodeSelector: Zone anti-affinity failed for group {} - " +
                    "all healthy nodes are in already-used zones {}. Using same zone (worst case acceptable).", 
                    group.getGroupId(), usedZones);
            // eligibleNodes remains as healthyNodes
        }
        
        // Step 3: Use selection strategy to pick ONE node from eligible nodes
        SearchUnit selectedNode = selectionStrategy.selectNode(
            eligibleNodes, group, shardId, indexName
        );
        
        if (selectedNode == null) {
            log.error("IngesterNodeSelector: Selection strategy returned null for group {} shard {}/{}", 
                     group.getGroupId(), indexName, shardId);
        }
        
        return selectedNode;
    }
    
    /**
     * Filter nodes using a decider.
     */
    private List<SearchUnit> filterNodesByDecider(
        List<SearchUnit> nodes,
        String shardId,
        String indexName,
        AllocationDecider decider
    ) {
        return nodes.stream()
            .filter(node -> {
                Decision decision = decider.canAllocate(shardId, node, indexName, NodeRole.PRIMARY);
                return decision == Decision.YES;
            })
            .collect(Collectors.toList());
    }
}

