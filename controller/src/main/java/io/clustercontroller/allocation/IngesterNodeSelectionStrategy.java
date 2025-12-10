package io.clustercontroller.allocation;

import io.clustercontroller.models.NodesGroup;
import io.clustercontroller.models.SearchUnit;

import java.util.List;

/**
 * Strategy for selecting ONE ingester node from eligible nodes within a group.
 * 
 * This is specific to PRIMARY (ingester) allocation.
 * Input: Pre-filtered list of eligible nodes (already passed all deciders)
 * Output: ONE selected node
 * 
 * Different implementations:
 * - RandomIngesterNodeSelectionStrategy: Pick random node
 * - LoadBasedIngesterNodeSelectionStrategy: Pick least-loaded node (future)
 */
public interface IngesterNodeSelectionStrategy {
    
    /**
     * Select ONE node from a list of eligible nodes.
     * 
     * @param eligibleNodes List of nodes that passed all deciders (health, zone, etc.)
     * @param group The group these nodes belong to (for context/logging)
     * @param shardId Shard ID (for logging)
     * @param indexName Index name (for logging)
     * @return Selected node, or null if input is empty
     */
    SearchUnit selectNode(
        List<SearchUnit> eligibleNodes,
        NodesGroup group,
        String shardId,
        String indexName
    );
    
    /**
     * Get strategy name for logging/debugging.
     */
    String getStrategyName();
}

