package io.clustercontroller.allocation;

import io.clustercontroller.models.NodesGroup;
import io.clustercontroller.models.SearchUnit;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Load-based node selection for ingester allocation.
 * 
 * TODO: NOT YET IMPLEMENTED
 * 
 * When implemented, this should:
 * - Count shards allocated to each node in the eligible list
 * - Pick the node with the lowest load (least shards)
 * - Break ties randomly or by node name (lexicographic)
 */
@Slf4j
public class LoadBasedIngesterNodeSelectionStrategy implements IngesterNodeSelectionStrategy {
    
    @Override
    public SearchUnit selectNode(
        List<SearchUnit> eligibleNodes,
        NodesGroup group,
        String shardId,
        String indexName
    ) {
        log.error("LoadBasedIngesterNodeSelection is not yet implemented. Returning null.");
        // TODO: Implement load-based selection:
        // 1. Query actual state for each eligible node
        // 2. Count shards per node
        // 3. Pick node with lowest load
        // 4. Break ties deterministically (e.g., lexicographic by node name)
        return null;
    }
    
    @Override
    public String getStrategyName() {
        return "LoadBased";
    }
}

