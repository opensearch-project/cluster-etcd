package io.clustercontroller.allocation;

import io.clustercontroller.models.NodesGroup;
import io.clustercontroller.models.SearchUnit;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;

/**
 * Random node selection for ingester allocation.
 * 
 * Simply picks a random node from the eligible list.
 */
@Slf4j
public class RandomIngesterNodeSelectionStrategy implements IngesterNodeSelectionStrategy {
    
    private final Random random;
    
    public RandomIngesterNodeSelectionStrategy() {
        this.random = new Random();
    }
    
    /**
     * Constructor with seed for testing purposes.
     * Allows deterministic random selection in tests.
     */
    public RandomIngesterNodeSelectionStrategy(long seed) {
        this.random = new Random(seed);
    }
    
    @Override
    public SearchUnit selectNode(
        List<SearchUnit> eligibleNodes,
        NodesGroup group,
        String shardId,
        String indexName
    ) {
        if (eligibleNodes == null || eligibleNodes.isEmpty()) {
            log.warn("RandomIngesterNodeSelection: No eligible nodes to select from for group {} shard {}/{}", 
                    group.getGroupId(), indexName, shardId);
            return null;
        }
        
        // Randomly pick one node
        int randomIndex = random.nextInt(eligibleNodes.size());
        SearchUnit selectedNode = eligibleNodes.get(randomIndex);
        
        log.info("RandomIngesterNodeSelection: Selected node {} (index {}/{}) from group {} (zone={})", 
                selectedNode.getName(), randomIndex, eligibleNodes.size(), 
                group.getGroupId(), selectedNode.getZone());
        
        return selectedNode;
    }
    
    @Override
    public String getStrategyName() {
        return "Random";
    }
}

