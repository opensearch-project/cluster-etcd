package io.clustercontroller.allocation.deciders;

import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;

/**
 * Interface for allocation decision making.
 * 
 * Each AllocationDecider implements a specific rule for determining
 * whether a shard can be allocated to a particular node.
 * 
 * Inspired by OpenSearch's AllocationDecider pattern.
 */
public interface AllocationDecider {
    
    /**
     * Determine if a shard can be allocated to a node.
     * 
     * @param shardId the shard ID to allocate
     * @param node the target node
     * @param indexName the index name
     * @param targetRole the role we're selecting for
     * @return allocation decision
     */
    Decision canAllocate(String shardId, SearchUnit node, String indexName, NodeRole targetRole);
    
    /**
     * Get the name of this decider.
     */
    String getName();
    
    /**
     * Check if this decider is enabled.
     */
    boolean isEnabled();
    
    /**
     * Enable or disable this decider.
     */
    void setEnabled(boolean enabled);
}
