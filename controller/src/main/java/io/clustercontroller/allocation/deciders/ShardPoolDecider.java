package io.clustercontroller.allocation.deciders;

import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;

/**
 * Decider that filters nodes based on shard pool assignment.
 * 
 * Only allows allocation to nodes where node.getShardId() matches the target shardId.
 */
public class ShardPoolDecider implements AllocationDecider {
    private boolean enabled = true;
    
    @Override
    public Decision canAllocate(String shardId, SearchUnit node, String indexName, NodeRole targetRole) {
        String nodeShardId = node.getShardId();
        return (nodeShardId != null && nodeShardId.equals(shardId)) ? Decision.YES : Decision.NO;
    }
    
    @Override
    public String getName() { return "ShardPoolDecider"; }
    
    @Override
    public boolean isEnabled() { return enabled; }
    
    @Override
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
}
