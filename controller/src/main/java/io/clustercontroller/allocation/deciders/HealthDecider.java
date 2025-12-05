package io.clustercontroller.allocation.deciders;

import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.config.Constants;

/**
 * Decider that filters nodes based on health status.
 * 
 * Rejects nodes with stateAdmin != "NORMAL" or statePulled == RED.
 */
public class HealthDecider implements AllocationDecider {
    private boolean enabled = true;
    
    @Override
    public Decision canAllocate(String shardId, SearchUnit node, String indexName, NodeRole targetRole) {
        String stateAdmin = node.getStateAdmin();
        HealthState statePulled = node.getStatePulled();
        
        if (stateAdmin == null || !Constants.ADMIN_STATE_NORMAL.equalsIgnoreCase(stateAdmin)) {
            return Decision.NO;
        }
        
        if (statePulled == HealthState.RED) {
            return Decision.NO;
        }
        
        return Decision.YES;
    }
    
    @Override
    public String getName() { return "HealthDecider"; }
    
    @Override
    public boolean isEnabled() { return enabled; }
    
    @Override
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
}
