package io.clustercontroller.allocation;

import io.clustercontroller.allocation.deciders.*;
import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.ShardAllocation;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Standard allocation engine that uses deciders to filter eligible nodes.
 * 
 * This is the current/existing allocation logic extracted into its own class.
 * Uses: RoleDecider, ShardPoolDecider, HealthDecider
 * (Note: AllNodesDecider removed as it always returns YES - useless)
 */
@Slf4j
public class StandardAllocationEngine implements AllocationDecisionEngine {
    
    private final List<AllocationDecider> deciders;
    
    public StandardAllocationEngine() {
        // Fixed list of deciders (simplified from old dynamic enable/disable approach)
        this.deciders = Arrays.asList(
            new RoleDecider(),
            new ShardPoolDecider(),
            new HealthDecider()
            // Note: Removed AllNodesDecider - it always returns YES, so it's useless
        );
    }
    
    @Override
    public List<SearchUnit> getAvailableNodesForAllocation(
        int shardId,
        String indexName,
        Index indexConfig,
        List<SearchUnit> allNodes,
        NodeRole targetRole,
        ShardAllocation currentPlanned
    ) {
        // StandardAllocationEngine doesn't use currentPlanned - it always recomputes
        // (This is the existing behavior - no stable allocation logic)
        
        List<SearchUnit> selectedNodes = new ArrayList<>();
        
        // Convert shardId to String for deciders
        String shardIdStr = String.valueOf(shardId);
        
        for (SearchUnit node : allNodes) {
            Decision finalDecision = Decision.YES;
            
            for (AllocationDecider decider : deciders) {
                if (!decider.isEnabled()) continue;
                
                Decision deciderResult = decider.canAllocate(shardIdStr, node, indexName, targetRole);
                finalDecision = finalDecision.merge(deciderResult);
                
                if (finalDecision == Decision.NO) break;
            }
            
            if (finalDecision == Decision.YES) {
                selectedNodes.add(node);
            }
        }
        
        return selectedNodes;
    }
}


