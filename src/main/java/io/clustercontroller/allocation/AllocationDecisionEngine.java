package io.clustercontroller.allocation;

import io.clustercontroller.allocation.deciders.*;
import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Allocation decision engine that chains multiple AllocationDeciders.
 * 
 * Evaluates all enabled deciders and returns nodes that pass all checks.
 * Callers must explicitly enable the deciders they want to use.
 */
@Slf4j
public class AllocationDecisionEngine {
    
    private final Map<Class<? extends AllocationDecider>, AllocationDecider> availableDeciders;
    
    /**
     * List of currently enabled deciders - populated by calling enableDecider().
     * Callers must explicitly enable the deciders they want to use.
     * 
     * TODO: Make decider configuration externally configurable via properties
     */
    private final List<AllocationDecider> enabledDeciders;
    
    public AllocationDecisionEngine() {
        this.availableDeciders = new HashMap<>();
        this.enabledDeciders = new ArrayList<>();
        
        // Register all available deciders
        registerDecider(AllNodesDecider.class, new AllNodesDecider());
        registerDecider(HealthDecider.class, new HealthDecider());
        registerDecider(RoleDecider.class, new RoleDecider());
        registerDecider(ShardPoolDecider.class, new ShardPoolDecider());
    }
    
    /**
     * Get available nodes for allocation by applying all enabled deciders.
     */
    public List<SearchUnit> getAvailableNodesForAllocation(String shardId, String indexName, 
                                                          List<SearchUnit> candidateNodes, NodeRole targetRole) {
        List<SearchUnit> selectedNodes = new ArrayList<>();
        
        // If no deciders enabled, return empty list
        if (enabledDeciders.isEmpty()) {
            return selectedNodes; // Empty list
        }
        
        for (SearchUnit node : candidateNodes) {
            Decision finalDecision = Decision.YES;
            
            for (AllocationDecider decider : enabledDeciders) {
                if (!decider.isEnabled()) continue;
                
                Decision deciderResult = decider.canAllocate(shardId, node, indexName, targetRole);
                finalDecision = finalDecision.merge(deciderResult);
                
                if (finalDecision == Decision.NO) break;
            }
            
            if (finalDecision == Decision.YES) {
                selectedNodes.add(node);
            }
        }
        
        return selectedNodes;
    }
    
    /**
     * Enable a decider.
     */
    public void enableDecider(Class<? extends AllocationDecider> deciderClass) {
        AllocationDecider decider = availableDeciders.get(deciderClass);
        if (decider != null && !enabledDeciders.contains(decider)) {
            decider.setEnabled(true);
            enabledDeciders.add(decider);
        }
    }
    
    /**
     * Disable a decider.
     */
    public void disableDecider(Class<? extends AllocationDecider> deciderClass) {
        AllocationDecider decider = availableDeciders.get(deciderClass);
        if (decider != null) {
            decider.setEnabled(false);
            enabledDeciders.remove(decider);
        }
    }
    
    /**
     * Register a decider.
     */
    public void registerDecider(Class<? extends AllocationDecider> deciderClass, AllocationDecider decider) {
        availableDeciders.put(deciderClass, decider);
    }
}
