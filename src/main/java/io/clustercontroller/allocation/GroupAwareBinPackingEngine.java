package io.clustercontroller.allocation;

import io.clustercontroller.allocation.deciders.HealthDecider;
import io.clustercontroller.allocation.deciders.RoleDecider;
import io.clustercontroller.allocation.deciders.ShardPoolDecider;
import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.SearchReplicaGroup;
import io.clustercontroller.models.SearchUnit;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Group-aware bin-packing allocation engine.
 * 
 * Strategy:
 * - For REPLICA allocation:
 *   - Uses GroupManager to discover replica groups (fixed sets of 3-5 nodes)
 *   - Allocates hot shards to the least loaded group (bin-packing)
 *   - Does NOT use ShardPoolDecider (replaced by group-based logic)
 * 
 * - For PRIMARY allocation:
 *   - Falls back to standard decider-based allocation
 *   - Uses: RoleDecider, ShardPoolDecider, HealthDecider
 * 
 * Note: This engine does NOT support RESPECT_REPLICA_COUNT strategy.
 * Only USE_ALL_AVAILABLE_NODES is supported.
 */
@Slf4j
public class GroupAwareBinPackingEngine implements AllocationDecisionEngine {
    
    private final GroupManager groupManager;
    private final List<io.clustercontroller.allocation.deciders.AllocationDecider> primaryDeciders;
    
    public GroupAwareBinPackingEngine() {
        this.groupManager = new GroupManager();
        
        // For PRIMARY allocation, use standard deciders
        this.primaryDeciders = Arrays.asList(
            new RoleDecider(),
            new ShardPoolDecider(),
            new HealthDecider()
        );
    }
    
    @Override
    public List<SearchUnit> getAvailableNodesForAllocation(
        int shardId,
        String indexName,
        Index indexConfig,
        List<SearchUnit> allNodes,
        NodeRole targetRole
    ) {
        
        if (targetRole == NodeRole.PRIMARY) {
            // PRIMARY allocation: use standard decider-based approach
            return getPrimaryNodes(shardId, indexName, allNodes);
        } else {
            // REPLICA allocation: use group-based bin-packing
            return getReplicaNodesFromGroup(shardId, indexName, allNodes, targetRole);
        }
    }
    
    /**
     * Get eligible nodes for PRIMARY allocation using standard deciders.
     */
    private List<SearchUnit> getPrimaryNodes(int shardId, String indexName, List<SearchUnit> allNodes) {
        List<SearchUnit> selectedNodes = new ArrayList<>();
        String shardIdStr = String.valueOf(shardId);
        
        for (SearchUnit node : allNodes) {
            Decision finalDecision = Decision.YES;
            
            for (io.clustercontroller.allocation.deciders.AllocationDecider decider : primaryDeciders) {
                if (!decider.isEnabled()) continue;
                
                Decision deciderResult = decider.canAllocate(shardIdStr, node, indexName, NodeRole.PRIMARY);
                finalDecision = finalDecision.merge(deciderResult);
                
                if (finalDecision == Decision.NO) break;
            }
            
            if (finalDecision == Decision.YES) {
                selectedNodes.add(node);
            }
        }
        
        log.debug("PRIMARY allocation for shard {}/{}: {} eligible nodes", 
                 indexName, shardId, selectedNodes.size());
        
        return selectedNodes;
    }
    
    /**
     * Get eligible nodes for REPLICA allocation using group-based bin-packing.
     * 
     * Strategy:
     * 1. Apply HealthDecider and RoleDecider at the node level to get eligible nodes
     * 2. Discover replica groups from eligible nodes (grouped by shardId)
     * 3. Select the least loaded group (bin-packing policy)
     * 4. Return all nodes from that group
     * 
     * This ensures hot shards are allocated to a new GROUP, not scattered across nodes.
     */
    private List<SearchUnit> getReplicaNodesFromGroup(
        int shardId,
        String indexName,
        List<SearchUnit> allNodes,
        NodeRole targetRole
    ) {
        
        // Step 1: Apply HealthDecider and RoleDecider at node level to filter eligible nodes
        HealthDecider healthDecider = new HealthDecider();
        RoleDecider roleDecider = new RoleDecider();
        String shardIdStr = String.valueOf(shardId);
        
        List<SearchUnit> eligibleNodes = new ArrayList<>();
        for (SearchUnit node : allNodes) {
            Decision healthDecision = healthDecider.canAllocate(shardIdStr, node, indexName, targetRole);
            Decision roleDecision = roleDecider.canAllocate(shardIdStr, node, indexName, targetRole);
            
            if (healthDecision == Decision.YES && roleDecision == Decision.YES) {
                eligibleNodes.add(node);
            }
        }
        
        if (eligibleNodes.isEmpty()) {
            log.warn("No eligible nodes after applying HealthDecider and RoleDecider for shard {}/{}", 
                    indexName, shardId);
            return eligibleNodes;
        }
        
        // Step 2: Discover replica groups from eligible nodes
        // GroupManager extracts GROUP ID from each node (currently: node.getShardId())
        List<SearchReplicaGroup> groups = groupManager.discoverGroups(eligibleNodes);
        
        if (groups.isEmpty()) {
            log.warn("No replica groups discovered for shard {}/{}. Falling back to all eligible nodes.", 
                    indexName, shardId);
            return eligibleNodes;
        }
        
        // Step 3: Select the least loaded group using load-based bin-packing policy
        SearchReplicaGroup selectedGroup = groupManager.selectLeastLoadedGroup(groups);
        
        if (selectedGroup == null) {
            log.warn("No group with available capacity for shard {}/{}. Falling back to all eligible nodes.", 
                    indexName, shardId);
            return eligibleNodes;
        }
        
        log.info("REPLICA allocation for shard {}/{}: Selected group {} with {} nodes (capacity: {}/{})", 
                indexName, shardId, selectedGroup.getGroupId(), selectedGroup.size(),
                selectedGroup.availableCapacity(), selectedGroup.getMaxCapacity());
        
        // Step 4: Return all nodes from the selected group
        return selectedGroup.getNodes();
    }
}

