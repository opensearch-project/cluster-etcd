package io.clustercontroller.allocation;

import io.clustercontroller.allocation.deciders.AllocationDecider;
import io.clustercontroller.allocation.deciders.HealthDecider;
import io.clustercontroller.allocation.deciders.RoleDecider;
import io.clustercontroller.allocation.deciders.ShardPoolDecider;
import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.NodesGroup;
import io.clustercontroller.models.SearchUnit;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Group-aware bin-packing allocation engine.
 * 
 * Strategy:
 * - For REPLICA allocation:
 *   - Uses GroupManager to discover replica groups (fixed sets of 3-5 nodes)
 *   - Allocates hot shards to the least loaded group (bin-packing)
 *   - Applies: HealthDecider, RoleDecider (NOT ShardPoolDecider - group logic replaces it)
 * 
 * - For PRIMARY allocation:
 *   - Falls back to standard decider-based allocation
 *   - Applies: RoleDecider, ShardPoolDecider, HealthDecider
 * 
 * Note: This engine does NOT support RESPECT_REPLICA_COUNT strategy.
 * Only USE_ALL_AVAILABLE_NODES is supported.
 */
@Slf4j
public class GroupAwareBinPackingEngine implements AllocationDecisionEngine {
    
    private final GroupManager groupManager;
    
    // Deciders for PRIMARY allocation (standard decider-based)
    // TODO: When group-aware bin packing is implemented for PRIMARY, change to only HealthDecider
    //       This ensures hot shards are allocated to a new GROUP, not scattered across nodes.
    private final List<AllocationDecider> primaryDeciders;
    
    // Deciders for REPLICA allocation (group-based, only health check)
    private final List<AllocationDecider> replicaDeciders;
    
    public GroupAwareBinPackingEngine() {
        // TODO: Make selection strategy configurable via properties
        // For now, use Random strategy (NOT YET IMPLEMENTED)
        GroupSelectionStrategy selectionStrategy = new RandomGroupSelectionStrategy();
        this.groupManager = new GroupManager(selectionStrategy);
        
        // Initialize deciders
        HealthDecider healthDecider = new HealthDecider();
        
        // PRIMARY uses all standard deciders (for now)
        this.primaryDeciders = List.of(
            new RoleDecider(),
            new ShardPoolDecider(),
            healthDecider
        );
        
        // REPLICA only uses HealthDecider (role/pool handled by group filtering)
        this.replicaDeciders = List.of(healthDecider);
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
            return getPrimaryNodesForAllocation(shardId, indexName, allNodes);
        } else {
            // REPLICA allocation: use group-based bin-packing
            return getReplicaNodesForAllocation(shardId, indexName, indexConfig, allNodes, targetRole);
        }
    }
    
    /**
     * Get eligible nodes for PRIMARY allocation using standard deciders.
     * 
     * Applies: RoleDecider, ShardPoolDecider, HealthDecider
     * 
     * TODO: Change to group-aware bin packing (same as REPLICA flow)
     */
    private List<SearchUnit> getPrimaryNodesForAllocation(int shardId, String indexName, List<SearchUnit> allNodes) {
        List<SearchUnit> selectedNodes = new ArrayList<>();
        String shardIdStr = String.valueOf(shardId);
        
        for (SearchUnit node : allNodes) {
            boolean canAllocate = true;
            
            for (AllocationDecider decider : primaryDeciders) {
                Decision decision = decider.canAllocate(shardIdStr, node, indexName, NodeRole.PRIMARY);
                if (decision == Decision.NO) {
                    canAllocate = false;
                    break;
                }
            }
            
            if (canAllocate) {
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
     * Flow:
     * 1. Discover ALL groups from all nodes (no filtering yet)
     * 2. Filter groups to get only replica groups (based on node role)
     * 3. Calculate number of groups needed (from index config)
     * 4. Select N least loaded replica groups (bin-packing policy)
     * 5. Apply deciders (HealthDecider) to filter nodes within ALL selected groups
     * 6. Return eligible nodes from ALL selected groups
     */
    private List<SearchUnit> getReplicaNodesForAllocation(
        int shardId,
        String indexName,
        Index indexConfig,
        List<SearchUnit> allNodes,
        NodeRole targetRole
    ) {
        
        // Step 1: Discover ALL groups from all nodes (GROUP ID = node.getShardId())
        List<NodesGroup> allGroups = groupManager.discoverGroups(allNodes);
        
        if (allGroups.isEmpty()) {
            log.warn("No groups discovered for shard {}/{}. Cannot proceed with group-based allocation.", 
                    indexName, shardId);
            return List.of();
        }
        
        // Step 2: Filter to get only REPLICA groups (exclude PRIMARY/ingest groups)
        List<NodesGroup> replicaGroups = groupManager.filterGroupsByRole(allGroups, NodeRole.REPLICA);
        
        if (replicaGroups.isEmpty()) {
            log.warn("No REPLICA groups found for shard {}/{}. Cannot proceed with group-based allocation.", 
                    indexName, shardId);
            return List.of();
        }
        
        // Step 3: Calculate number of groups needed from index config
        int numGroupsNeeded = 1; // default
        if (indexConfig != null && indexConfig.getShardGroupsAllocateCount() != null 
            && shardId < indexConfig.getShardGroupsAllocateCount().size()) {
            numGroupsNeeded = indexConfig.getShardGroupsAllocateCount().get(shardId);
        }
        
        // Step 4: Select REPLICA groups using GroupManager's configured strategy
        List<NodesGroup> selectedGroups = groupManager.selectGroups(replicaGroups, targetRole, numGroupsNeeded);
        
        if (selectedGroups.isEmpty()) {
            log.warn("No replica groups selected for shard {}/{}.", 
                    indexName, shardId);
            return List.of();
        }
        
        log.info("REPLICA allocation for shard {}/{}: Selected {} group(s)", 
                indexName, shardId, selectedGroups.size());
        
        // Step 5: Apply deciders to filter nodes within ALL selected groups
        // (Only HealthDecider - role/pool already handled by group filtering)
        String shardIdStr = String.valueOf(shardId);
        
        List<SearchUnit> eligibleNodes = new ArrayList<>();
        for (NodesGroup group : selectedGroups) {
            log.debug("Processing group {} with {} nodes (load: {})", 
                     group.getGroupId(), group.size(), group.getCurrentLoad());
            
            for (SearchUnit node : group.getNodes()) {
                boolean canAllocate = true;
                
                for (AllocationDecider decider : replicaDeciders) {
                    Decision decision = decider.canAllocate(shardIdStr, node, indexName, targetRole);
                    if (decision == Decision.NO) {
                        canAllocate = false;
                        break;
                    }
                }
                
                if (canAllocate) {
                    eligibleNodes.add(node);
                }
            }
        }
        
        if (eligibleNodes.isEmpty()) {
            log.warn("No eligible nodes in selected groups after applying deciders for shard {}/{}", 
                    indexName, shardId);
        }
        
        // Step 6: Return eligible nodes from ALL selected groups
        return eligibleNodes;
    }
}

