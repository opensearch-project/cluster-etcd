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
import io.clustercontroller.models.ShardAllocation;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
        NodeRole targetRole,
        ShardAllocation currentPlanned
    ) {
        
        if (targetRole == NodeRole.PRIMARY) {
            // PRIMARY allocation: use standard decider-based approach
            return getPrimaryNodesForAllocation(shardId, indexName, allNodes);
        } else {
            // REPLICA allocation: use group-based bin-packing with stable allocation
            return getReplicaNodesForAllocation(shardId, indexName, indexConfig, allNodes, targetRole, currentPlanned);
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
     * Get eligible nodes for REPLICA allocation using group-based bin-packing with stable allocation.
     * 
     * Flow (Stable Allocation):
     * 0. Get current groups from planned allocation (if exists)
     * 1. Calculate desired number of groups (from index config)
     * 2. If current == desired → return nodes from current groups (stable!)
     * 3. If current < desired → select additional groups (scale up)
     * 4. If current > desired → log warning, not supported yet (scale down)
     * 
     * Traditional Flow (when selecting groups):
     * 1. Discover ALL groups from all nodes
     * 2. Filter groups to get only replica groups (based on node role)
     * 3. Select N groups using strategy (excluding current groups if scale-up)
     * 4. Apply deciders (HealthDecider) to filter nodes within selected groups
     * 5. Return eligible nodes from selected groups
     */
    private List<SearchUnit> getReplicaNodesForAllocation(
        int shardId,
        String indexName,
        Index indexConfig,
        List<SearchUnit> allNodes,
        NodeRole targetRole,
        ShardAllocation currentPlanned
    ) {
        // Step 0: Get current groups from planned allocation (stable allocation check)
        Set<String> currentGroupIds = getCurrentGroupsFromPlannedAllocation(currentPlanned, allNodes);
        
        // Step 1: Calculate desired number of groups from index config
        int desiredGroupCount = 1; // default
        if (indexConfig != null && indexConfig.getSettings() != null 
            && indexConfig.getSettings().getShardGroupsAllocateCount() != null 
            && shardId < indexConfig.getSettings().getShardGroupsAllocateCount().size()) {
            desiredGroupCount = indexConfig.getSettings().getShardGroupsAllocateCount().get(shardId);
        }
        
        int currentGroupCount = currentGroupIds.size();
        
        log.debug("REPLICA allocation for shard {}/{}: current groups = {}, desired groups = {}", 
                 indexName, shardId, currentGroupCount, desiredGroupCount);
        
        // Step 2: Stable allocation check
        if (currentGroupCount == desiredGroupCount && !currentGroupIds.isEmpty()) {
            // Already correctly allocated → return nodes from current groups (stable!)
            log.info("Shard {}/{} already allocated to {} groups - stable allocation, returning current nodes", 
                    indexName, shardId, currentGroupCount);
            return getNodesFromGroupIds(currentGroupIds, allNodes, shardId, indexName, targetRole);
        }
        
        // Step 3: Handle downscaling (not supported yet)
        if (currentGroupCount > desiredGroupCount) {
            log.warn("Shard {}/{} currently allocated to {} groups but config requires {} groups. " +
                    "Downscaling not supported yet - keeping current allocation.", 
                    indexName, shardId, currentGroupCount, desiredGroupCount);
            // TODO: Implement downscaling - need to:
            // 1. Select which groups to remove (least loaded? most loaded? last N?)
            // 2. Remove nodes from planned allocation in etcd
            // 3. Remove shard from goal-states of those nodes
            // 4. Create a cleanup task to handle removal
            return getNodesFromGroupIds(currentGroupIds, allNodes, shardId, indexName, targetRole);
        }
        
        // Step 4: Scale up - need more groups
        log.info("Shard {}/{} needs scale-up: current = {} groups, desired = {} groups", 
                indexName, shardId, currentGroupCount, desiredGroupCount);
        
        // Discover ALL groups from all nodes
        List<NodesGroup> allGroups = groupManager.discoverGroups(allNodes);
        
        if (allGroups.isEmpty()) {
            log.warn("No groups discovered for shard {}/{}. Cannot proceed with group-based allocation.", 
                    indexName, shardId);
            return List.of();
        }
        
        // Filter to get only REPLICA groups
        List<NodesGroup> replicaGroups = groupManager.filterGroupsByRole(allGroups, NodeRole.REPLICA);
        
        if (replicaGroups.isEmpty()) {
            log.warn("No REPLICA groups found for shard {}/{}. Cannot proceed with group-based allocation.", 
                    indexName, shardId);
            return List.of();
        }
        
        // Exclude current groups from selection (don't reselect them!)
        List<NodesGroup> availableGroups = replicaGroups.stream()
            .filter(group -> !currentGroupIds.contains(group.getGroupId()))
            .collect(Collectors.toList());
        
        if (availableGroups.isEmpty()) {
            log.warn("No available REPLICA groups for scale-up of shard {}/{} (all groups already allocated)", 
                    indexName, shardId);
            // Return nodes from current groups (better than nothing)
            return getNodesFromGroupIds(currentGroupIds, allNodes, shardId, indexName, targetRole);
        }
        
        // Calculate how many additional groups we need
        int additionalGroupsNeeded = desiredGroupCount - currentGroupCount;
        
        // Select additional groups using GroupManager's configured strategy
        List<NodesGroup> additionalGroups = groupManager.selectGroups(availableGroups, targetRole, additionalGroupsNeeded);
        
        if (additionalGroups.isEmpty()) {
            log.warn("No additional groups selected for shard {}/{} scale-up", 
                    indexName, shardId);
            // Return nodes from current groups
            return getNodesFromGroupIds(currentGroupIds, allNodes, shardId, indexName, targetRole);
        }
        
        log.info("REPLICA scale-up for shard {}/{}: Adding {} new group(s) to existing {} group(s)", 
                indexName, shardId, additionalGroups.size(), currentGroupCount);
        
        // Combine current groups + new groups
        Set<String> allSelectedGroupIds = new HashSet<>(currentGroupIds);
        additionalGroups.forEach(group -> allSelectedGroupIds.add(group.getGroupId()));
        
        // Return nodes from all groups (current + new)
        return getNodesFromGroupIds(allSelectedGroupIds, allNodes, shardId, indexName, targetRole);
    }
    
    /**
     * Get current group IDs from planned allocation.
     * 
     * Extracts node names from planned allocation, finds those nodes in allNodes,
     * and gets their group IDs (node.getShardId()).
     * 
     * @param currentPlanned Current planned allocation (may be null)
     * @param allNodes All nodes in the cluster
     * @return Set of current group IDs (empty if no planned allocation)
     */
    private Set<String> getCurrentGroupsFromPlannedAllocation(ShardAllocation currentPlanned, List<SearchUnit> allNodes) {
        Set<String> currentGroupIds = new HashSet<>();
        
        if (currentPlanned == null || currentPlanned.getSearchSUs() == null || currentPlanned.getSearchSUs().isEmpty()) {
            return currentGroupIds; // No current allocation
        }
        
        List<String> allocatedNodeNames = currentPlanned.getSearchSUs();
        
        // For each allocated node, find it in allNodes and get its group ID
        for (String nodeName : allocatedNodeNames) {
            SearchUnit node = allNodes.stream()
                .filter(n -> n.getName().equals(nodeName))
                .findFirst()
                .orElse(null);
            
            if (node != null && node.getShardId() != null) {
                // TODO: Replace shardId with dedicated group identifier in the future
                currentGroupIds.add(node.getShardId()); // Group ID from SearchUnit.shardId
            } else {
                log.debug("Node {} from planned allocation not found in allNodes or has no shardId", nodeName);
            }
        }
        
        return currentGroupIds;
    }
    
    /**
     * Get eligible nodes from specific group IDs.
     * 
     * Finds all nodes belonging to the specified groups and applies HealthDecider.
     * 
     * @param groupIds Set of group IDs to get nodes from
     * @param allNodes All nodes in the cluster
     * @param shardId Shard ID
     * @param indexName Index name
     * @param targetRole Target role
     * @return List of eligible nodes from the specified groups
     */
    private List<SearchUnit> getNodesFromGroupIds(Set<String> groupIds, List<SearchUnit> allNodes, 
                                                   int shardId, String indexName, NodeRole targetRole) {
        if (groupIds.isEmpty()) {
            return List.of();
        }
        
        String shardIdStr = String.valueOf(shardId);
        List<SearchUnit> eligibleNodes = new ArrayList<>();
        
        // Find all nodes that belong to the specified groups
        for (SearchUnit node : allNodes) {
            if (node.getShardId() == null || !groupIds.contains(node.getShardId())) {
                continue; // Node doesn't belong to any of the selected groups
            }
            
            // Apply deciders (HealthDecider)
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
        
        if (eligibleNodes.isEmpty()) {
            log.warn("No eligible nodes found in groups {} after applying deciders for shard {}/{}", 
                    groupIds, indexName, shardId);
        }
        
        return eligibleNodes;
    }
}

