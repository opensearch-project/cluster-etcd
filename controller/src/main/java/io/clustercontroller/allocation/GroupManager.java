package io.clustercontroller.allocation;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.NodesGroup;
import io.clustercontroller.models.SearchUnit;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Manages discovery, filtering, and selection of node groups.
 * 
 * Generic manager that works for BOTH replica groups (SEARCH_REPLICA) and ingest groups (PRIMARY).
 * 
 * Key concept: GROUP ID is extracted from each node's attributes.
 * 
 * Current implementation: GROUP ID = node.getShardId()
 * - This represents the "clusterless shard ID" reported by the node
 * - All nodes with the same GROUP ID form one replica group
 * 
 * Future: GROUP ID could be extracted from a different node attribute
 * - To change: modify getGroupId() method below
 * 
 * Responsibilities:
 * - Discover groups of nodes based on GROUP ID
 * - Validate group homogeneity (all nodes in a group must have the same role)
 * - Filter groups by NodeRole (PRIMARY vs REPLICA)
 * - Select a group using pluggable GroupSelectionStrategy
 */
@Slf4j
public class GroupManager {
    
    private final GroupSelectionStrategy selectionStrategy;
    
    public GroupManager(GroupSelectionStrategy selectionStrategy) {
        this.selectionStrategy = selectionStrategy;
    }
    
    /**
     * Extract GROUP ID from a node.
     * 
     * Current implementation: Returns node.getShardId()
     * 
     * TODO: GROUP ID currently uses node.getShardId() (clusterless shard ID from node config).
     * In the future, this will be replaced by a dedicated group identifier attribute.
     * When that happens, modify this method and the rest of the code will automatically adapt.
     * 
     * @param node SearchUnit node
     * @return GROUP ID for this node
     */
    private String getGroupId(SearchUnit node) {
        // TODO: Replace shardId with dedicated group identifier in the future
        // GROUP ID = shardId (clusterless shard ID reported by node)
        return node.getShardId();
    }
    
    /**
     * Discover groups from all nodes in the cluster.
     * 
     * Groups nodes by their GROUP ID (extracted via getGroupId() method).
     * Validates that all nodes in a group have the same role.
     * 
     * @param allNodes All nodes in the cluster
     * @return List of NodesGroups, grouped by GROUP ID
     * @throws IllegalStateException if a group contains mixed roles (FATAL error)
     */
    public List<NodesGroup> discoverGroups(List<SearchUnit> allNodes) {
        
        // Group nodes by GROUP ID (currently: shardId)
        Map<String, List<SearchUnit>> groupedByGroupId = allNodes.stream()
            .collect(Collectors.groupingBy(this::getGroupId));
        
        List<NodesGroup> groups = new ArrayList<>();
        
        for (Map.Entry<String, List<SearchUnit>> entry : groupedByGroupId.entrySet()) {
            String groupId = entry.getKey();
            List<SearchUnit> nodesInGroup = entry.getValue();
            
            // Validate role homogeneity within the group
            Set<String> rolesInGroup = nodesInGroup.stream()
                .map(SearchUnit::getRole)
                .collect(Collectors.toSet());
            
            if (rolesInGroup.size() > 1) {
                // FATAL: Mixed roles in a group - this is a configuration error
                String nodeNames = nodesInGroup.stream()
                    .map(n -> n.getName() + ":" + n.getRole())
                    .collect(Collectors.joining(", "));
                
                log.error("FATAL: Mixed roles detected in group (groupId={}) - Roles: {}. " +
                         "All nodes in a group MUST have the same role. Nodes: {}. " +
                         "Skipping this group to avoid killing allocation thread.", 
                         groupId, rolesInGroup, nodeNames);
                
                // TODO: Add alerting for this critical misconfiguration
                // Skip this group instead of throwing exception (which would kill allocation thread)
                continue;
            }
            
            // Create group
            String role = nodesInGroup.get(0).getRole();
            String shardId = nodesInGroup.get(0).getShardId();
            
            NodesGroup group = new NodesGroup();
            group.setGroupId(groupId);
            group.setRole(role);
            group.setShardId(shardId);
            group.setNodes(nodesInGroup);
            group.setNodeIds(nodesInGroup.stream()
                .map(SearchUnit::getName)
                .collect(Collectors.toList()));
            
            // TODO: Calculate current load (number of shards allocated to this group)
            // For now, set to 0 (will be calculated from node states)
            group.setCurrentLoad(0);
            
            groups.add(group);
            
            log.debug("Discovered group: groupId={}, {} nodes, role={}", 
                     groupId, nodesInGroup.size(), role);
        }
        
        log.info("Discovered {} groups from {} nodes", 
                groups.size(), allNodes.size());
        
        return groups;
    }
    
    /**
     * Filter groups by NodeRole.
     * 
     * @param groups All groups
     * @param targetRole Target role (PRIMARY or REPLICA)
     * @return Filtered list of groups matching the target role
     */
    public List<NodesGroup> filterGroupsByRole(List<NodesGroup> groups, NodeRole targetRole) {
        return groups.stream()
            .filter(group -> matchesRole(group, targetRole))
            .collect(Collectors.toList());
    }
    
    /**
     * Check if a group matches the target NodeRole.
     * 
     * Handles case-insensitive matching and multiple role name variants
     * (e.g., "replica", "REPLICA", "search_replica", "SEARCH_REPLICA" all match NodeRole.REPLICA).
     * 
     * @param group The group to check
     * @param targetRole Target role (PRIMARY or REPLICA)
     * @return true if group matches target role
     */
    private boolean matchesRole(NodesGroup group, NodeRole targetRole) {
        String groupRole = group.getRole();
        if (groupRole == null) {
            return false;
        }
        
        // Parse group role string to NodeRole enum (handles case-insensitivity and variants)
        // Then compare enum values directly
        NodeRole parsedRole = NodeRole.fromString(groupRole);
        return parsedRole == targetRole;
    }
    
    /**
     * Select multiple groups from the list using the configured selection strategy.
     * 
     * @param groups List of groups (already filtered by role)
     * @param targetRole Target role (for logging/context)
     * @param numGroupsNeeded Number of groups needed for allocation
     * @return List of selected groups (may be less than numGroupsNeeded if not enough available)
     */
    public List<NodesGroup> selectGroups(List<NodesGroup> groups, NodeRole targetRole, int numGroupsNeeded) {
        return selectionStrategy.selectGroups(groups, targetRole, numGroupsNeeded);
    }
}

