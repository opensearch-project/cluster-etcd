package io.clustercontroller.allocation;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchReplicaGroup;
import io.clustercontroller.models.SearchUnit;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Manages discovery and organization of replica groups.
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
 * - Provide group selection policies (load-based, random, etc.)
 */
@Slf4j
public class GroupManager {
    
    /**
     * Extract GROUP ID from a node.
     * 
     * Current implementation: Returns node.getShardId()
     * 
     * TODO: If GROUP ID source changes in the future (e.g., from a different node attribute),
     * modify this method. The rest of the code will automatically adapt.
     * 
     * @param node SearchUnit node
     * @return GROUP ID for this node
     */
    private String getGroupId(SearchUnit node) {
        // GROUP ID = shardId (clusterless shard ID reported by node)
        return node.getShardId();
    }
    
    /**
     * Discover replica groups from eligible nodes.
     * 
     * Groups nodes by their GROUP ID (currently extracted from shardId).
     * Validates that all nodes in a group have the same role.
     * 
     * @param eligibleNodes Nodes that have already passed health/role checks
     * @return List of SearchReplicaGroups, grouped by GROUP ID
     * @throws IllegalStateException if a group contains mixed roles (FATAL error)
     */
    public List<SearchReplicaGroup> discoverGroups(List<SearchUnit> eligibleNodes) {
        
        // Group nodes by GROUP ID (currently: shardId)
        Map<String, List<SearchUnit>> groupedByGroupId = eligibleNodes.stream()
            .collect(Collectors.groupingBy(this::getGroupId));
        
        List<SearchReplicaGroup> groups = new ArrayList<>();
        
        for (Map.Entry<String, List<SearchUnit>> entry : groupedByGroupId.entrySet()) {
            String groupId = entry.getKey();
            List<SearchUnit> nodesInGroup = entry.getValue();
            
            // Validate role homogeneity within the group
            Set<String> rolesInGroup = nodesInGroup.stream()
                .map(SearchUnit::getRole)
                .collect(Collectors.toSet());
            
            if (rolesInGroup.size() > 1) {
                // FATAL: Mixed roles in a group
                String nodeNames = nodesInGroup.stream()
                    .map(n -> n.getName() + ":" + n.getRole())
                    .collect(Collectors.joining(", "));
                
                log.error("FATAL: Mixed roles detected in group (groupId={}) - Roles: {}. " +
                         "All nodes in a group MUST have the same role. Nodes: {}", 
                         groupId, rolesInGroup, nodeNames);
                
                throw new IllegalStateException(
                    String.format("Mixed roles in group groupId=%s: %s. " +
                                "This is a configuration error and must be fixed.", 
                                groupId, rolesInGroup)
                );
            }
            
            // Create group
            String role = nodesInGroup.get(0).getRole();
            String shardId = nodesInGroup.get(0).getShardId(); // For SearchReplicaGroup.shardId field
            
            SearchReplicaGroup group = new SearchReplicaGroup();
            group.setGroupId(groupId);
            group.setRole(role);
            group.setShardId(shardId); // Store the shardId separately (currently same as groupId)
            group.setNodes(nodesInGroup);
            group.setNodeIds(nodesInGroup.stream()
                .map(SearchUnit::getName)
                .collect(Collectors.toList()));
            
            // TODO: Calculate current shard count and max capacity
            // For now, set placeholder values
            group.setCurrentShardCount(0);
            group.setMaxCapacity(100); // Placeholder
            
            groups.add(group);
            
            log.debug("Discovered group: groupId={}, {} nodes, role: {}", 
                     groupId, nodesInGroup.size(), role);
        }
        
        log.info("Discovered {} replica groups from {} eligible nodes", 
                groups.size(), eligibleNodes.size());
        
        return groups;
    }
    
    /**
     * Select the least loaded group (load-based bin-packing).
     * 
     * @param groups List of groups
     * @return The group with most available capacity, or null if no groups have capacity
     */
    public SearchReplicaGroup selectLeastLoadedGroup(List<SearchReplicaGroup> groups) {
        return groups.stream()
            .filter(SearchReplicaGroup::hasCapacity)
            .max(Comparator.comparingInt(SearchReplicaGroup::availableCapacity))
            .orElse(null);
    }
    
    /**
     * Select a random group (randomization policy).
     * 
     * TODO: Implement randomization policy (currently NOT supported).
     * This would select a random group with available capacity.
     * 
     * @param groups List of groups
     * @return A random group with available capacity
     * @throws UnsupportedOperationException Always (not yet implemented)
     */
    public SearchReplicaGroup selectRandomGroup(List<SearchReplicaGroup> groups) {
        throw new UnsupportedOperationException(
            "Random group selection is not yet implemented. " +
            "Only load-based (bin-packing) selection is currently supported."
        );
    }
}

