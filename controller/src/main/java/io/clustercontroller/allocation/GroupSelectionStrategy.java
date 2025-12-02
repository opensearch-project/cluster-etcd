package io.clustercontroller.allocation;

import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.NodesGroup;

import java.util.List;

/**
 * Strategy interface for selecting groups from a list of available groups.
 * 
 * Different implementations:
 * - LoadBasedStrategy: Select the N least loaded groups (bin-packing)
 * - RandomStrategy: Select N random groups
 * - RoundRobinStrategy: Select groups in round-robin fashion
 * - etc.
 */
public interface GroupSelectionStrategy {
    
    /**
     * Select multiple groups from the list of available groups.
     * 
     * The number of groups to select is determined by the index configuration
     * (e.g., if index wants 15 replicas with 5 nodes per group, select 3 groups)
     * 
     * @param groups List of groups (already filtered by NodeRole)
     * @param targetRole Target role (for logging/context)
     * @param numGroupsNeeded Number of groups needed for allocation
     * @return List of selected groups (may be less than numGroupsNeeded if not enough groups available)
     */
    List<NodesGroup> selectGroups(List<NodesGroup> groups, NodeRole targetRole, int numGroupsNeeded);
    
    /**
     * Get the name of this strategy (for logging).
     */
    String getStrategyName();
}

