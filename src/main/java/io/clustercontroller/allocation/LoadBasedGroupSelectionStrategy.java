package io.clustercontroller.allocation;

import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.NodesGroup;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;

/**
 * Load-based group selection strategy (bin-packing).
 * 
 * TODO: NOT YET IMPLEMENTED
 * 
 * When implemented, this should:
 * - Select N groups with the lowest current load
 * - Implement classic bin-packing to balance load across groups
 * - Sort groups by currentLoad ascending and return top N
 */
@Slf4j
public class LoadBasedGroupSelectionStrategy implements GroupSelectionStrategy {
    
    @Override
    public List<NodesGroup> selectGroups(List<NodesGroup> groups, NodeRole targetRole, int numGroupsNeeded) {
        // TODO: Implement load-based bin-packing selection
        // 
        // List<NodesGroup> selected = groups.stream()
        //     .sorted(Comparator.comparingInt(NodesGroup::getCurrentLoad))
        //     .limit(numGroupsNeeded)
        //     .collect(Collectors.toList());
        // 
        // log.debug("LoadBased strategy selected {} groups for role={}", selected.size(), targetRole);
        // return selected;
        
        log.error("Load-based group selection is not yet implemented. Returning empty list to avoid blocking allocation thread.");
        return List.of();
    }
    
    @Override
    public String getStrategyName() {
        return "LoadBased";
    }
}

