package io.clustercontroller.tasks;

import io.clustercontroller.allocation.ActualAllocationUpdater;
import io.clustercontroller.allocation.ShardAllocator;
import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.indices.IndexManager;
import io.clustercontroller.orchestration.GoalStateOrchestrator;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Context object providing access to components for task execution.
 * 
 * TODO: Remove clusterName field - it's a legacy from single-cluster mode.
 * In multi-cluster setup, TaskContext is shared across all clusters, so having
 * a hardcoded clusterName doesn't make sense. The actual cluster context should
 * come from the task data itself (TaskManager passes clusterId separately).
 */
@Getter
@AllArgsConstructor
public class TaskContext {
    
    private final String clusterName;  // TODO: Remove - legacy field, not used in multi-cluster
    private final IndexManager indexManager;
    private final ShardAllocator shardAllocator;
    private final ActualAllocationUpdater actualAllocationUpdater;
    private final GoalStateOrchestrator goalStateOrchestrator;
    private final Discovery discovery;
}




