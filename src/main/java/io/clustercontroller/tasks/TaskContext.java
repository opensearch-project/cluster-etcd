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
 * In multi-cluster mode, each cluster gets its own TaskContext instance with
 * the appropriate clusterId. This ensures tasks always operate on the correct
 * cluster data paths.
 */
@Getter
@AllArgsConstructor
public class TaskContext {
    
    private final String clusterName;  // Cluster identifier for this context
    private final IndexManager indexManager;
    private final ShardAllocator shardAllocator;
    private final ActualAllocationUpdater actualAllocationUpdater;
    private final GoalStateOrchestrator goalStateOrchestrator;
    private final Discovery discovery;
}




