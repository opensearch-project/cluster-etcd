package io.clustercontroller.tasks;

import io.clustercontroller.allocation.ActualAllocationUpdater;
import io.clustercontroller.allocation.ShardAllocator;
import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.indices.IndexManager;
import io.clustercontroller.orchestration.GoalStateOrchestrator;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Context object providing access to shared components for task execution.
 * 
 * This is a singleton that contains stateless or cluster-agnostic services.
 * The cluster-specific context (clusterId) is passed separately to task execution.
 */
@Getter
@AllArgsConstructor
public class TaskContext {
    
    private final IndexManager indexManager;
    private final ShardAllocator shardAllocator;
    private final ActualAllocationUpdater actualAllocationUpdater;
    private final GoalStateOrchestrator goalStateOrchestrator;
    private final Discovery discovery;
}




