package io.clustercontroller.tasks;

import io.clustercontroller.allocation.ActualAllocationUpdater;
import io.clustercontroller.allocation.ShardAllocator;
import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.indices.IndexManager;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Context object providing access to components for task execution.
 */
@Getter
@AllArgsConstructor
public class TaskContext {
    
    private final IndexManager indexManager;
    private final Discovery discovery;
    private final ShardAllocator shardAllocator;
    private final ActualAllocationUpdater actualAllocationUpdater;
}
