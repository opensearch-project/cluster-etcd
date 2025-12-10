package io.clustercontroller.tasks.impl;

import io.clustercontroller.allocation.AllocationStrategy;
import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

/**
 * Task implementation for shard allocation.
 */
@Slf4j
@Getter
@AllArgsConstructor
public class ShardAllocatorTask implements Task {
    
    private final String name;
    private final int priority;
    private final String input;
    private final String schedule;
    
    @Override
    public String execute(TaskContext context, String clusterId) {
        log.info("Executing shard allocator task: {} for cluster: {}", name, clusterId);
        
        try {
            AllocationStrategy strategy = AllocationStrategy.USE_ALL_AVAILABLE_NODES; // TODO: Pick from config later
            context.getShardAllocator().planShardAllocation(clusterId, strategy);
            return TASK_STATUS_COMPLETED;
        } catch (Exception e) {
            log.error("Failed to execute shard allocator task for cluster {}: {}", clusterId, e.getMessage(), e);
            return TASK_STATUS_FAILED;
        }
    }
}


