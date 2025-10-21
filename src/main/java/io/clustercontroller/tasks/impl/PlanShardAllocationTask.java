package io.clustercontroller.tasks.impl;

import io.clustercontroller.allocation.AllocationStrategy;
import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

/**
 * Task implementation for planning shard allocation.
 */
@Slf4j
@Getter
@AllArgsConstructor
public class PlanShardAllocationTask implements Task {
    
    private final String name;
    private final int priority;
    private final String input;
    private final String schedule;
    
    @Override
    public String execute(TaskContext context, String clusterId) {
        log.info("Executing plan shard allocation task: {} for cluster: {}", name, clusterId);
        
        try {
            // Run shard allocation planning for all indices in the cluster
            // Use RESPECT_REPLICA_COUNT strategy to honor the replica count in index configuration
            context.getShardAllocator().planShardAllocation(
                clusterId, 
                AllocationStrategy.RESPECT_REPLICA_COUNT
            );
            log.info("Shard allocation planning completed successfully for cluster: {}", clusterId);
            return TASK_STATUS_COMPLETED;
        } catch (Exception e) {
            log.error("Failed to execute plan shard allocation task for cluster {}: {}", clusterId, e.getMessage(), e);
            return TASK_STATUS_FAILED;
        }
    }
}

