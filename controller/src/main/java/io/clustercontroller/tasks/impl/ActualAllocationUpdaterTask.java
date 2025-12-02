package io.clustercontroller.tasks.impl;

import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

/**
 * Task implementation for updating actual allocations.
 */
@Slf4j
@Getter
@AllArgsConstructor
public class ActualAllocationUpdaterTask implements Task {
    
    private final String name;
    private final int priority;
    private final String input;
    private final String schedule;
    
    @Override
    public String execute(TaskContext context, String clusterId) {
        log.info("Executing actual allocation updater task: {} for cluster: {}", name, clusterId);
        
        try {
            context.getActualAllocationUpdater().updateActualAllocations(clusterId);
            return TASK_STATUS_COMPLETED;
        } catch (Exception e) {
            log.error("Failed to execute actual allocation updater task for cluster {}: {}", clusterId, e.getMessage(), e);
            return TASK_STATUS_FAILED;
        }
    }
}
