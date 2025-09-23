package io.clustercontroller.tasks.impl;

import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

/**
 * Task implementation for discovering search units.
 */
@Slf4j
@Getter
@AllArgsConstructor
public class DiscoverSearchUnitTask implements Task {
    
    private final String name;
    private final int priority;
    private final String input;
    private final String schedule;
    
    @Override
    public String execute(TaskContext context) {
        log.info("Executing discover search unit task: {}", name);
        
        try {
            context.getDiscovery().discoverSearchUnits();
            return TASK_STATUS_COMPLETED;
        } catch (Exception e) {
            log.error("Failed to execute discover search unit task: {}", e.getMessage(), e);
            return TASK_STATUS_FAILED;
        }
    }
}

