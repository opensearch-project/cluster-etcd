package io.clustercontroller.tasks.impl;

import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

/**
 * Task implementation for creating indices.
 */
@Slf4j
@Getter
@AllArgsConstructor
public class CreateIndexTask implements Task {
    
    private final String name;
    private final int priority;
    private final String input;
    private final String schedule;
    
    @Override
    public String execute(TaskContext context) {
        log.info("Executing create index task: {}", name);
        
        try {
            context.getIndexManager().createIndex(input);
            return TASK_STATUS_COMPLETED;
        } catch (Exception e) {
            log.error("Failed to execute create index task: {}", e.getMessage(), e);
            return TASK_STATUS_FAILED;
        }
    }
}
