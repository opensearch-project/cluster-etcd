package io.clustercontroller.tasks.impl;

import io.clustercontroller.orchestration.GoalStateOrchestrator;
import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

/**
 * Task to execute goal state orchestration
 */
@Slf4j
@Getter
@AllArgsConstructor
public class GoalStateOrchestratorTask implements Task {
    
    private final String name;
    private final int priority;
    private final String input;
    private final String schedule;
    
    @Override
    public String execute(TaskContext context, String clusterId) {
        log.info("Executing goal state orchestrator task: {} for cluster: {}", name, clusterId);
        
        try {
            context.getGoalStateOrchestrator().orchestrateGoalStates(clusterId);
            return TASK_STATUS_COMPLETED;
        } catch (Exception e) {
            log.error("Failed to execute goal state orchestrator task for cluster {}: {}", clusterId, e.getMessage(), e);
            return TASK_STATUS_FAILED;
        }
    }
}
