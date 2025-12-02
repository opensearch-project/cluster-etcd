package io.clustercontroller.tasks.impl;

import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

/**
 * Task implementation for discovering search units from etcd.
 * This task regularly scans etcd for actual-state updates and updates the SearchUnit inventory.
 */
@Slf4j
@Getter
@AllArgsConstructor
public class DiscoveryTask implements Task {
    
    private final String name;
    private final int priority;
    private final String input;
    private final String schedule;
    
    @Override
    public String execute(TaskContext context, String clusterId) {
        log.info("Executing discovery task: {} for cluster: {}", name, clusterId);
        
        try {
            context.getDiscovery().discoverSearchUnits(clusterId);
            log.info("Discovery task completed successfully for cluster: {}", clusterId);
            return TASK_STATUS_COMPLETED;
        } catch (Exception e) {
            log.error("Failed to execute discovery task for cluster {}: {}", clusterId, e.getMessage(), e);
            return TASK_STATUS_FAILED;
        }
    }
}

