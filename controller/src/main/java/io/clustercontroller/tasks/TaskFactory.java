package io.clustercontroller.tasks;

import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.tasks.impl.ActualAllocationUpdaterTask;
import io.clustercontroller.tasks.impl.DiscoveryTask;
import io.clustercontroller.tasks.impl.GoalStateOrchestratorTask;
import io.clustercontroller.tasks.impl.ShardAllocatorTask;
import io.clustercontroller.tasks.impl.UnknownTask;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

/**
 * Factory for creating Task implementations from TaskMetadata.
 */
@Slf4j
public class TaskFactory {
    
    /**
     * Create a Task implementation from TaskMetadata
     */
    public static Task createTask(TaskMetadata metadata) {
        String taskName = metadata.getName();
        
        return switch (taskName) {
            case TASK_ACTION_DISCOVERY -> new DiscoveryTask(
                metadata.getName(),
                metadata.getPriority(),
                metadata.getInput(),
                metadata.getSchedule()
            );
            case TASK_ACTION_SHARD_ALLOCATOR -> new ShardAllocatorTask(
                metadata.getName(),
                metadata.getPriority(),
                metadata.getInput(),
                metadata.getSchedule()
            );
            case TASK_ACTION_ACTUAL_ALLOCATION_UPDATER -> new ActualAllocationUpdaterTask(
                metadata.getName(),
                metadata.getPriority(),
                metadata.getInput(),
                metadata.getSchedule()
            );
            case TASK_ACTION_GOAL_STATE_ORCHESTRATOR -> new GoalStateOrchestratorTask(
                metadata.getName(),
                metadata.getPriority(),
                metadata.getInput(),
                metadata.getSchedule()
            );
            default -> {
                log.warn("Unknown task type: {}", taskName);
                yield new UnknownTask(metadata.getName(), metadata.getPriority(), metadata.getInput(), metadata.getSchedule());
            }
        };
    }
}



