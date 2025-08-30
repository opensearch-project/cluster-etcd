package io.clustercontroller.tasks;

import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.tasks.impl.*;
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
            case TASK_ACTION_CREATE_INDEX -> new CreateIndexTask(
                metadata.getName(),
                metadata.getPriority(),
                metadata.getInput(),
                metadata.getSchedule()
            );
            case TASK_ACTION_DELETE_INDEX -> new DeleteIndexTask(
                metadata.getName(),
                metadata.getPriority(),
                metadata.getInput(),
                metadata.getSchedule()
            );
            case TASK_ACTION_DISCOVER_SEARCH_UNIT -> new DiscoverSearchUnitTask(
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
            case TASK_ACTION_PLAN_SHARD_ALLOCATION -> new PlanShardAllocationTask(
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
