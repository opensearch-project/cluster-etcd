package io.clustercontroller;

import io.clustercontroller.allocation.ActualAllocationUpdater;
import io.clustercontroller.allocation.ShardAllocator;
import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.indices.IndexManager;
import io.clustercontroller.models.Task;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Main task manager for cluster controller.
 * Handles task lifecycle, scheduling, and coordination with other components.
 */
@Slf4j
public class TaskManager {
    
    private final MetadataStore metadataStore;
    private final IndexManager indexManager;
    private final Discovery discovery;
    private final ShardAllocator shardAllocator;
    private final ActualAllocationUpdater actualAllocationUpdater;
    
    private final ScheduledExecutorService scheduler;
    private final long intervalSeconds;
    private boolean isRunning = false;
    
    public TaskManager(
            MetadataStore metadataStore,
            IndexManager indexManager,
            Discovery discovery,
            ShardAllocator shardAllocator,
            ActualAllocationUpdater actualAllocationUpdater,
            long intervalSeconds) {
        this.metadataStore = metadataStore;
        this.indexManager = indexManager;
        this.discovery = discovery;
        this.shardAllocator = shardAllocator;
        this.actualAllocationUpdater = actualAllocationUpdater;
        this.intervalSeconds = intervalSeconds;
        this.scheduler = Executors.newScheduledThreadPool(1);
    }
    
    public Task createTask(String taskName, String input, int priority) {
        log.info("Creating task: name={}, priority={}", taskName, priority);
        Task task = new Task(taskName, priority);
        task.setInput(input);
        try {
            metadataStore.createTask(task);
        } catch (Exception e) {
            log.error("Failed to create task: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create task", e);
        }
        return task;
    }
    
    public List<Task> getAllTasks() {
        log.debug("Getting all tasks");
        try {
            return metadataStore.getAllTasks();
        } catch (Exception e) {
            log.error("Failed to get all tasks: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to get tasks", e);
        }
    }
    
    public Optional<Task> getTask(String taskName) {
        log.debug("Getting task: {}", taskName);
        try {
            return metadataStore.getTask(taskName);
        } catch (Exception e) {
            log.error("Failed to get task {}: {}", taskName, e.getMessage(), e);
            throw new RuntimeException("Failed to get task", e);
        }
    }
    
    public void updateTask(Task task) {
        log.debug("Updating task: {}", task.getName());
        try {
            metadataStore.updateTask(task);
        } catch (Exception e) {
            log.error("Failed to update task {}: {}", task.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to update task", e);
        }
    }
    
    public void deleteTask(String taskName) {
        log.info("Deleting task: {}", taskName);
        try {
            metadataStore.deleteTask(taskName);
        } catch (Exception e) {
            log.error("Failed to delete task {}: {}", taskName, e.getMessage(), e);
            throw new RuntimeException("Failed to delete task", e);
        }
    }
    
    public void start() {
        log.info("Starting task manager");
        isRunning = true;
        scheduler.scheduleWithFixedDelay(
                this::processTaskLoop,
                0,
                intervalSeconds,
                TimeUnit.SECONDS
        );
    }
    
    public void stop() {
        log.info("Stopping task manager");
        isRunning = false;
        scheduler.shutdown();
        try {
            metadataStore.close();
        } catch (Exception e) {
            log.error("Error closing metadata store: {}", e.getMessage());
        }
    }
    
    private void processTaskLoop() {
        try {
            log.debug("Running task processing loop");
            
            List<Task> tasks = getAllTasks();
            cleanupOldTasks(tasks);
            
            Task taskToProcess = selectNextTask(tasks);
            if (taskToProcess != null) {
                log.info("Processing task: {}", taskToProcess.getName());
                String result = executeTask(taskToProcess);
                log.info("Task {} completed with result: {}", taskToProcess.getName(), result);
            } else {
                log.debug("No pending tasks to process");
            }
        } catch (Exception e) {
            log.error("Error in task processing loop: {}", e.getMessage(), e);
        }
    }
    
    private String executeTask(Task task) {
        try {
            task.setStatus(TASK_STATUS_RUNNING);
            updateTask(task);
            
            String action = task.getName();
            log.info("Executing action: {}", action);
            
            switch (action) {
                case TASK_ACTION_CREATE_INDEX:
                    indexManager.createIndex(task.getInput());
                    break;
                case TASK_ACTION_DELETE_INDEX:
                    indexManager.deleteIndex(task.getInput());
                    break;
                case TASK_ACTION_PLAN_SHARD_ALLOCATION:
                    indexManager.planShardAllocation();
                    break;
                case TASK_ACTION_DISCOVER_SEARCH_UNIT:
                    discovery.discoverSearchUnits();
                    break;
                case TASK_ACTION_SHARD_ALLOCATOR:
                    shardAllocator.allocateShards();
                    break;
                case TASK_ACTION_ACTUAL_ALLOCATION_UPDATER:
                    actualAllocationUpdater.updateActualAllocations();
                    break;
                default:
                    log.warn("Unknown task action: {}", action);
                    return TASK_STATUS_FAILED;
            }
            
            task.setStatus(TASK_STATUS_COMPLETED);
            updateTask(task);
            return TASK_STATUS_COMPLETED;
            
        } catch (Exception e) {
            log.error("Failed to execute task {}: {}", task.getName(), e.getMessage(), e);
            task.setStatus(TASK_STATUS_FAILED);
            try {
                updateTask(task);
            } catch (Exception updateException) {
                log.error("Failed to update task status to failed: {}", updateException.getMessage());
            }
            return TASK_STATUS_FAILED;
        }
    }
    
    private Task selectNextTask(List<Task> tasks) {
        // Select highest priority pending task
        return tasks.stream()
                .filter(task -> TASK_STATUS_PENDING.equals(task.getStatus()) || 
                               (TASK_SCHEDULE_REPEAT.equals(task.getSchedule()) && TASK_STATUS_COMPLETED.equals(task.getStatus())))
                .min((t1, t2) -> {
                    int priorityCompare = Integer.compare(t1.getPriority(), t2.getPriority());
                    if (priorityCompare != 0) return priorityCompare;
                    return t1.getLastUpdated().compareTo(t2.getLastUpdated());
                })
                .orElse(null);
    }
    
    private void cleanupOldTasks(List<Task> tasks) {
        // TODO: Implement task cleanup logic
        log.debug("Cleaning up old tasks");
    }
}
