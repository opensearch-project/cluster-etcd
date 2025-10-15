package io.clustercontroller;

import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import io.clustercontroller.tasks.TaskFactory;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Generic task manager for scheduling and executing tasks.
 * Agnostic to specific task types - delegates execution to Task implementations.
 */
@Slf4j
public class TaskManager {
    
    private final MetadataStore metadataStore;
    private final TaskContext taskContext;
    private final String clusterName;
    
    private final ScheduledExecutorService scheduler;
    private final long intervalSeconds;
    private boolean isRunning = false;
    
    public TaskManager(MetadataStore metadataStore, TaskContext taskContext, String clusterName, long intervalSeconds) {
        this.metadataStore = metadataStore;
        this.taskContext = taskContext;
        this.clusterName = clusterName;
        this.intervalSeconds = intervalSeconds;
        this.scheduler = Executors.newScheduledThreadPool(1);
    }
    
    public TaskMetadata createTask(String taskName, String input, int priority) {
        log.info("Creating task: name={}, priority={}", taskName, priority);
        TaskMetadata taskMetadata = new TaskMetadata(taskName, priority);
        taskMetadata.setInput(input);
        try {
            metadataStore.createTask(clusterName, taskMetadata);
        } catch (Exception e) {
            log.error("Failed to create task: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create task", e);
        }
        return taskMetadata;
    }
    
    public List<TaskMetadata> getAllTasks() {
        log.debug("Getting all tasks");
        try {
            return metadataStore.getAllTasks(clusterName);
        } catch (Exception e) {
            log.error("Failed to get all tasks: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to get tasks", e);
        }
    }
    
    public Optional<TaskMetadata> getTask(String taskName) {
        log.debug("Getting task: {}", taskName);
        try {
            return metadataStore.getTask(clusterName, taskName);
        } catch (Exception e) {
            log.error("Failed to get task {}: {}", taskName, e.getMessage(), e);
            throw new RuntimeException("Failed to get task", e);
        }
    }
    
    public void updateTask(TaskMetadata taskMetadata) {
        log.debug("Updating task: {}", taskMetadata.getName());
        try {
            metadataStore.updateTask(clusterName, taskMetadata);
        } catch (Exception e) {
            log.error("Failed to update task {}: {}", taskMetadata.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to update task", e);
        }
    }
    
    public void deleteTask(String taskName) {
        log.info("Deleting task: {}", taskName);
        try {
            metadataStore.deleteTask(clusterName, taskName);
        } catch (Exception e) {
            log.error("Failed to delete task {}: {}", taskName, e.getMessage(), e);
            throw new RuntimeException("Failed to delete task", e);
        }
    }
    
    public void start() {
        log.info("[Cluster: {}] Starting task manager", clusterName);
        
        // Bootstrap standard recurring tasks if they don't exist
        bootstrapRecurringTasks();
        
        isRunning = true;
        scheduler.scheduleWithFixedDelay(
                this::processTaskLoop,
                0,
                intervalSeconds,
                TimeUnit.SECONDS
        );
    }
    
    /**
     * Bootstrap standard recurring tasks needed for normal cluster operation.
     * These tasks are created automatically if they don't already exist.
     */
    private void bootstrapRecurringTasks() {
        log.info("[Cluster: {}] Bootstrapping recurring tasks", clusterName);
        
        try {
            // 1. Discovery task - discovers search units from actual-state (highest priority)
            ensureRecurringTask(TASK_ACTION_DISCOVERY, 1, "Discover search units from etcd");
            
            // 2. Plan Shard Allocation - plans shard allocation based on discovered nodes
            ensureRecurringTask(TASK_ACTION_PLAN_SHARD_ALLOCATION, 2, "Plan shard allocation");
            
            log.info("[Cluster: {}] Successfully bootstrapped recurring tasks", clusterName);
        } catch (Exception e) {
            log.error("[Cluster: {}] Failed to bootstrap recurring tasks: {}", clusterName, e.getMessage(), e);
            // Don't throw - allow TaskManager to start even if bootstrap fails
        }
    }
    
    /**
     * Ensure a recurring task exists, creating it if necessary.
     */
    private void ensureRecurringTask(String taskName, int priority, String description) {
        try {
            if (getTask(taskName).isPresent()) {
                log.debug("[Cluster: {}] Task {} already exists", clusterName, taskName);
                return;
            }
            
            // Create recurring task
            TaskMetadata task = new TaskMetadata(taskName, priority);
            task.setSchedule(TASK_SCHEDULE_REPEAT);
            task.setInput(description);
            
            metadataStore.createTask(clusterName, task);
            log.info("[Cluster: {}] Created recurring task: {} (priority: {})", clusterName, taskName, priority);
        } catch (Exception e) {
            log.error("[Cluster: {}] Failed to ensure recurring task {}: {}", clusterName, taskName, e.getMessage());
        }
    }
    
    public void stop() {
        log.info("[Cluster: {}] Stopping task manager", clusterName);
        isRunning = false;
        scheduler.shutdown();
        // NOTE: Do NOT close metadataStore here - it's a shared resource used by all TaskManagers
        // The metadataStore will be closed when the application shuts down
    }
    
    public boolean isRunning() {
        return isRunning;
    }
    
    private void processTaskLoop() {
        try {
            // TODO: Leader check disabled for multi-cluster mode
            // In multi-cluster mode, MultiClusterManager handles cluster ownership via distributed locks
            // If reverting to single-cluster mode, uncomment the following:
            // if (!metadataStore.isLeader()) {
            //     log.debug("Skipping task processing - not the leader");
            //     return;
            // }
            
            log.info("[Cluster: {}] Running task processing loop - checking for tasks", clusterName);
            
            List<TaskMetadata> taskMetadataList = getAllTasks();
            log.info("[Cluster: {}] Found {} tasks in etcd", clusterName, taskMetadataList.size());
            for (TaskMetadata task : taskMetadataList) {
                log.info("[Cluster: {}] Task: {} status: {} priority: {}", clusterName, task.getName(), task.getStatus(), task.getPriority());
            }
            
            cleanupOldTasks(taskMetadataList);
            
            TaskMetadata taskMetadataToProcess = selectNextTask(taskMetadataList);
            if (taskMetadataToProcess != null) {
                log.info("[Cluster: {}] Processing task: {}", clusterName, taskMetadataToProcess.getName());
                String result = executeTask(taskMetadataToProcess);
                log.info("[Cluster: {}] Task {} completed with result: {}", clusterName, taskMetadataToProcess.getName(), result);
            } else {
                log.info("[Cluster: {}] No pending tasks to process", clusterName);
            }
        } catch (Exception e) {
            log.error("[Cluster: {}] Error in task processing loop: {}", clusterName, e.getMessage(), e);
        }
    }
    
    private String executeTask(TaskMetadata taskMetadata) {
        try {
            taskMetadata.setStatus(TASK_STATUS_RUNNING);
            updateTask(taskMetadata);
            
            log.info("Executing task: {}", taskMetadata.getName());
            
            // Create Task implementation from metadata and execute
            Task task = TaskFactory.createTask(taskMetadata);
            String result = task.execute(taskContext);
            
            // Make repeat tasks eligible again by resetting status to pending
            if (TASK_SCHEDULE_REPEAT.equalsIgnoreCase(taskMetadata.getSchedule())) {
                taskMetadata.setStatus(TASK_STATUS_PENDING);
            } else {
                taskMetadata.setStatus(result);
            }
            
            // Update timestamp so task selection considers recency
            taskMetadata.setLastUpdated(OffsetDateTime.now(ZoneOffset.UTC));
            updateTask(taskMetadata);
            return result;
            
        } catch (Exception e) {
            log.error("Failed to execute task {}: {}", taskMetadata.getName(), e.getMessage(), e);
            taskMetadata.setStatus(TASK_STATUS_FAILED);
            try {
                updateTask(taskMetadata);
            } catch (Exception updateException) {
                log.error("Failed to update task status to failed: {}", updateException.getMessage());
            }
            return TASK_STATUS_FAILED;
        }
    }
    
    private TaskMetadata selectNextTask(List<TaskMetadata> tasks) {
        // Select tasks based on "effective time" = lastUpdated + priority weight
        // This allows repeat tasks to alternate naturally based on priority + age
        // Lower effective time = higher priority (should run sooner)
        return tasks.stream()
                .filter(t -> TASK_SCHEDULE_REPEAT.equals(t.getSchedule()) || TASK_STATUS_PENDING.equals(t.getStatus()))
                .min(Comparator.comparingLong(t -> {
                    long lastUpdated = t.getLastUpdated() != null 
                        ? t.getLastUpdated().toInstant().toEpochMilli() 
                        : 0;
                    // Weight priority: higher priority (lower number) = run sooner
                    // Each priority level adds 1 second (1000ms) delay
                    return lastUpdated + t.getPriority() * 1000L;
                }))
                .orElse(null);
    }
    
    private void cleanupOldTasks(List<TaskMetadata> tasks) {
        // TODO: Implement task cleanup logic
        log.debug("Cleaning up old tasks");
    }
}
