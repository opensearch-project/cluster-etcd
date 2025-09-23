package io.clustercontroller;

import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import io.clustercontroller.tasks.TaskFactory;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

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
            
            List<TaskMetadata> taskMetadataList = getAllTasks();
            cleanupOldTasks(taskMetadataList);
            
            TaskMetadata taskMetadataToProcess = selectNextTask(taskMetadataList);
            if (taskMetadataToProcess != null) {
                log.info("Processing task: {}", taskMetadataToProcess.getName());
                String result = executeTask(taskMetadataToProcess);
                log.info("Task {} completed with result: {}", taskMetadataToProcess.getName(), result);
            } else {
                log.debug("No pending tasks to process");
            }
        } catch (Exception e) {
            log.error("Error in task processing loop: {}", e.getMessage(), e);
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
            
            taskMetadata.setStatus(result);
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
    
    private void cleanupOldTasks(List<TaskMetadata> tasks) {
        // TODO: Implement task cleanup logic
        log.debug("Cleaning up old tasks");
    }
}
