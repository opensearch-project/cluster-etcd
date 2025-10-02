package io.clustercontroller;

import io.clustercontroller.config.Constants;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.models.NodeAttributes;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.tasks.Task;
import io.clustercontroller.tasks.TaskContext;
import io.clustercontroller.tasks.TaskFactory;
import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        scheduler.scheduleWithFixedDelay(
                this::processTaskLoop,
                0,
                intervalSeconds,
                TimeUnit.SECONDS
        );
    }
    
    public void stop() {
        log.info("Stopping task manager");
        scheduler.shutdown();
        try {
            metadataStore.close();
        } catch (Exception e) {
            log.error("Error closing metadata store: {}", e.getMessage());
        }
    }
    
    private void processTaskLoop() {
        try {
            log.info("Running task processing loop - checking for tasks");
            
            List<TaskMetadata> taskMetadataList = getAllTasks();
            log.info("Found {} tasks in etcd", taskMetadataList.size());
            for (TaskMetadata task : taskMetadataList) {
                log.info("Task: {} status: {} priority: {}", task.getName(), task.getStatus(), task.getPriority());
            }
            
            cleanupOldTasks(taskMetadataList);
            
            TaskMetadata taskMetadataToProcess = selectNextTask(taskMetadataList);
            if (taskMetadataToProcess != null) {
                log.info("Processing task: {}", taskMetadataToProcess.getName());
                String result = executeTask(taskMetadataToProcess);
                log.info("Task {} completed with result: {}", taskMetadataToProcess.getName(), result);
            } else {
                log.info("No pending tasks to process");
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
        // Simple priority-based selection - execute all repeat tasks regardless of status
        // This prevents priority inversion and task starvation
        return tasks.stream()
                .filter(task -> TASK_SCHEDULE_REPEAT.equals(task.getSchedule()) || TASK_STATUS_PENDING.equals(task.getStatus()))
                .min((t1, t2) -> {
                    // Compare by priority (lower number = higher priority)
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
    
    // ========== Discovery Methods ==========
    
    /**
     * Discover search units from cluster topology.
     * This method contains the core discovery logic previously in Discovery class.
     */
    public void discoverSearchUnits() throws Exception {
        log.info("Discovery - Starting search unit discovery process");
        
        // Discover and update search units from Etcd actual-states
        discoverSearchUnitsFromEtcd();
        
        // Clean up stale search units before processing
        cleanupStaleSearchUnits();
        
        // Process all search units to ensure they're up-to-date
        processAllSearchUnits();
        
        log.info("Discovery - Completed search unit discovery process");
    }
    
    /**
     * Process all search units to ensure they're current
     */
    private void processAllSearchUnits() {
        try {
            List<SearchUnit> allSearchUnits = metadataStore.getAllSearchUnits(clusterName);
            log.info("Discovery - Processing {} total search units for updates", allSearchUnits.size());
            
            for (SearchUnit searchUnit : allSearchUnits) {
                try {
                    log.debug("Discovery - Processing search unit: {}", searchUnit.getName());
                    
                    // Update the search unit (this could include health checks, metrics, etc.)
                    metadataStore.updateSearchUnit(clusterName, searchUnit);
                    
                    log.debug("Discovery - Successfully updated search unit: {}", searchUnit.getName());
                } catch (Exception e) {
                    log.error("Discovery - Failed to update search unit {}: {}", searchUnit.getName(), e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Discovery - Failed to process all search units: {}", e.getMessage());
        }
    }

    /**
     * Dynamically discover search units from Etcd actual-states
     */
    private void discoverSearchUnitsFromEtcd() {
        try {
            log.info("Discovery - Discovering search units from Etcd...");
            // fetch search units from actual-state paths
            List<SearchUnit> etcdSearchUnits = fetchSearchUnitsFromEtcd(); 
            log.info("Discovery - Found {} search units from Etcd", etcdSearchUnits.size());
            
            // Update/create search units in metadata store
            for (SearchUnit searchUnit : etcdSearchUnits) {
                try {
                    if (metadataStore.getSearchUnit(clusterName, searchUnit.getName()).isPresent()) {
                        log.debug("Discovery - Updating existing search unit '{}' from Etcd", searchUnit.getName());
                        metadataStore.updateSearchUnit(clusterName, searchUnit);
                    } else {
                        log.info("Discovery - Creating new search unit '{}' from Etcd", searchUnit.getName());
                        metadataStore.upsertSearchUnit(clusterName, searchUnit.getName(), searchUnit);
                    }
                } catch (Exception e) {
                    log.warn("Discovery - Failed to update search unit '{}' from Etcd: {}", 
                        searchUnit.getName(), e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Discovery - Failed to discover search units from Etcd: {}", e.getMessage());
        }
    }

    /**
     * Fetch search units from Etcd using actual-state paths
     * Public method to allow reuse by SearchUnitLoader for bootstrapping
     */
    public List<SearchUnit> fetchSearchUnitsFromEtcd() {
        log.info("Discovery - Fetching search units from Etcd...");
        
        try {
            Map<String, SearchUnitActualState> actualStates = 
                    metadataStore.getAllSearchUnitActualStates(clusterName);
            
            List<SearchUnit> searchUnits = new ArrayList<>();
            
            for (Map.Entry<String, SearchUnitActualState> entry : actualStates.entrySet()) {
                String unitName = entry.getKey();
                SearchUnitActualState actualState = entry.getValue();
                
                try {
                    // Convert to SearchUnit
                    SearchUnit searchUnit = convertActualStateToSearchUnit(actualState, unitName);
                    if (searchUnit != null) {
                        searchUnits.add(searchUnit);
                    }
                } catch (Exception e) {
                    log.warn("Discovery - Failed to convert actual state for unit {}: {}", unitName, e.getMessage());
                }
            }
            
            log.info("Discovery - Successfully fetched {} search units from Etcd actual-states", searchUnits.size());
            return searchUnits;
            
        } catch (Exception e) {
            log.error("Discovery - Failed to fetch search units from Etcd: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }
    
    /**
     * Convert SearchUnitActualState to SearchUnit object
     */
    private SearchUnit convertActualStateToSearchUnit(SearchUnitActualState actualState, String unitName) {
        SearchUnit searchUnit = new SearchUnit();
        
        // Basic node identification
        searchUnit.setName(unitName);
        searchUnit.setHost(actualState.getAddress());
        searchUnit.setPortHttp(actualState.getPort());
        
        // Extract role, shard_id, and cluster_name directly from actual state (populated by worker)
        searchUnit.setRole(actualState.getRole());
        searchUnit.setShardId(actualState.getShardId());
        searchUnit.setClusterName(actualState.getClusterName());
        
        // Set node state directly from deriveNodeState 
        HealthState statePulled = actualState.deriveNodeState();
        searchUnit.setStatePulled(statePulled);
        
        // Set admin state based on health
        searchUnit.setStateAdmin(actualState.deriveAdminState());
        
        // Set node attributes based on role
        Map<String, String> attributes = NodeAttributes.getAttributesForRole(searchUnit.getRole());
        searchUnit.setNodeAttributes(new HashMap<>(attributes));
        
        log.debug("Discovery - Converted actual state to SearchUnit: {} (role: {}, shard: {}, state: {})", 
                unitName, searchUnit.getRole(), searchUnit.getShardId(), searchUnit.getStatePulled());
        
        return searchUnit;
    }
    
    /**
     * Clean up search units with missing or stale actual state timestamp (older than configured timeout)
     */
    private void cleanupStaleSearchUnits() {
        try {
            log.info("Discovery - Starting cleanup of stale search units...");
            
            // Get all existing search units from metadata store
            List<SearchUnit> allSearchUnits = metadataStore.getAllSearchUnits(clusterName);
            int deletedCount = 0;
            
            for (SearchUnit searchUnit : allSearchUnits) {
                String unitName = searchUnit.getName();
                
                try {
                    // Check if actual state exists
                    java.util.Optional<SearchUnitActualState> actualStateOpt = 
                            metadataStore.getSearchUnitActualState(clusterName, unitName);
                    
                    boolean shouldDelete = false;
                    String reason = "";
                    
                    if (!actualStateOpt.isPresent()) {
                        // Case 1: Missing actual state
                        shouldDelete = true;
                        reason = "missing actual state";
                    } else {
                        // Case 2: Check if timestamp is older than configured timeout
                        SearchUnitActualState actualState = actualStateOpt.get();
                        if (isActualStateStale(actualState)) {
                            shouldDelete = true;
                            reason = "stale timestamp (older than " + Constants.STALE_SEARCH_UNIT_TIMEOUT_MINUTES + " minutes)";
                        }
                    }
                    
                    if (shouldDelete) {
                        log.info("Discovery - Deleting search unit '{}' due to: {}", unitName, reason);
                        metadataStore.deleteSearchUnit(clusterName, unitName);
                        deletedCount++;
                    }
                    
                } catch (Exception e) {
                    log.error("Discovery - Failed to check/delete search unit '{}': {}", unitName, e.getMessage());
                }
            }
            
            log.info("Discovery - Cleanup completed. Deleted {} stale search units", deletedCount);
            
        } catch (Exception e) {
            log.error("Discovery - Failed to cleanup stale search units: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Check if actual state timestamp is older than the configured timeout
     */
    private boolean isActualStateStale(SearchUnitActualState actualState) {
        long currentTime = System.currentTimeMillis();
        long nodeTimestamp = actualState.getTimestamp();
        long timeDiff = currentTime - nodeTimestamp;
        long timeoutInMs = Constants.STALE_SEARCH_UNIT_TIMEOUT_MINUTES * 60 * 1000; // Convert minutes to milliseconds
        
        boolean isStale = timeDiff > timeoutInMs;
        
        if (isStale) {
            log.debug("Discovery - Actual state is stale: timestamp={}, age={}ms ({}min), threshold={}ms ({}min)", 
                nodeTimestamp, timeDiff, timeDiff / (60 * 1000), timeoutInMs, Constants.STALE_SEARCH_UNIT_TIMEOUT_MINUTES);
        }
        
        return isStale;
    }

    /**
     * Monitor cluster health - placeholder for future implementation
     */
    public void monitorClusterHealth() {
        log.info("Monitoring cluster health");
        // TODO: Implement cluster health monitoring logic
    }
    
    /**
     * Update cluster topology - placeholder for future implementation
     */
    public void updateClusterTopology() {
        log.info("Updating cluster topology state");
        // TODO: Implement cluster topology update logic
    }
}
