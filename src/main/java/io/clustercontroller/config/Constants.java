package io.clustercontroller.config;

/**
 * Application constants.
 */
public final class Constants {
    
    private Constants() {
        // Utility class
    }
    
    // Default configuration values
    public static final String DEFAULT_CLUSTER_NAME = "default-cluster";
    public static final String DEFAULT_ETCD_ENDPOINT = "localhost:2379";
    public static final long DEFAULT_TASK_INTERVAL_SECONDS = 30L;
    
    // Task statuses
    public static final String TASK_STATUS_PENDING = "PENDING";
    public static final String TASK_STATUS_RUNNING = "RUNNING";
    public static final String TASK_STATUS_COMPLETED = "COMPLETED";
    public static final String TASK_STATUS_FAILED = "FAILED";
    
    // Task schedule types
    public static final String TASK_SCHEDULE_ONCE = "once";
    public static final String TASK_SCHEDULE_REPEAT = "repeat";
    
    // Task actions
    public static final String TASK_ACTION_CREATE_INDEX = "create_index";
    public static final String TASK_ACTION_DELETE_INDEX = "delete_index";
    public static final String TASK_ACTION_PLAN_SHARD_ALLOCATION = "plan_shard_allocation";
    public static final String TASK_ACTION_DISCOVER_SEARCH_UNIT = "discover_search_unit";
    public static final String TASK_ACTION_SHARD_ALLOCATOR = "shard_allocator";
    public static final String TASK_ACTION_ACTUAL_ALLOCATION_UPDATER = "actual_allocation_updater";
}
