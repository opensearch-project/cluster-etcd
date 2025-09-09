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
    
    // etcd path segments
    public static final String PATH_DELIMITER = "/";
    public static final String PATH_CTL_TASKS = "ctl-tasks";
    public static final String PATH_SEARCH_UNITS = "search-units";
    public static final String PATH_INDICES = "indices";
    public static final String PATH_COORDINATORS = "coordinators";
    public static final String PATH_SHARD = "shard";
    public static final String PATH_LEADER_ELECTION = "leader-election";
    
    // etcd path suffixes
    public static final String SUFFIX_CONF = "conf";
    public static final String SUFFIX_GOAL_STATE = "goal-state";
    public static final String SUFFIX_ACTUAL_STATE = "actual-state";
    public static final String SUFFIX_MAPPINGS = "mappings";
    public static final String SUFFIX_SETTINGS = "settings";
    public static final String SUFFIX_PLANNED_ALLOCATION = "planned-allocation";
    public static final String SUFFIX_ACTUAL_ALLOCATION = "actual-allocation";
    
    // Etcd path components (alternative naming for compatibility)
    public static final String PATH_COMPONENT_CONF = "conf";
    public static final String PATH_COMPONENT_GOAL_STATE = "goal-state";
    public static final String PATH_COMPONENT_ACTUAL_STATE = "actual-state";
    
    // Etcd path prefixes (alternative naming for compatibility)
    public static final String PATH_PREFIX_CTL_TASKS = "ctl-tasks";
    public static final String PATH_PREFIX_SEARCH_UNIT = "search-unit";
    public static final String PATH_PREFIX_INDICES = "indices";
    public static final String PATH_PREFIX_CONFIG = "config";
    
    // Health check thresholds
    // TODO: Make these configurable via application properties or environment variables
    public static final int HEALTH_CHECK_MEMORY_THRESHOLD_PERCENT = 90;
    public static final long HEALTH_CHECK_DISK_THRESHOLD_MB = 1024;
    
    // Admin state values
    public static final String ADMIN_STATE_NORMAL = "NORMAL";
    public static final String ADMIN_STATE_DRAIN = "DRAIN";
}
