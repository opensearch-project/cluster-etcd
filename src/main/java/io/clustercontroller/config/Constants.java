package io.clustercontroller.config;

/**
 * Application constants.
 */
public final class Constants {
    
    private Constants() {
        // Utility class
    }
    
    // Default configuration values
    public static final String DEFAULT_ETCD_ENDPOINT = "http://localhost:2379";
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
    public static final String TASK_ACTION_DISCOVERY = "discovery";
    public static final String TASK_ACTION_PLAN_SHARD_ALLOCATION = "plan_shard_allocation";
    public static final String TASK_ACTION_SHARD_ALLOCATOR = "shard_allocator";
    public static final String TASK_ACTION_ACTUAL_ALLOCATION_UPDATER = "actual_allocation_updater";
    public static final String TASK_ACTION_GOAL_STATE_ORCHESTRATOR = "goal_state_orchestrator";
    
    // etcd path segments
    public static final String PATH_DELIMITER = "/";
    public static final String PATH_CTL_TASKS = "ctl-tasks";
    public static final String PATH_SEARCH_UNITS = "search-unit";
    public static final String PATH_INDICES = "indices";
    public static final String PATH_TEMPLATES = "templates";
    public static final String PATH_COORDINATORS = "coordinators";
    public static final String PATH_LEADER_ELECTION = "leader-election";
    // Default coordinator unit name for coordinator goal state path
    public static final String COORDINATOR_DEFAULT_UNIT = "default-coordinator";
    
    // etcd path suffixes
    public static final String SUFFIX_CONF = "conf";
    public static final String SUFFIX_GOAL_STATE = "goal-state";
    public static final String SUFFIX_ACTUAL_STATE = "actual-state";
    public static final String SUFFIX_MAPPINGS = "mappings";
    public static final String SUFFIX_SETTINGS = "settings";
    public static final String SUFFIX_PLANNED_ALLOCATION = "planned-allocation";
    public static final String SUFFIX_ACTUAL_ALLOCATION = "actual-allocation";
    
    // Health check thresholds
    // TODO: Make these configurable via application properties or environment variables
    public static final int HEALTH_CHECK_MEMORY_THRESHOLD_PERCENT = 90;
    public static final long HEALTH_CHECK_DISK_THRESHOLD_MB = 1024;
    
    // Discovery cleanup thresholds
    public static final long STALE_SEARCH_UNIT_TIMEOUT_MINUTES = 10L;
    
    // Admin state values
    public static final String ADMIN_STATE_NORMAL = "NORMAL";
    public static final String ADMIN_STATE_DRAIN = "DRAIN";
    // Leader election constants
    public static final String ELECTION_KEY_SUFFIX = "-election";
    public static final long LEADER_ELECTION_TTL_SECONDS = 30L;
    
    // Multi-cluster coordination paths
    public static final String PATH_MULTI_CLUSTER = "multi-cluster";
    public static final String PATH_CONTROLLERS = "controllers";
    public static final String PATH_CLUSTERS = "clusters";
    public static final String PATH_LOCKS = "locks";
    public static final String PATH_HEARTBEAT = "heartbeat";
    public static final String PATH_ASSIGNED = "assigned";
    public static final String PATH_METADATA = "metadata";

    // Health API level constants
    public static final String LEVEL_INDICES = "indices";
    public static final String LEVEL_SHARDS = "shards";
    public static final String LEVEL_CLUSTER = "cluster";

    // Shard/Node role string constants (as reported in actual state payloads)
    public static final String ROLE_PRIMARY = "primary";
    public static final String ROLE_SEARCH_REPLICA = "search_replica";
}
