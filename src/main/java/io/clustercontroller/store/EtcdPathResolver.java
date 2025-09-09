package io.clustercontroller.store;

import java.nio.file.Paths;

import static io.clustercontroller.config.Constants.*;

/**
 * Centralized etcd path resolver for all metadata keys.
 * Provides consistent path structure for tasks, search units, indices, and other cluster metadata.
 */
public class EtcdPathResolver {
    
    private static final String PATH_DELIMITER = "/";
    
    private final String clusterName;
    
    public EtcdPathResolver(String clusterName) {
        this.clusterName = clusterName;
    }
    
    // =================================================================
    // CONTROLLER TASKS PATHS
    // =================================================================
    
    /**
     * Get prefix for all controller tasks
     * Pattern: /<cluster-name>/ctl-tasks
     */
    public String getControllerTasksPrefix() {
        return Paths.get(PATH_DELIMITER, clusterName, PATH_CTL_TASKS).toString();
    }
    
    /**
     * Get path for specific controller task
     * Pattern: /<cluster-name>/ctl-tasks/<task-name>
     */
    public String getControllerTaskPath(String taskName) {
        return Paths.get(getControllerTasksPrefix(), taskName).toString();
    }
    
    // =================================================================
    // SEARCH UNIT PATHS
    // =================================================================
    
    /**
     * Get prefix for all search units
     * Pattern: /<cluster-name>/search-units
     */
    public String getSearchUnitsPrefix() {
        return Paths.get(PATH_DELIMITER, clusterName, PATH_SEARCH_UNITS).toString();
    }
    
    /**
     * Get search unit configuration path
     * Pattern: /<cluster-name>/search-units/<unit-name>/conf
     */
    public String getSearchUnitConfPath(String unitName) {
        return Paths.get(getSearchUnitsPrefix(), unitName, SUFFIX_CONF).toString();
    }
    
    /**
     * Get search unit goal state path
     * Pattern: /<cluster-name>/search-units/<unit-name>/goal-state
     */
    public String getSearchUnitGoalStatePath(String unitName) {
        return Paths.get(getSearchUnitsPrefix(), unitName, SUFFIX_GOAL_STATE).toString();
    }
    
    /**
     * Get search unit actual state path
     * Pattern: /<cluster-name>/search-units/<unit-name>/actual-state
     */
    public String getSearchUnitActualStatePath(String unitName) {
        return Paths.get(getSearchUnitsPrefix(), unitName, SUFFIX_ACTUAL_STATE).toString();
    }
    
    
    // =================================================================
    // INDEX PATHS
    // =================================================================
    
    /**
     * Get prefix for all indices
     * Pattern: /<cluster-name>/indices
     */
    public String getIndicesPrefix() {
        return Paths.get(PATH_DELIMITER, clusterName, PATH_INDICES).toString();
    }
    
    /**
     * Get index configuration path
     * Pattern: /<cluster-name>/indices/<index-name>/conf
     */
    public String getIndexConfPath(String indexName) {
        return Paths.get(getIndicesPrefix(), indexName, SUFFIX_CONF).toString();
    }
    
    /**
     * Get index mappings path
     * Pattern: /<cluster-name>/indices/<index-name>/mappings
     */
    public String getIndexMappingsPath(String indexName) {
        return Paths.get(getIndicesPrefix(), indexName, SUFFIX_MAPPINGS).toString();
    }
    
    /**
     * Get index settings path
     * Pattern: /<cluster-name>/indices/<index-name>/settings
     */
    public String getIndexSettingsPath(String indexName) {
        return Paths.get(getIndicesPrefix(), indexName, SUFFIX_SETTINGS).toString();
    }
    
    // =================================================================
    // SHARD ALLOCATION PATHS
    // =================================================================
    
    /**
     * Get shard planned allocation path
     * Pattern: /<cluster-name>/indices/<index-name>/shard/<shard-id>/planned-allocation
     */
    public String getShardPlannedAllocationPath(String indexName, String shardId) {
        return Paths.get(getIndicesPrefix(), indexName, PATH_SHARD, shardId, SUFFIX_PLANNED_ALLOCATION).toString();
    }
    
    /**
     * Get shard actual allocation path
     * Pattern: /<cluster-name>/indices/<index-name>/shard/<shard-id>/actual-allocation
     */
    public String getShardActualAllocationPath(String indexName, String shardId) {
        return Paths.get(getIndicesPrefix(), indexName, PATH_SHARD, shardId, SUFFIX_ACTUAL_ALLOCATION).toString();
    }
    
    // =================================================================
    // COORDINATOR PATHS
    // =================================================================
    
    /**
     * Get prefix for all coordinators
     * Pattern: /<cluster-name>/coordinators
     */
    public String getCoordinatorsPrefix() {
        return Paths.get(PATH_DELIMITER, clusterName, PATH_COORDINATORS).toString();
    }
    
    /**
     * Get shared coordinator goal state path (common for all coordinators)
     * Pattern: /<cluster-name>/coordinators/goal-state
     */
    public String getCoordinatorGoalStatePath() {
        return Paths.get(getCoordinatorsPrefix(), SUFFIX_GOAL_STATE).toString();
    }
    
    /**
     * Get coordinator actual state path (per-coordinator reporting)
     * Pattern: /<cluster-name>/coordinators/<coordinator-name>/actual-state
     */
    public String getCoordinatorActualStatePath(String coordinatorName) {
        return Paths.get(getCoordinatorsPrefix(), coordinatorName, SUFFIX_ACTUAL_STATE).toString();
    }
    
    
    // =================================================================
    // LEADER ELECTION PATHS
    // =================================================================
    
    /**
     * Get leader election path
     * Pattern: /<cluster-name>/leader-election
     */
    public String getLeaderElectionPath() {
        return Paths.get(PATH_DELIMITER, clusterName, PATH_LEADER_ELECTION).toString();
    }
    
    // =================================================================
    // UTILITY METHODS
    // =================================================================
    
    /**
     * Get cluster root path
     * Pattern: /<cluster-name>
     */
    public String getClusterRoot() {
        return Paths.get(PATH_DELIMITER, clusterName).toString();
    }
    
    /**
     * Get cluster name
     */
    public String getClusterName() {
        return clusterName;
    }
}