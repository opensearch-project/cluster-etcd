package io.clustercontroller.store;

import java.nio.file.Paths;
import org.springframework.stereotype.Component;

import static io.clustercontroller.config.Constants.*;

/**
 * Centralized etcd path resolver for all metadata keys with multi-cluster support.
 * Provides consistent path structure for tasks, search units, indices, and other cluster metadata.
 * All methods accept dynamic cluster names to support multi-cluster operations.
 * Stateless singleton - no cluster-specific state stored.
 */
@Component
public class EtcdPathResolver {
    
    private static final String PATH_DELIMITER = "/";
    
    // Singleton instance - stateless
    private static final EtcdPathResolver INSTANCE = new EtcdPathResolver();
    
    private EtcdPathResolver() {
        // Private constructor for singleton
    }
    
    public static EtcdPathResolver getInstance() {
        return INSTANCE;
    }
    
    // =================================================================
    // CONTROLLER TASKS PATHS
    // =================================================================
    
    /**
     * Get prefix for all controller tasks
     * Pattern: /<cluster-name>/ctl-tasks
     */
    public String getControllerTasksPrefix(String clusterName) {
        return Paths.get(PATH_DELIMITER, clusterName, PATH_CTL_TASKS).toString();
    }
    
    /**
     * Get path for specific controller task
     * Pattern: /<cluster-name>/ctl-tasks/<task-name>
     */
    public String getControllerTaskPath(String clusterName, String taskName) {
        return Paths.get(getControllerTasksPrefix(clusterName), taskName).toString();
    }
    
    // =================================================================
    // SEARCH UNIT PATHS
    // =================================================================
    
    /**
     * Get prefix for all search units
     * Pattern: /<cluster-name>/search-unit
     */
    public String getSearchUnitsPrefix(String clusterName) {
        return Paths.get(PATH_DELIMITER, clusterName, PATH_SEARCH_UNITS).toString();
    }
    
    /**
     * Get search unit configuration path
     * Pattern: /<cluster-name>/search-unit/<unit-name>/conf
     */
    public String getSearchUnitConfPath(String clusterName, String unitName) {
        return Paths.get(getSearchUnitsPrefix(clusterName), unitName, SUFFIX_CONF).toString();
    }
    
    /**
     * Get search unit goal state path
     * Pattern: /<cluster-name>/search-unit/<unit-name>/goal-state
     */
    public String getSearchUnitGoalStatePath(String clusterName, String unitName) {
        return Paths.get(getSearchUnitsPrefix(clusterName), unitName, SUFFIX_GOAL_STATE).toString();
    }
    
    /**
     * Get search unit actual state path
     * Pattern: /<cluster-name>/search-unit/<unit-name>/actual-state
     */
    public String getSearchUnitActualStatePath(String clusterName, String unitName) {
        return Paths.get(getSearchUnitsPrefix(clusterName), unitName, SUFFIX_ACTUAL_STATE).toString();
    }
    
    // =================================================================
    // INDEX PATHS
    // =================================================================
    
    /**
     * Get prefix for all indices
     * Pattern: /<cluster-name>/indices
     */
    public String getIndicesPrefix(String clusterName) {
        return Paths.get(PATH_DELIMITER, clusterName, PATH_INDICES).toString();
    }
    
    /**
     * Get prefix for a specific index (for prefix deletion)
     * Pattern: /<cluster-name>/indices/<index-name>
     */
    public String getIndexPrefix(String clusterName, String indexName) {
        return Paths.get(getIndicesPrefix(clusterName), indexName).toString();
    }
    
    /**
     * Get index configuration path
     * Pattern: /<cluster-name>/indices/<index-name>/conf
     */
    public String getIndexConfPath(String clusterName, String indexName) {
        return Paths.get(getIndicesPrefix(clusterName), indexName, SUFFIX_CONF).toString();
    }
    
    /**
     * Get index mappings path
     * Pattern: /<cluster-name>/indices/<index-name>/mappings
     */
    public String getIndexMappingsPath(String clusterName, String indexName) {
        return Paths.get(getIndicesPrefix(clusterName), indexName, SUFFIX_MAPPINGS).toString();
    }
    
    /**
     * Get index settings path
     * Pattern: /<cluster-name>/indices/<index-name>/settings
     */
    public String getIndexSettingsPath(String clusterName, String indexName) {
        return Paths.get(getIndicesPrefix(clusterName), indexName, SUFFIX_SETTINGS).toString();
    }
    
    // =================================================================
    // SHARD ALLOCATION PATHS
    // =================================================================
    
    /**
     * Get shard planned allocation path
     * Pattern: /<cluster-name>/indices/<index-name>/<shard-id>/planned-allocation
     */
    public String getShardPlannedAllocationPath(String clusterName, String indexName, String shardId) {
        return Paths.get(getIndicesPrefix(clusterName), indexName, shardId, SUFFIX_PLANNED_ALLOCATION).toString();
    }
    
    /**
     * Get shard actual allocation path
     * Pattern: /<cluster-name>/indices/<index-name>/<shard-id>/actual-allocation
     */
    public String getShardActualAllocationPath(String clusterName, String indexName, String shardId) {
        return Paths.get(getIndicesPrefix(clusterName), indexName, shardId, SUFFIX_ACTUAL_ALLOCATION).toString();
    }
    
    // =================================================================
    // COORDINATOR PATHS
    // =================================================================
    
    /**
     * Get prefix for all coordinators
     * Pattern: /<cluster-name>/coordinators
     */
    public String getCoordinatorsPrefix(String clusterName) {
        return Paths.get(PATH_DELIMITER, clusterName, PATH_COORDINATORS).toString();
    }
    
    /**
     * Get shared coordinator goal state path (common for all coordinators)
     * Pattern: /<cluster-name>/coordinators/goal-state
     */
    public String getCoordinatorGoalStatePath(String clusterName) {
        return Paths.get(getCoordinatorsPrefix(clusterName), SUFFIX_GOAL_STATE).toString();
    }
    
    /**
     * Get coordinator actual state path (per-coordinator reporting)
     * Pattern: /<cluster-name>/coordinators/<coordinator-name>/actual-state
     */
    public String getCoordinatorActualStatePath(String clusterName, String coordinatorName) {
        return Paths.get(getCoordinatorsPrefix(clusterName), coordinatorName, SUFFIX_ACTUAL_STATE).toString();
    }
    
    // =================================================================
    // LEADER ELECTION PATHS
    // =================================================================
    
    /**
     * Get leader election path
     * Pattern: /<cluster-name>/leader-election
     */
    public String getLeaderElectionPath(String clusterName) {
        return Paths.get(PATH_DELIMITER, clusterName, PATH_LEADER_ELECTION).toString();
    }
    
    // =================================================================
    // UTILITY METHODS
    // =================================================================
    
    /**
     * Get cluster root path
     * Pattern: /<cluster-name>
     */
    public String getClusterRoot(String clusterName) {
        return Paths.get(PATH_DELIMITER, clusterName).toString();
    }
    
    // =================================================================
    // MULTI-CLUSTER COORDINATION PATHS
    // =================================================================
    
    /**
     * Get multi-cluster root path
     * Pattern: /multi-cluster
     */
    public String getMultiClusterRoot() {
        return Paths.get(PATH_DELIMITER, PATH_MULTI_CLUSTER).toString();
    }
    
    /**
     * Get controller heartbeat path
     * Pattern: /multi-cluster/controllers/<controller-id>/heartbeat
     */
    public String getControllerHeartbeatPath(String controllerId) {
        return Paths.get(PATH_DELIMITER, PATH_MULTI_CLUSTER, PATH_CONTROLLERS, controllerId, PATH_HEARTBEAT).toString();
    }
    
    /**
     * Get controller assignment path
     * Pattern: /multi-cluster/controllers/<controller-id>/assigned/<cluster-id>
     */
    public String getControllerAssignmentPath(String controllerId, String clusterId) {
        return Paths.get(PATH_DELIMITER, PATH_MULTI_CLUSTER, PATH_CONTROLLERS, controllerId, PATH_ASSIGNED, clusterId).toString();
    }
    
    /**
     * Get cluster lock path
     * Pattern: /multi-cluster/locks/clusters/<cluster-id>
     */
    public String getClusterLockPath(String clusterId) {
        return Paths.get(PATH_DELIMITER, PATH_MULTI_CLUSTER, PATH_LOCKS, PATH_CLUSTERS, clusterId).toString();
    }
    
    /**
     * Get cluster registry path
     * Pattern: /multi-cluster/clusters/<cluster-id>/metadata
     */
    public String getClusterRegistryPath(String clusterId) {
        return Paths.get(PATH_DELIMITER, PATH_MULTI_CLUSTER, PATH_CLUSTERS, clusterId, PATH_METADATA).toString();
    }
    
    /**
     * Get controllers prefix for listing
     * Pattern: /multi-cluster/controllers/
     */
    public String getControllersPrefix() {
        return Paths.get(PATH_DELIMITER, PATH_MULTI_CLUSTER, PATH_CONTROLLERS, "").toString();
    }
    
    /**
     * Get clusters prefix for listing
     * Pattern: /multi-cluster/clusters/
     */
    public String getClustersPrefix() {
        return Paths.get(PATH_DELIMITER, PATH_MULTI_CLUSTER, PATH_CLUSTERS, "").toString();
    }
}