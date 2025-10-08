package io.clustercontroller.store;

import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Abstraction layer for metadata storage supporting different backends (etcd, redis, etc.)
 * This is the main interface for all cluster metadata operations.
 */
public interface MetadataStore {
    
    // =================================================================
    // CONTROLLER TASKS OPERATIONS
    // =================================================================
    
    /**
     * Get all controller tasks sorted by priority
     */
    List<TaskMetadata> getAllTasks(String clusterId) throws Exception;
    
    /**
     * Get task by name
     */
    Optional<TaskMetadata> getTask(String clusterId, String taskName) throws Exception;
    
    /**
     * Create new task
     */
    String createTask(String clusterId, TaskMetadata task) throws Exception;
    
    /**
     * Update existing task
     */
    void updateTask(String clusterId, TaskMetadata task) throws Exception;
    
    /**
     * Delete task
     */
    void deleteTask(String clusterId, String taskName) throws Exception;
    
    /**
     * Delete old completed tasks (cleanup)
     */
    void deleteOldTasks(long olderThanTimestamp) throws Exception;
    
    // =================================================================
    // SEARCH UNITS OPERATIONS
    // =================================================================
    
    /**
     * Get all search units
     */
    List<SearchUnit> getAllSearchUnits(String clusterId) throws Exception;
    
    /**
     * Get search unit by name
     */
    Optional<SearchUnit> getSearchUnit(String clusterId, String unitName) throws Exception;
    
    /**
     * Create or update search unit
     */
    void upsertSearchUnit(String clusterId, String unitName, SearchUnit searchUnit) throws Exception;
    
    /**
     * Update search unit
     */
    void updateSearchUnit(String clusterId, SearchUnit searchUnit) throws Exception;
    
    /**
     * Delete search unit
     */
    void deleteSearchUnit(String clusterId, String unitName) throws Exception;
    
    // =================================================================
    // SEARCH UNIT STATE OPERATIONS (for discovery)
    // =================================================================
    
    /**
     * Get all search unit actual states (for discovery)
     */
    Map<String, SearchUnitActualState> getAllSearchUnitActualStates(String clusterId) throws Exception;
    
    /**
     * Get search unit goal state
     */
    SearchUnitGoalState getSearchUnitGoalState(String clusterId, String unitName) throws Exception;
    
    /**
     * Get search unit actual state
     */
    SearchUnitActualState getSearchUnitActualState(String clusterId, String unitName) throws Exception;
    
    /**
     * Set search unit goal state
     */
    void setSearchUnitGoalState(String clusterId, String unitName, SearchUnitGoalState goalState) throws Exception;
    
    /**
     * Set search unit actual state
     */
    void setSearchUnitActualState(String clusterId, String unitName, SearchUnitActualState actualState) throws Exception;
    // =================================================================
    // INDEX CONFIGURATIONS OPERATIONS
    // =================================================================
    
    /**
     * Get all index configurations
     */
    List<Index> getAllIndexConfigs(String clusterId) throws Exception;
    
    /**
     * Get index configuration by name
     */
    Optional<String> getIndexConfig(String clusterId, String indexName) throws Exception;
    
    /**
     * Create new index configuration
     */
    String createIndexConfig(String clusterId, String indexName, String indexConfig) throws Exception;
    
    /**
     * Update index configuration
     */
    void updateIndexConfig(String clusterId, String indexName, String indexConfig) throws Exception;
    
    /**
     * Delete index configuration
     */
    void deleteIndexConfig(String clusterId, String indexName) throws Exception;
    
    /**
     * Set index mappings
     */
    void setIndexMappings(String clusterId, String indexName, String mappings) throws Exception;
    
    /**
     * Get index settings
     */
    IndexSettings getIndexSettings(String clusterId, String indexName) throws Exception;
    
    /**
     * Set index settings
     */
    void setIndexSettings(String clusterId, String indexName, String settings) throws Exception;
    
    /**
     * Delete all keys with the given prefix
     */
    void deletePrefix(String clusterId, String prefix) throws Exception;
    
    // =================================================================
    // TEMPLATE OPERATIONS
    // =================================================================
    
    /**
     * Get template configuration by name
     */
    Optional<String> getTemplate(String clusterId, String templateName) throws Exception;
    
    /**
     * Create new template configuration
     */
    String createTemplate(String clusterId, String templateName, String templateConfig) throws Exception;
    
    /**
     * Update template configuration
     */
    void updateTemplate(String clusterId, String templateName, String templateConfig) throws Exception;
    
    /**
     * Delete template configuration
     */
    void deleteTemplate(String clusterId, String templateName) throws Exception;
    
    /**
     * Get all templates for a cluster
     */
    List<String> getAllTemplates(String clusterId) throws Exception;
    
    // =================================================================
    // SHARD ALLOCATION OPERATIONS
    // =================================================================
    
    /**
     * Get planned allocation for a specific shard
     */
    ShardAllocation getPlannedAllocation(String clusterId, String indexName, String shardId) throws Exception;
    
    /**
     * Set planned allocation for a specific shard
     */
    void setPlannedAllocation(String clusterId, String indexName, String shardId, ShardAllocation allocation) throws Exception;
    
    // =================================================================
    // CLUSTER OPERATIONS
    // =================================================================
    
    /**
     * Initialize/setup the metadata store
     */
    void initialize() throws Exception;
    
    /**
     * Close/cleanup the metadata store
     */
    void close() throws Exception;
    
    /**
     * Check if this controller instance is the leader.
     * Only the leader should perform active management operations.
     * 
     * @return true if this instance is the leader, false otherwise
     */
    boolean isLeader();
}
