package io.clustercontroller.store;

import io.clustercontroller.election.LeaderElection;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.TypeMapping;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.models.Template;
import io.clustercontroller.models.CoordinatorGoalState;
import io.clustercontroller.models.ClusterInformation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.models.TaskMetadata;
import io.clustercontroller.models.ClusterControllerAssignment;
import io.clustercontroller.util.EnvironmentUtils;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import lombok.extern.slf4j.Slf4j;

import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.clustercontroller.config.Constants;
import static io.clustercontroller.config.Constants.PATH_DELIMITER;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * etcd-based implementation of MetadataStore.
 * Singleton to ensure single etcd client connection.
 */
@Slf4j
public class EtcdMetadataStore implements MetadataStore {
    
    // TODO: Make etcd timeout configurable via environment variable or config
    private static final long ETCD_OPERATION_TIMEOUT_SECONDS = 5;
    
    private static EtcdMetadataStore instance;
    
    private final String[] etcdEndpoints;
    private final Client etcdClient;
    private final KV kvClient;
    private final EtcdPathResolver pathResolver;
    private final ObjectMapper objectMapper;
    // Configurable coordinator goal state location
    private volatile String coordinatorGoalStateGroup = Constants.PATH_COORDINATORS;
    private volatile String coordinatorGoalStateUnit = "default-coordinator";
    
    // Leader election fields
    private final String nodeId;
    private final LeaderElection leaderElection;
    
    /**
     * Private constructor for singleton pattern
     */
    private EtcdMetadataStore(String[] etcdEndpoints) throws Exception {
        this.etcdEndpoints = etcdEndpoints;
        this.nodeId = EnvironmentUtils.getRequiredEnv("NODE_NAME");
        
        // Initialize Jackson ObjectMapper
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        // Initialize etcd client
        this.etcdClient = Client.builder().endpoints(etcdEndpoints).build();
        this.kvClient = etcdClient.getKVClient();
        
        // Initialize path resolver
        this.pathResolver = EtcdPathResolver.getInstance();
        
        // Initialize leader election (controller-level, not cluster-specific)
        this.leaderElection = new LeaderElection(etcdClient, nodeId);
        
        log.info("EtcdMetadataStore initialized with endpoints: {} and nodeId: {}", 
            String.join(",", etcdEndpoints), nodeId);
    }
    
    // =================================================================
    // SINGLETON MANAGEMENT
    // =================================================================
    
    /**
     * Test constructor with injected dependencies
     */
    private EtcdMetadataStore(String[] etcdEndpoints, String nodeId, Client etcdClient, KV kvClient) {
        this.etcdEndpoints = etcdEndpoints;
        this.nodeId = nodeId;
        this.etcdClient = etcdClient;
        this.kvClient = kvClient;
        
        // Initialize Jackson ObjectMapper
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        // Initialize path resolver
        this.pathResolver = EtcdPathResolver.getInstance();
        
        // Initialize leader election for testing (controller-level, not cluster-specific)
        this.leaderElection = new LeaderElection(etcdClient, nodeId);
        
        log.info("EtcdMetadataStore initialized for testing with nodeId: {}", nodeId);
    }
    /**
     * Get singleton instance
     */
    public static synchronized EtcdMetadataStore getInstance(String[] etcdEndpoints) throws Exception {
        if (instance == null) {
            instance = new EtcdMetadataStore(etcdEndpoints);
        }
        return instance;
    }
    
    /**
     * Get existing instance (throws if not initialized)
     */
    public static EtcdMetadataStore getInstance() {
        if (instance == null) {
            throw new IllegalStateException("EtcdMetadataStore not initialized. Call getInstance(etcdEndpoints) first.");
        }
        return instance;
    }
    
    /**
     * Reset singleton instance (for testing only)
     */
    public static synchronized void resetInstance() {
        instance = null;
    }
    
    /**
     * Create test instance with mocked dependencies (for testing only)
     */
    public static synchronized EtcdMetadataStore createTestInstance(String[] etcdEndpoints, String nodeId, Client etcdClient, KV kvClient) {
        resetInstance();
        instance = new EtcdMetadataStore(etcdEndpoints, nodeId, etcdClient, kvClient);
        return instance;
    }
    
    /**
     * Get the etcd client for use by other components (e.g., MultiClusterManager)
     * @return the etcd Client instance
     */
    public Client getEtcdClient() {
        return etcdClient;
    }

    
    // =================================================================
    // CONTROLLER TASKS OPERATIONS
    // =================================================================
    
    public List<TaskMetadata> getAllTasks(String clusterId) throws Exception {
        log.debug("Getting all tasks from etcd");
        
        try {
            String tasksPrefix = pathResolver.getControllerTasksPrefix(clusterId);
            List<TaskMetadata> tasks = getAllObjectsByPrefix(tasksPrefix, TaskMetadata.class);
            
            // Sort by priority (0 = highest priority)
            tasks.sort((t1, t2) -> Integer.compare(t1.getPriority(), t2.getPriority()));
            
            log.debug("Retrieved {} tasks from etcd", tasks.size());
            return tasks;
            
        } catch (Exception e) {
            log.error("Failed to get all tasks from etcd: {}", e.getMessage(), e);
            throw new Exception("Failed to retrieve tasks from etcd", e);
        }
    }
    
    public Optional<TaskMetadata> getTask(String clusterId, String taskName) throws Exception {
        log.debug("Getting task {} from etcd", taskName);
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(clusterId, taskName);
            Optional<TaskMetadata> result = getObjectByPath(taskPath, TaskMetadata.class);
            
            if (result.isPresent()) {
                log.debug("Retrieved task {} from etcd", taskName);
            } else {
                log.debug("Task {} not found in etcd", taskName);
            }
            
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get task {} from etcd: {}", taskName, e.getMessage(), e);
            throw new Exception("Failed to retrieve task from etcd", e);
        }
    }
    
    public String createTask(String clusterId, TaskMetadata task) throws Exception {
        log.info("Creating task {} in etcd", task.getName());
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(clusterId, task.getName());
            storeObjectAsJson(taskPath, task);
            
            log.info("Successfully created task {} in etcd", task.getName());
            return task.getName();
            
        } catch (Exception e) {
            log.error("Failed to create task {} in etcd: {}", task.getName(), e.getMessage(), e);
            throw new Exception("Failed to create task in etcd", e);
        }
    }
    
    public void updateTask(String clusterId, TaskMetadata task) throws Exception {
        log.debug("Updating task {} in etcd", task.getName());
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(clusterId, task.getName());
            storeObjectAsJson(taskPath, task);
            
            log.debug("Successfully updated task {} in etcd", task.getName());
            
        } catch (Exception e) {
            log.error("Failed to update task {} in etcd: {}", task.getName(), e.getMessage(), e);
            throw new Exception("Failed to update task in etcd", e);
        }
    }
    
    public void deleteTask(String clusterId, String taskName) throws Exception {
        log.info("Deleting task {} from etcd", taskName);
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(clusterId, taskName);
            executeEtcdDelete(taskPath);
            
            log.info("Successfully deleted task {} from etcd", taskName);
            
        } catch (Exception e) {
            log.error("Failed to delete task {} from etcd: {}", taskName, e.getMessage(), e);
            throw new Exception("Failed to delete task from etcd", e);
        }
    }
    
    public void deleteOldTasks(long olderThanTimestamp) throws Exception {
        log.debug("Deleting old tasks from etcd older than {}", olderThanTimestamp);
        // TODO: Implement etcd cleanup for old tasks
    }
    
    // =================================================================
    // SEARCH UNITS OPERATIONS
    // =================================================================
    
    public List<SearchUnit> getAllSearchUnits(String clusterId) throws Exception {
        log.debug("Getting all search units from etcd");
        
        try {
            String unitsPrefix = pathResolver.getSearchUnitsPrefix(clusterId);
            GetResponse response = executeEtcdPrefixQuery(unitsPrefix);
            
            List<SearchUnit> searchUnits = new ArrayList<>();
            for (var kv : response.getKvs()) {
                String key = kv.getKey().toString(StandardCharsets.UTF_8);
                // Only process keys that end with /conf (search unit configuration files)
                // This filters out /actual-state and other non-config paths
                if (key.endsWith("/conf")) {
                    String json = kv.getValue().toString(StandardCharsets.UTF_8);
                    try {
                        SearchUnit searchUnit = objectMapper.readValue(json, SearchUnit.class);
                        searchUnits.add(searchUnit);
                    } catch (Exception parseException) {
                        log.warn("Failed to parse search unit config at key {}: {}", key, parseException.getMessage());
                    }
                }
            }
            
            log.debug("Retrieved {} search units from etcd", searchUnits.size());
            return searchUnits;
            
        } catch (Exception e) {
            log.error("Failed to get all search units from etcd: {}", e.getMessage(), e);
            throw new Exception("Failed to retrieve search units from etcd", e);
        }
    }
    
    public Optional<SearchUnit> getSearchUnit(String clusterId, String unitName) throws Exception {
        log.debug("Getting search unit {} from etcd", unitName);
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(clusterId, unitName);
            Optional<SearchUnit> result = getObjectByPath(unitPath, SearchUnit.class);
            
            if (result.isPresent()) {
                log.debug("Retrieved search unit {} from etcd", unitName);
            } else {
                log.debug("Search unit {} not found in etcd", unitName);
            }
            
            return result;
            
        } catch (Exception e) {
            log.error("Failed to get search unit {} from etcd: {}", unitName, e.getMessage(), e);
            throw new Exception("Failed to retrieve search unit from etcd", e);
        }
    }
    
    public void upsertSearchUnit(String clusterId, String unitName, SearchUnit searchUnit) throws Exception {
        log.info("Upserting search unit {} in etcd", unitName);
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(clusterId, unitName);
            storeObjectAsJson(unitPath, searchUnit);
            
            log.info("Successfully upserted search unit {} in etcd", unitName);
            
        } catch (Exception e) {
            log.error("Failed to upsert search unit {} in etcd: {}", unitName, e.getMessage(), e);
            throw new Exception("Failed to upsert search unit in etcd", e);
        }
    }
    
    public void updateSearchUnit(String clusterId, SearchUnit searchUnit) throws Exception {
        log.debug("Updating search unit {} in etcd", searchUnit.getName());
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(clusterId, searchUnit.getName());
            storeObjectAsJson(unitPath, searchUnit);
            
            log.debug("Successfully updated search unit {} in etcd", searchUnit.getName());
            
        } catch (Exception e) {
            log.error("Failed to update search unit {} in etcd: {}", searchUnit.getName(), e.getMessage(), e);
            throw new Exception("Failed to update search unit in etcd", e);
        }
    }
    
    public void deleteSearchUnit(String clusterId, String unitName) throws Exception {
        log.info("Deleting search unit {} (all state: conf, goal-state, actual-state) from etcd", unitName);
        
        try {
            // Delete the entire search unit node prefix to remove conf, goal-state, and actual-state
            String unitPrefix = pathResolver.getSearchUnitsPrefix(clusterId) + PATH_DELIMITER + unitName;
            
            ByteSequence prefixBytes = ByteSequence.from(unitPrefix + PATH_DELIMITER, UTF_8);
            kvClient.delete(
                prefixBytes,
                DeleteOption.newBuilder().withPrefix(prefixBytes).build()
            ).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            log.info("Successfully deleted search unit {} and all its state from etcd", unitName);
            
        } catch (Exception e) {
            log.error("Failed to delete search unit {} from etcd: {}", unitName, e.getMessage(), e);
            throw new Exception("Failed to delete search unit from etcd", e);
        }
    }
    
    // =================================================================
    // SEARCH UNIT STATE OPERATIONS (for discovery)
    // =================================================================
    
    public Map<String, SearchUnitActualState> getAllSearchUnitActualStates(String clusterId) throws Exception {
        log.info("Getting all search unit actual states from etcd for cluster '{}' in getAllSearchUnitActualStates", clusterId);

        String prefix = pathResolver.getSearchUnitsPrefix(clusterId);
        log.info("Querying etcd for actual-states with clusterId: '{}', prefix: '{}'", clusterId, prefix);
        
        GetOption option = GetOption.newBuilder()
                .withPrefix(ByteSequence.from(prefix, UTF_8))
                .build();
        
        CompletableFuture<GetResponse> getFuture = kvClient.get(
                ByteSequence.from(prefix, UTF_8), option);
        GetResponse response = getFuture.get();
        
        log.info("Etcd returned {} total keys for prefix '{}'", response.getKvs().size(), prefix);
        
        Map<String, SearchUnitActualState> actualStates = new HashMap<>();
        
        for (KeyValue kv : response.getKvs()) {
            String key = kv.getKey().toString(UTF_8);
            String json = kv.getValue().toString(UTF_8);
            log.info("Processing etcd key: {}", key);
            
            // Parse key to get unit name and check if it's an actual-state key
            // relativePath example: /unit-name/actual-state
            String relativePath = key.substring(prefix.length());
            if (relativePath.startsWith("/")) {
                relativePath = relativePath.substring(1); // Remove leading slash
            }
            String[] parts = relativePath.split("/");
            // After removing leading slash: parts[0] = unit-name, parts[1] = actual-state
            if (parts.length >= 2 && "actual-state".equals(parts[1])) {
                String unitName = parts[0];
                log.info("Found actual-state for unit: {} (key: {})", unitName, key);
                try {
                    SearchUnitActualState actualState = objectMapper.readValue(json, SearchUnitActualState.class);
                    actualStates.put(unitName, actualState);
                    log.info("Successfully parsed actual-state for unit: {}", unitName);
                } catch (Exception e) {
                    log.warn("Failed to parse actual state for unit {}: {}", unitName, e.getMessage(), e);
                }
            }
        }

        return actualStates;
    }
    
    public SearchUnitGoalState getSearchUnitGoalState(String clusterId, String unitName) throws Exception {
        String key = pathResolver.getSearchUnitGoalStatePath(clusterId, unitName);
        CompletableFuture<GetResponse> getFuture = kvClient.get(ByteSequence.from(key, UTF_8));
        GetResponse response = getFuture.get();
        
        if (response.getKvs().isEmpty()) {
            return null;
        }
        
        String json = response.getKvs().get(0).getValue().toString(UTF_8);
        SearchUnitGoalState goalState = objectMapper.readValue(json, SearchUnitGoalState.class);
        
        return goalState;
    }
    
    public SearchUnitActualState getSearchUnitActualState(String clusterId, String unitName) throws Exception {
        String key = pathResolver.getSearchUnitActualStatePath(clusterId, unitName);
        CompletableFuture<GetResponse> getFuture = kvClient.get(ByteSequence.from(key, UTF_8));
        GetResponse response = getFuture.get();
        
        if (response.getKvs().isEmpty()) {
            return null;
        }
        
        String json = response.getKvs().get(0).getValue().toString(UTF_8);
        SearchUnitActualState actualState = objectMapper.readValue(json, SearchUnitActualState.class);
        
        return actualState;
    }
    
    public void setSearchUnitGoalState(String clusterId, String unitName, SearchUnitGoalState goalState) throws Exception {
        String key = pathResolver.getSearchUnitGoalStatePath(clusterId, unitName);
        String json = objectMapper.writeValueAsString(goalState);
        
        // Use Compare-And-Swap (CAS) pattern with mod_revision for thread-safe updates
        ByteSequence keyBytes = ByteSequence.from(key, UTF_8);
        ByteSequence valueBytes = ByteSequence.from(json, UTF_8);
        
        // Get current revision to use in CAS operation
        GetResponse getResponse = kvClient.get(keyBytes).get();
        long currentRevision = 0;
        if (getResponse.getCount() > 0) {
            currentRevision = getResponse.getKvs().get(0).getModRevision();
        }
        
        // Perform atomic CAS operation
        TxnResponse txnResponse = kvClient.txn()
            .If(new Cmp(keyBytes, Cmp.Op.EQUAL, CmpTarget.modRevision(currentRevision)))
            .Then(Op.put(keyBytes, valueBytes, PutOption.DEFAULT))
            .Else(Op.get(keyBytes, GetOption.DEFAULT))
            .commit()
            .get();
            
        if (!txnResponse.isSucceeded()) {
            throw new RuntimeException("Failed to update goal state for " + unitName + " due to concurrent modification. Please retry.");
        }
        
        log.debug("Successfully set goal state for search unit {} using CAS", unitName);
    }
    
    public void setSearchUnitActualState(String clusterId, String unitName, SearchUnitActualState actualState) throws Exception {
        String key = pathResolver.getSearchUnitActualStatePath(clusterId, unitName);
        String json = objectMapper.writeValueAsString(actualState);
        
        var putFuture = kvClient.put(ByteSequence.from(key, UTF_8), ByteSequence.from(json, UTF_8));
        putFuture.get();
        
        log.debug("Successfully set actual state for search unit {}", unitName);
    }

    public List<String> getAllNodesWithGoalStates(String clusterId) throws Exception {
        log.debug("Getting all nodes with goal states from etcd");
        
        String prefix = pathResolver.getSearchUnitsPrefix(clusterId);
        GetOption option = GetOption.newBuilder().withPrefix(ByteSequence.from(prefix, UTF_8)).build();
        CompletableFuture<GetResponse> getFuture = kvClient.get(ByteSequence.from(prefix, UTF_8), option);
        GetResponse response = getFuture.get();
        
        List<String> nodeNames = new ArrayList<>();
        
        for (KeyValue kv : response.getKvs()) {
            String key = kv.getKey().toString(UTF_8);
            
            String relativePath = key.substring(prefix.length());
            if (relativePath.startsWith("/")) {
                relativePath = relativePath.substring(1);
            }
            String[] parts = relativePath.split("/");
            if (parts.length >= 2 && "goal-state".equals(parts[1])) {
                String unitName = parts[0];
                nodeNames.add(unitName);
                log.debug("Found goal-state for node: {} (key: {})", unitName, key);
            }
        }
        
        log.debug("Retrieved {} nodes with goal states from etcd", nodeNames.size());
        return nodeNames;
    }
    
    public void deleteActualAllocation(String clusterId, String indexName, String shardId) throws Exception {
        log.info("Deleting actual allocation for {}/{} from etcd", indexName, shardId);
        
        try {
            String actualAllocationPath = pathResolver.getShardActualAllocationPath(clusterId, indexName, shardId);
            executeEtcdDelete(actualAllocationPath);
            
            log.info("Successfully deleted actual allocation for {}/{} from etcd", indexName, shardId);
            
        } catch (Exception e) {
            log.error("Failed to delete actual allocation for {}/{} from etcd: {}", indexName, shardId, e.getMessage(), e);
            throw new Exception("Failed to delete actual allocation from etcd", e);
        }
    }
    
    @Override
    public Set<String> getAllIndicesWithActualAllocations(String clusterId) throws Exception {
        log.debug("Getting all indices with actual-allocation entries from etcd");
        
        Set<String> indicesWithAllocations = new HashSet<>();
        
        try {
            String indicesPrefix = pathResolver.getIndicesPrefix(clusterId);
            ByteSequence prefixBytes = ByteSequence.from(indicesPrefix, UTF_8);
            GetOption getOption = GetOption.newBuilder()
                    .withPrefix(prefixBytes)
                    .build();
            
            GetResponse response = kvClient.get(prefixBytes, getOption)
                    .get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            for (KeyValue kv : response.getKvs()) {
                String key = kv.getKey().toString(UTF_8);
                
                // Look for keys ending with "/actual-allocation"
                if (key.endsWith("/" + Constants.SUFFIX_ACTUAL_ALLOCATION)) {
                    // Extract index name from key pattern: /cluster/indices/INDEX_NAME/SHARD_ID/actual-allocation
                    String relativePath = key.substring(indicesPrefix.length());
                    if (relativePath.startsWith("/")) {
                        relativePath = relativePath.substring(1);
                    }
                    
                    String[] parts = relativePath.split("/");
                    if (parts.length >= 3 && Constants.SUFFIX_ACTUAL_ALLOCATION.equals(parts[2])) {
                        String indexName = parts[0];
                        indicesWithAllocations.add(indexName);
                        log.debug("Found actual-allocation for index: {} (key: {})", indexName, key);
                    }
                }
            }
            
            log.debug("Found {} indices with actual-allocation entries in etcd", indicesWithAllocations.size());
            return indicesWithAllocations;
            
        } catch (Exception e) {
            log.error("Failed to get indices with actual allocations from etcd: {}", e.getMessage(), e);
            throw e;
        }
    }
    // =================================================================
    // INDEX CONFIGURATIONS OPERATIONS
    // =================================================================
    
    public List<Index> getAllIndexConfigs(String clusterId) throws Exception {
        log.debug("Getting all index configs from etcd");

        try {
            String indicesPrefix = pathResolver.getIndicesPrefix(clusterId);
            GetResponse response = executeEtcdPrefixQuery(indicesPrefix);

            List<Index> indexConfigs = new ArrayList<>();
            for (var kv : response.getKvs()) {
                String key = kv.getKey().toString(StandardCharsets.UTF_8);
                // Only process keys that end with /conf (index configuration files)
                if (key.endsWith("/conf")) {
                    String indexConfigJson = kv.getValue().toString(StandardCharsets.UTF_8);
                    try {
                        Index indexConfig = objectMapper.readValue(indexConfigJson, Index.class);
                        indexConfigs.add(indexConfig);
                    } catch (Exception parseException) {
                        log.warn("Failed to parse index config JSON: {}, skipping", indexConfigJson);
                    }
                }
            }

            log.debug("Retrieved {} index configs from etcd", indexConfigs.size());
            return indexConfigs;

        } catch (Exception e) {
            log.error("Failed to get all index configs from etcd: {}", e.getMessage(), e);
            throw new Exception("Failed to retrieve index configs from etcd", e);
        }
    }
    
    public Optional<String> getIndexConfig(String clusterId, String indexName) throws Exception {
        log.debug("Getting index config {} from etcd", indexName);
        
        try {
            String indexPath = pathResolver.getIndexConfPath(clusterId, indexName);
            GetResponse response = executeEtcdGet(indexPath);
            
            if (response.getCount() == 0) {
                log.debug("Index config {} not found in etcd", indexName);
                return Optional.empty();
            }
            
            String indexConfigJson = response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
            
            log.debug("Retrieved index config {} from etcd", indexName);
            return Optional.of(indexConfigJson);
            
        } catch (Exception e) {
            log.error("Failed to get index config {} from etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to retrieve index config from etcd", e);
        }
    }
    
    public String createIndexConfig(String clusterId, String indexName, String indexConfig) throws Exception {
        log.info("Creating index config {} in etcd", indexName);
        
        try {
            String indexPath = pathResolver.getIndexConfPath(clusterId, indexName);
            executeEtcdPut(indexPath, indexConfig);
            
            log.info("Successfully created index config {} in etcd", indexName);
            return indexName;
            
        } catch (Exception e) {
            log.error("Failed to create index config {} in etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to create index config in etcd", e);
        }
    }
    
    public void updateIndexConfig(String clusterId, String indexName, String indexConfig) throws Exception {
        log.debug("Updating index config {} in etcd", indexName);
        
        try {
            String indexPath = pathResolver.getIndexConfPath(clusterId, indexName);
            executeEtcdPut(indexPath, indexConfig);
            
            log.debug("Successfully updated index config {} in etcd", indexName);
            
        } catch (Exception e) {
            log.error("Failed to update index config {} in etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to update index config in etcd", e);
        }
    }

    public void deleteIndexConfig(String clusterId, String indexName) throws Exception {
        log.info("Deleting index config {} from etcd", indexName);
        
        try {
            String indexPath = pathResolver.getIndexConfPath(clusterId, indexName);
            executeEtcdDelete(indexPath);
            
            log.info("Successfully deleted index config {} from etcd", indexName);
            
        } catch (Exception e) {
            log.error("Failed to delete index config {} from etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to delete index config from etcd", e);
        }
    }
    
    @Override
    public void setIndexMappings(String clusterId, String indexName, String mappings) throws Exception {
        log.debug("Setting index mappings for {} in etcd", indexName);
        
        try {
            String mappingsPath = pathResolver.getIndexMappingsPath(clusterId, indexName);
            executeEtcdPut(mappingsPath, mappings);
            
            log.debug("Successfully set index mappings for {} in etcd", indexName);
            
        } catch (Exception e) {
            log.error("Failed to set index mappings for {} in etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to set index mappings in etcd", e);
        }
    }
    
    @Override
    public TypeMapping getIndexMappings(String clusterId, String indexName) throws Exception {
        log.debug("Getting index mappings for {} from etcd", indexName);
        
        try {
            String mappingsPath = pathResolver.getIndexMappingsPath(clusterId, indexName);
            GetResponse response = executeEtcdGet(mappingsPath);
            
            if (response.getKvs().isEmpty()) {
                log.debug("No mappings found for index {} in cluster {}", indexName, clusterId);
                return null;
            }
            
            String mappingsJson = response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
            log.debug("Retrieved mappings for index {}: {}", indexName, mappingsJson);
            
            // Parse JSON to TypeMapping object
            return objectMapper.readValue(mappingsJson, TypeMapping.class);
            
        } catch (Exception e) {
            log.error("Failed to get index mappings for {} from etcd: {}", indexName, e.getMessage(), e);
            throw e;
        }
    }
    
    @Override
    public IndexSettings getIndexSettings(String clusterId, String indexName) throws Exception {
        log.debug("Getting index settings for {} from etcd", indexName);
        
        try {
            String settingsPath = pathResolver.getIndexSettingsPath(clusterId, indexName);
            GetResponse response = executeEtcdGet(settingsPath);
            
            if (response.getCount() == 0) {
                log.debug("Index settings {} not found in etcd", indexName);
                return null;
            }
            
            String settingsJson = response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
            log.debug("Retrieved index settings JSON for {}: {}", indexName, settingsJson);
            
            // Parse the JSON to check if it has a "settings" wrapper
            com.fasterxml.jackson.databind.JsonNode rootNode = objectMapper.readTree(settingsJson);
            
            IndexSettings settings;
            if (rootNode.has("index")) {
                // Extract the inner "settings" object
                com.fasterxml.jackson.databind.JsonNode settingsNode = rootNode.get("index");
                settings = objectMapper.treeToValue(settingsNode, IndexSettings.class);
                log.debug("Extracted settings from nested 'settings' field for {}", indexName);
            } else {
                // Direct deserialization if no wrapper
                settings = objectMapper.treeToValue(rootNode, IndexSettings.class);
                log.debug("Parsed settings directly for {}", indexName);
            }
            
            log.debug("Successfully parsed index settings for {}: {}", indexName, settings);
            return settings;
            
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            log.error("Failed to parse index settings JSON for {} from etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to parse index settings JSON from etcd", e);
        } catch (Exception e) {
            log.error("Failed to get index settings {} from etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to retrieve index settings from etcd", e);
        }
    }
    
    @Override
    public void setIndexSettings(String clusterId, String indexName, String settings) throws Exception {
        log.debug("Setting index settings for {} in etcd", indexName);
        
        try {
            // Wrap settings in "index" key to match Elasticsearch convention
            // Parse the incoming settings JSON and wrap it
            com.fasterxml.jackson.databind.JsonNode settingsNode = objectMapper.readTree(settings);
            java.util.Map<String, com.fasterxml.jackson.databind.JsonNode> wrappedSettings = new java.util.HashMap<>();
            wrappedSettings.put("index", settingsNode);
            String wrappedSettingsJson = objectMapper.writeValueAsString(wrappedSettings);
            
            String settingsPath = pathResolver.getIndexSettingsPath(clusterId, indexName);
            executeEtcdPut(settingsPath, wrappedSettingsJson);
            
            log.debug("Successfully set index settings for {} in etcd (wrapped in 'index' key)", indexName);
            
        } catch (Exception e) {
            log.error("Failed to set index settings for {} in etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to set index settings in etcd", e);
        }
    }
    
    // =================================================================
    // TEMPLATE OPERATIONS
    // =================================================================
    
    @Override
    public Template getTemplate(String clusterId, String templateName) throws Exception {
        log.debug("Getting template {} from etcd", templateName);
        
        try {
            String templatePath = pathResolver.getTemplateConfPath(clusterId, templateName);
            GetResponse response = executeEtcdGet(templatePath);
            
            if (response.getCount() == 0) {
                log.debug("Template {} not found in etcd", templateName);
                throw new IllegalArgumentException("Template '" + templateName + "' not found");
            }
            
            String templateConfigJson = response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
            Template template = objectMapper.readValue(templateConfigJson, Template.class);
            
            log.debug("Retrieved template {} from etcd", templateName);
            return template;
            
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to get template {} from etcd: {}", templateName, e.getMessage(), e);
            throw new Exception("Failed to retrieve template from etcd", e);
        }
    }
    
    @Override
    public String createTemplate(String clusterId, String templateName, String templateConfig) throws Exception {
        log.info("Creating template {} in etcd", templateName);
        
        try {
            String templatePath = pathResolver.getTemplateConfPath(clusterId, templateName);
            executeEtcdPut(templatePath, templateConfig);
            
            log.info("Successfully created template {} in etcd", templateName);
            return templateName;
            
        } catch (Exception e) {
            log.error("Failed to create template {} in etcd: {}", templateName, e.getMessage(), e);
            throw new Exception("Failed to create template in etcd", e);
        }
    }
    
    @Override
    public void updateTemplate(String clusterId, String templateName, String templateConfig) throws Exception {
        log.debug("Updating template {} in etcd", templateName);
        
        try {
            String templatePath = pathResolver.getTemplateConfPath(clusterId, templateName);
            executeEtcdPut(templatePath, templateConfig);
            
            log.debug("Successfully updated template {} in etcd", templateName);
            
        } catch (Exception e) {
            log.error("Failed to update template {} in etcd: {}", templateName, e.getMessage(), e);
            throw new Exception("Failed to update template in etcd", e);
        }
    }
    
    @Override
    public void deleteTemplate(String clusterId, String templateName) throws Exception {
        log.info("Deleting template {} from etcd", templateName);
        
        try {
            String templatePath = pathResolver.getTemplateConfPath(clusterId, templateName);
            executeEtcdDelete(templatePath);
            
            log.info("Successfully deleted template {} from etcd", templateName);
            
        } catch (Exception e) {
            log.error("Failed to delete template {} from etcd: {}", templateName, e.getMessage(), e);
            throw new Exception("Failed to delete template from etcd", e);
        }
    }
    
    @Override
    public List<Template> getAllTemplates(String clusterId) throws Exception {
        log.debug("Getting all templates from etcd for cluster {}", clusterId);
        
        try {
            String templatesPrefix = pathResolver.getTemplatesPrefix(clusterId);
            GetResponse response = executeEtcdPrefixQuery(templatesPrefix);
            
            List<Template> templates = new ArrayList<>();
            for (KeyValue kv : response.getKvs()) {
                String key = kv.getKey().toString(StandardCharsets.UTF_8);

                if (key.endsWith("/conf")) {
                    String templateJson = kv.getValue().toString(StandardCharsets.UTF_8);
                    try {
                        Template template = objectMapper.readValue(templateJson, Template.class);
                        templates.add(template);
                    } catch (Exception parseException) {
                        log.warn("Failed to parse template JSON: {}, skipping", templateJson);
                    }
                }
            }
            
            log.debug("Retrieved {} templates from etcd for cluster {}", templates.size(), clusterId);
            return templates;
            
        } catch (Exception e) {
            log.error("Failed to get all templates from etcd for cluster {}: {}", clusterId, e.getMessage(), e);
            throw new Exception("Failed to retrieve templates from etcd", e);
        }
    }
    
    // =================================================================
    // CLUSTER OPERATIONS
    // =================================================================
    
    public void initialize() throws Exception {
        log.info("Initialize called - already done in constructor");
        
        // TODO: Single-cluster leader election temporarily disabled
        // Multi-cluster coordination is now handled by MultiClusterManager using distributed locks
        // Each cluster is locked by exactly one controller via etcd's Lock API
        // leaderElection.startElection();
        
        log.info("Skipping single-cluster leader election - using MultiClusterManager for distributed coordination");
    }
    
    @PreDestroy
    public void close() throws Exception {
        log.info("Closing etcd metadata store");
        
        try {
            // Shutdown leader election first to avoid errors during etcd client closure
            if (leaderElection != null) {
                leaderElection.shutdown();
            }
            
            if (etcdClient != null) {
                etcdClient.close();
                log.info("etcd client closed successfully");
            }
        } catch (Exception e) {
            log.error("Error closing etcd client: {}", e.getMessage(), e);
            throw new Exception("Failed to close etcd client", e);
        }
    }
    
    
    /**
     * Get the path resolver for external use
     */
    public EtcdPathResolver getPathResolver() {
        return pathResolver;
    }
    
    // =================================================================
    // PRIVATE HELPER METHODS FOR ETCD OPERATIONS
    // =================================================================
    
    /**
     * Executes etcd prefix query to retrieve all keys matching the given prefix
     */
    private GetResponse executeEtcdPrefixQuery(String prefix) throws Exception {
        // Add trailing slash for etcd prefix queries to ensure precise matching
        String prefixWithSlash = prefix + PATH_DELIMITER;
        ByteSequence prefixBytes = ByteSequence.from(prefixWithSlash, StandardCharsets.UTF_8);
        return kvClient.get(
            prefixBytes,
            GetOption.newBuilder().withPrefix(prefixBytes).build()
        ).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Executes etcd get operation for a single key
     */
    private GetResponse executeEtcdGet(String key) throws Exception {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        return kvClient.get(keyBytes).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Executes etcd put operation for a key-value pair
     */
    private void executeEtcdPut(String key, String value) throws Exception {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        ByteSequence valueBytes = ByteSequence.from(value, StandardCharsets.UTF_8);
        kvClient.put(keyBytes, valueBytes).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Executes etcd delete operation for a key
     */
    private void executeEtcdDelete(String key) throws Exception {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        kvClient.delete(keyBytes).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Deserializes list of objects from etcd GetResponse
     */
    private <T> List<T> deserializeObjectList(GetResponse response, Class<T> clazz) throws Exception {
        List<T> items = new ArrayList<>();
        for (var kv : response.getKvs()) {
            String json = kv.getValue().toString(StandardCharsets.UTF_8);
            T item = objectMapper.readValue(json, clazz);
            items.add(item);
        }
        return items;
    }
    
    /**
     * Deserializes single object from etcd GetResponse
     */
    private <T> Optional<T> deserializeObject(GetResponse response, Class<T> clazz) throws Exception {
        if (response.getCount() == 0) {
            return Optional.empty();
        }
        String json = response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
        T item = objectMapper.readValue(json, clazz);
        return Optional.of(item);
    }
    
    /**
     * Retrieves all objects of a specific type using etcd prefix query
     */
    private <T> List<T> getAllObjectsByPrefix(String prefix, Class<T> clazz) throws Exception {
        GetResponse response = executeEtcdPrefixQuery(prefix);
        return deserializeObjectList(response, clazz);
    }
    
    /**
     * Retrieves single object by etcd path
     */
    private <T> Optional<T> getObjectByPath(String path, Class<T> clazz) throws Exception {
        GetResponse response = executeEtcdGet(path);
        return deserializeObject(response, clazz);
    }
    
    /**
     * Stores object as JSON at the specified etcd path
     */
    private void storeObjectAsJson(String path, Object object) throws Exception {
        String json = objectMapper.writeValueAsString(object);
        executeEtcdPut(path, json);
    }

     // =================================================================
    // LEADER ELECTION OPERATIONS
    // =================================================================
    
    /**
     * Get the leader election instance for direct access.
     * 
     * @return the LeaderElection instance
     */
    public LeaderElection getLeaderElection() {
        return leaderElection;
    }
    
    /**
     * Check if this node is currently the leader.
     * 
     * @return true if this node is the leader, false otherwise
     */
    public boolean isLeader() {
        return leaderElection.isLeader();
    }
    
    // =================================================================
    // SHARD ALLOCATION OPERATIONS
    // =================================================================
    
    @Override
    public ShardAllocation getPlannedAllocation(String clusterId, String indexName, String shardId) throws Exception {
        String path = pathResolver.getShardPlannedAllocationPath(clusterId, indexName, shardId);
        
        try {
            ByteSequence key = ByteSequence.from(path, UTF_8);
            GetResponse response = kvClient.get(key).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            if (response.getKvs().isEmpty()) {
                return null; // No planned allocation exists
            }
            
            String json = response.getKvs().get(0).getValue().toString(UTF_8);
            return objectMapper.readValue(json, ShardAllocation.class);
            
        } catch (Exception e) {
            log.error("Failed to get planned allocation for shard {}/{}: {}", indexName, shardId, e.getMessage(), e);
            throw e;
        }
    }
    
    @Override
    public void setPlannedAllocation(String clusterId, String indexName, String shardId, ShardAllocation allocation) throws Exception {
        String path = pathResolver.getShardPlannedAllocationPath(clusterId, indexName, shardId);
        
        try {
            String json = objectMapper.writeValueAsString(allocation);
            executeEtcdPut(path, json);
            log.debug("Set planned allocation for shard {}/{}: {}", indexName, shardId, allocation);
            
        } catch (Exception e) {
            log.error("Failed to set planned allocation for shard {}/{}: {}", indexName, shardId, e.getMessage(), e);
            throw e;
        }
    }
    
    @Override
    public ShardAllocation getActualAllocation(String clusterId, String indexName, String shardId) throws Exception {
        String path = pathResolver.getShardActualAllocationPath(clusterId, indexName, shardId);
        
        try {
            ByteSequence key = ByteSequence.from(path, UTF_8);
            GetResponse response = kvClient.get(key).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            if (response.getKvs().isEmpty()) {
                return null; // No actual allocation exists
            }
            
            String json = response.getKvs().get(0).getValue().toString(UTF_8);
            return objectMapper.readValue(json, ShardAllocation.class);
            
        } catch (Exception e) {
            log.error("Failed to get actual allocation for shard {}/{}: {}", indexName, shardId, e.getMessage(), e);
            throw e;
        }
    }
    
    @Override
    public void setActualAllocation(String clusterId, String indexName, String shardId, ShardAllocation allocation) throws Exception {
        String path = pathResolver.getShardActualAllocationPath(clusterId, indexName, shardId);
        
        try {
            String json = objectMapper.writeValueAsString(allocation);
            executeEtcdPut(path, json);
            log.debug("Set actual allocation for shard {}/{}: {}", indexName, shardId, allocation);
            
        } catch (Exception e) {
            log.error("Failed to set actual allocation for shard {}/{}: {}", indexName, shardId, e.getMessage(), e);
            throw e;
        }
    }
    
    @Override
    public List<ShardAllocation> getAllActualAllocations(String clusterId, String indexName) throws Exception {
        String indexPrefix = pathResolver.getIndicesPrefix(clusterId) + PATH_DELIMITER + indexName;
        
        try {
            List<ShardAllocation> allocations = new ArrayList<>();
            ByteSequence prefixBytes = ByteSequence.from(indexPrefix, UTF_8);
            GetOption getOption = GetOption.newBuilder()
                    .withPrefix(prefixBytes)
                    .build();
            
            GetResponse response = kvClient.get(prefixBytes, getOption)
                    .get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            for (KeyValue kv : response.getKvs()) {
                String key = kv.getKey().toString(UTF_8);
                // Only include keys that end with "/actual-allocation"
                if (key.endsWith("/" + Constants.SUFFIX_ACTUAL_ALLOCATION)) {
                    String json = kv.getValue().toString(UTF_8);
                    ShardAllocation allocation = objectMapper.readValue(json, ShardAllocation.class);
                    allocations.add(allocation);
                }
            }
            
            log.debug("Retrieved {} actual allocations for index {}", allocations.size(), indexName);
            return allocations;
            
        } catch (Exception e) {
            log.error("Failed to get actual allocations for index {}: {}", indexName, e.getMessage(), e);
            throw e;
        }
    }
    
    // =================================================================
    // COORDINATOR GOAL STATE OPERATIONS
    // =================================================================
    
    @Override
    public CoordinatorGoalState getCoordinatorGoalState(String clusterId) throws Exception {
        String path = pathResolver.getCoordinatorGoalStatePath(clusterId, coordinatorGoalStateGroup, coordinatorGoalStateUnit);
        
        try {
            ByteSequence key = ByteSequence.from(path, UTF_8);
            GetResponse response = kvClient.get(key).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            if (response.getKvs().isEmpty()) {
                return null; // No coordinator goal state exists
            }
            
            String json = response.getKvs().get(0).getValue().toString(UTF_8);
            return objectMapper.readValue(json, CoordinatorGoalState.class);
            
        } catch (Exception e) {
            log.error("Failed to get coordinator goal state: {}", e.getMessage(), e);
            throw e;
        }
    }
    
    @Override
    public void setCoordinatorGoalState(String clusterId, CoordinatorGoalState goalState) throws Exception {
        String path = pathResolver.getCoordinatorGoalStatePath(clusterId, coordinatorGoalStateGroup, coordinatorGoalStateUnit);
        
        try {
            String json = objectMapper.writeValueAsString(goalState);
            executeEtcdPut(path, json);
            log.debug("Set coordinator goal state: {}", goalState);
            
        } catch (Exception e) {
            log.error("Failed to set coordinator goal state: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Configure coordinator goal state location from application config
     */
    public void setCoordinatorGoalStateLocation(String searchUnitGroup, String searchUnit) {
        if (searchUnitGroup != null && !searchUnitGroup.isBlank()) {
            this.coordinatorGoalStateGroup = searchUnitGroup;
        }
        if (searchUnit != null && !searchUnit.isBlank()) {
            this.coordinatorGoalStateUnit = searchUnit;
        }
        log.info("Coordinator goal state path configured to group='{}', unit='{}'", this.coordinatorGoalStateGroup, this.coordinatorGoalStateUnit);
    }

    @Override
    public void deletePrefix(String clusterId, String prefix) throws Exception {
        log.debug("Deleting all keys with prefix {} in etcd", prefix);

        try {
            // Add trailing slash for etcd prefix queries to ensure precise matching
            String prefixWithSlash = prefix + PATH_DELIMITER;
            ByteSequence prefixBytes = ByteSequence.from(prefixWithSlash, StandardCharsets.UTF_8);
            
            // Use etcd delete with prefix option
            kvClient.delete(
                prefixBytes,
                DeleteOption.newBuilder().withPrefix(prefixBytes).build()
            ).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            log.debug("Successfully deleted all keys with prefix {} in etcd", prefix);
        } catch (Exception e) {
            log.error("Failed to delete keys with prefix {} in etcd: {}", prefix, e.getMessage(), e);
            throw new Exception("Failed to delete keys with prefix in etcd", e);
        }
    }
    
    /**
     * Get the controller ID assigned to a cluster.
     */
    @Override
    public ClusterControllerAssignment getAssignedController(String clusterId) throws Exception {
        try {
            String assignmentPath = pathResolver.getClusterAssignedControllerPath(clusterId);
            GetResponse getResponse = kvClient.get(
                ByteSequence.from(assignmentPath, UTF_8)
            ).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            if (getResponse.getKvs().isEmpty()) {
                log.debug("No controller assigned to cluster '{}'", clusterId);
                return null;
            }
            
            String json = getResponse.getKvs().get(0).getValue().toString(UTF_8);
            ClusterControllerAssignment clusterControllerAssignment = objectMapper.readValue(json, ClusterControllerAssignment.class);
            log.debug("Cluster '{}' is assigned to controller '{}'", clusterId, clusterControllerAssignment.getController());
            return clusterControllerAssignment;
        } catch (Exception e) {
            log.error("Failed to get assigned controller for cluster '{}': {}", clusterId, e.getMessage(), e);
            throw new Exception("Failed to get assigned controller: " + e.getMessage(), e);
        }
    }
    
    @Override
    public ClusterInformation.Version getClusterVersion(String clusterId) throws Exception {
        log.debug("Getting cluster version from registry for cluster '{}'", clusterId);
        
        try {
            String clusterRegistryPath = pathResolver.getClusterRegistryPath(clusterId);
            GetResponse response = executeEtcdGet(clusterRegistryPath);
            
            if (response.getKvs().isEmpty()) {
                log.debug("No cluster version found in registry for cluster '{}'", clusterId);
                return null;
            }
            
            String json = response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
            log.debug("Retrieved cluster metadata from registry for cluster '{}': {}", clusterId, json);
            
            // Parse as generic Map to extract version field
            Map<String, Object> metadata = objectMapper.readValue(json, Map.class);
            
            // Extract and deserialize the version field
            if (metadata.containsKey("version")) {
                Object versionObj = metadata.get("version");
                ClusterInformation.Version version = objectMapper.convertValue(versionObj, ClusterInformation.Version.class);
                return version;
            }
            
            return null;
            
        } catch (Exception e) {
            log.error("Failed to get cluster version from registry for '{}': {}", clusterId, e.getMessage(), e);
            throw e;
        }
    }
}
