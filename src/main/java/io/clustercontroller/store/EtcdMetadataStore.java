package io.clustercontroller.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.TaskMetadata;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.GetOption;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * etcd-based implementation of MetadataStore.
 * Singleton to ensure single etcd client connection.
 */
@Slf4j
public class EtcdMetadataStore implements MetadataStore {
    
    // TODO: Make etcd timeout configurable via environment variable or config
    private static final long ETCD_OPERATION_TIMEOUT_SECONDS = 5;
    
    private static EtcdMetadataStore instance;
    
    private final String clusterName;
    private final String[] etcdEndpoints;
    private final Client etcdClient;
    private final KV kvClient;
    private final EtcdPathResolver pathResolver;
    private final ObjectMapper objectMapper;
    
    /**
     * Private constructor for singleton pattern
     */
    private EtcdMetadataStore(String clusterName, String[] etcdEndpoints) throws Exception {
        this.clusterName = clusterName;
        this.etcdEndpoints = etcdEndpoints;
        
        // Initialize Jackson ObjectMapper
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        
        // Initialize etcd client
        this.etcdClient = Client.builder().endpoints(etcdEndpoints).build();
        this.kvClient = etcdClient.getKVClient();
        
        // Initialize path resolver
        this.pathResolver = new EtcdPathResolver(clusterName);
        
        log.info("EtcdMetadataStore initialized for cluster: {} with endpoints: {}", 
            clusterName, String.join(",", etcdEndpoints));
    }
    
    // =================================================================
    // HELPER METHODS FOR COMMON ETCD OPERATIONS
    // =================================================================
    
    /**
     * Helper method for etcd get operations with prefix
     */
    private GetResponse getWithPrefix(String prefix) throws Exception {
        ByteSequence prefixBytes = ByteSequence.from(prefix, StandardCharsets.UTF_8);
        return kvClient.get(
            prefixBytes,
            GetOption.newBuilder().withPrefix(prefixBytes).build()
        ).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Helper method for etcd get operations for single key
     */
    private GetResponse getSingle(String key) throws Exception {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        return kvClient.get(keyBytes).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Helper method for etcd put operations
     */
    private void putValue(String key, String value) throws Exception {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        ByteSequence valueBytes = ByteSequence.from(value, StandardCharsets.UTF_8);
        kvClient.put(keyBytes, valueBytes).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Helper method for etcd delete operations
     */
    private void deleteKey(String key) throws Exception {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        kvClient.delete(keyBytes).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
    
    /**
     * Helper method to deserialize list of objects from etcd response
     */
    private <T> List<T> deserializeList(GetResponse response, Class<T> clazz) throws Exception {
        List<T> items = new ArrayList<>();
        for (var kv : response.getKvs()) {
            String json = kv.getValue().toString(StandardCharsets.UTF_8);
            T item = objectMapper.readValue(json, clazz);
            items.add(item);
        }
        return items;
    }
    
    /**
     * Helper method to deserialize single object from etcd response
     */
    private <T> Optional<T> deserializeSingle(GetResponse response, Class<T> clazz) throws Exception {
        if (response.getCount() == 0) {
            return Optional.empty();
        }
        String json = response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
        T item = objectMapper.readValue(json, clazz);
        return Optional.of(item);
    }
    
    /**
     * Get singleton instance
     */
    public static synchronized EtcdMetadataStore getInstance(String clusterName, String[] etcdEndpoints) throws Exception {
        if (instance == null) {
            instance = new EtcdMetadataStore(clusterName, etcdEndpoints);
        }
        return instance;
    }
    
    /**
     * Get existing instance (throws if not initialized)
     */
    public static EtcdMetadataStore getInstance() {
        if (instance == null) {
            throw new IllegalStateException("EtcdMetadataStore not initialized. Call getInstance(clusterName, etcdEndpoints) first.");
        }
        return instance;
    }
    
    // =================================================================
    // CONTROLLER TASKS OPERATIONS
    // =================================================================
    
    @Override
    public List<TaskMetadata> getAllTasks() throws Exception {
        log.debug("Getting all tasks from etcd");
        
        try {
            String tasksPrefix = pathResolver.getControllerTasksPrefix();
            GetResponse response = getWithPrefix(tasksPrefix);
            
            List<TaskMetadata> tasks = deserializeList(response, TaskMetadata.class);
            
            // Sort by priority (0 = highest priority)
            tasks.sort((t1, t2) -> Integer.compare(t1.getPriority(), t2.getPriority()));
            
            log.debug("Retrieved {} tasks from etcd", tasks.size());
            return tasks;
            
        } catch (Exception e) {
            log.error("Failed to get all tasks from etcd: {}", e.getMessage(), e);
            throw new Exception("Failed to retrieve tasks from etcd", e);
        }
    }
    
    @Override
    public Optional<TaskMetadata> getTask(String taskName) throws Exception {
        log.debug("Getting task {} from etcd", taskName);
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(taskName);
            GetResponse response = getSingle(taskPath);
            
            Optional<TaskMetadata> result = deserializeSingle(response, TaskMetadata.class);
            
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
    
    @Override
    public String createTask(TaskMetadata task) throws Exception {
        log.info("Creating task {} in etcd", task.getName());
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(task.getName());
            String taskJson = objectMapper.writeValueAsString(task);
            
            putValue(taskPath, taskJson);
            
            log.info("Successfully created task {} in etcd", task.getName());
            return task.getName();
            
        } catch (Exception e) {
            log.error("Failed to create task {} in etcd: {}", task.getName(), e.getMessage(), e);
            throw new Exception("Failed to create task in etcd", e);
        }
    }
    
    @Override
    public void updateTask(TaskMetadata task) throws Exception {
        log.debug("Updating task {} in etcd", task.getName());
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(task.getName());
            String taskJson = objectMapper.writeValueAsString(task);
            
            putValue(taskPath, taskJson);
            
            log.debug("Successfully updated task {} in etcd", task.getName());
            
        } catch (Exception e) {
            log.error("Failed to update task {} in etcd: {}", task.getName(), e.getMessage(), e);
            throw new Exception("Failed to update task in etcd", e);
        }
    }
    
    @Override
    public void deleteTask(String taskName) throws Exception {
        log.info("Deleting task {} from etcd", taskName);
        
        try {
            String taskPath = pathResolver.getControllerTaskPath(taskName);
            deleteKey(taskPath);
            
            log.info("Successfully deleted task {} from etcd", taskName);
            
        } catch (Exception e) {
            log.error("Failed to delete task {} from etcd: {}", taskName, e.getMessage(), e);
            throw new Exception("Failed to delete task from etcd", e);
        }
    }
    
    @Override
    public void deleteOldTasks(long olderThanTimestamp) throws Exception {
        log.debug("Deleting old tasks from etcd older than {}", olderThanTimestamp);
        // TODO: Implement etcd cleanup for old tasks
    }
    
    // =================================================================
    // SEARCH UNITS OPERATIONS
    // =================================================================
    
    @Override
    public List<SearchUnit> getAllSearchUnits() throws Exception {
        log.debug("Getting all search units from etcd");
        
        try {
            String unitsPrefix = pathResolver.getSearchUnitsPrefix();
            GetResponse response = getWithPrefix(unitsPrefix);
            
            List<SearchUnit> searchUnits = deserializeList(response, SearchUnit.class);
            
            log.debug("Retrieved {} search units from etcd", searchUnits.size());
            return searchUnits;
            
        } catch (Exception e) {
            log.error("Failed to get all search units from etcd: {}", e.getMessage(), e);
            throw new Exception("Failed to retrieve search units from etcd", e);
        }
    }
    
    @Override
    public Optional<SearchUnit> getSearchUnit(String unitName) throws Exception {
        log.debug("Getting search unit {} from etcd", unitName);
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(unitName);
            GetResponse response = getSingle(unitPath);
            
            Optional<SearchUnit> result = deserializeSingle(response, SearchUnit.class);
            
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
    
    @Override
    public void upsertSearchUnit(String unitName, SearchUnit searchUnit) throws Exception {
        log.info("Upserting search unit {} in etcd", unitName);
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(unitName);
            String unitJson = objectMapper.writeValueAsString(searchUnit);
            
            putValue(unitPath, unitJson);
            
            log.info("Successfully upserted search unit {} in etcd", unitName);
            
        } catch (Exception e) {
            log.error("Failed to upsert search unit {} in etcd: {}", unitName, e.getMessage(), e);
            throw new Exception("Failed to upsert search unit in etcd", e);
        }
    }
    
    @Override
    public void updateSearchUnit(SearchUnit searchUnit) throws Exception {
        log.debug("Updating search unit {} in etcd", searchUnit.getName());
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(searchUnit.getName());
            String unitJson = objectMapper.writeValueAsString(searchUnit);
            
            putValue(unitPath, unitJson);
            
            log.debug("Successfully updated search unit {} in etcd", searchUnit.getName());
            
        } catch (Exception e) {
            log.error("Failed to update search unit {} in etcd: {}", searchUnit.getName(), e.getMessage(), e);
            throw new Exception("Failed to update search unit in etcd", e);
        }
    }
    
    @Override
    public void deleteSearchUnit(String unitName) throws Exception {
        log.info("Deleting search unit {} from etcd", unitName);
        
        try {
            String unitPath = pathResolver.getSearchUnitConfPath(unitName);
            deleteKey(unitPath);
            
            log.info("Successfully deleted search unit {} from etcd", unitName);
            
        } catch (Exception e) {
            log.error("Failed to delete search unit {} from etcd: {}", unitName, e.getMessage(), e);
            throw new Exception("Failed to delete search unit from etcd", e);
        }
    }
    
    // =================================================================
    // INDEX CONFIGURATIONS OPERATIONS
    // =================================================================
    
    @Override
    public List<String> getAllIndexConfigs() throws Exception {
        log.debug("Getting all index configs from etcd");
        
        try {
            String indicesPrefix = pathResolver.getIndicesPrefix();
            GetResponse response = getWithPrefix(indicesPrefix);
            
            List<String> indexConfigs = new ArrayList<>();
            for (var kv : response.getKvs()) {
                String indexConfig = kv.getValue().toString(StandardCharsets.UTF_8);
                indexConfigs.add(indexConfig);
            }
            
            log.debug("Retrieved {} index configs from etcd", indexConfigs.size());
            return indexConfigs;
            
        } catch (Exception e) {
            log.error("Failed to get all index configs from etcd: {}", e.getMessage(), e);
            throw new Exception("Failed to retrieve index configs from etcd", e);
        }
    }
    
    @Override
    public Optional<String> getIndexConfig(String indexName) throws Exception {
        log.debug("Getting index config {} from etcd", indexName);
        
        try {
            String indexPath = pathResolver.getIndexConfPath(indexName);
            GetResponse response = getSingle(indexPath);
            
            if (response.getCount() == 0) {
                log.debug("Index config {} not found in etcd", indexName);
                return Optional.empty();
            }
            
            String indexConfig = response.getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
            
            log.debug("Retrieved index config {} from etcd", indexName);
            return Optional.of(indexConfig);
            
        } catch (Exception e) {
            log.error("Failed to get index config {} from etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to retrieve index config from etcd", e);
        }
    }
    
    @Override
    public String createIndexConfig(String indexName, String indexConfig) throws Exception {
        log.info("Creating index config {} in etcd", indexName);
        
        try {
            String indexPath = pathResolver.getIndexConfPath(indexName);
            putValue(indexPath, indexConfig);
            
            log.info("Successfully created index config {} in etcd", indexName);
            return indexName;
            
        } catch (Exception e) {
            log.error("Failed to create index config {} in etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to create index config in etcd", e);
        }
    }
    
    @Override
    public void updateIndexConfig(String indexName, String indexConfig) throws Exception {
        log.debug("Updating index config {} in etcd", indexName);
        
        try {
            String indexPath = pathResolver.getIndexConfPath(indexName);
            putValue(indexPath, indexConfig);
            
            log.debug("Successfully updated index config {} in etcd", indexName);
            
        } catch (Exception e) {
            log.error("Failed to update index config {} in etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to update index config in etcd", e);
        }
    }
    
    @Override
    public void deleteIndexConfig(String indexName) throws Exception {
        log.info("Deleting index config {} from etcd", indexName);
        
        try {
            String indexPath = pathResolver.getIndexConfPath(indexName);
            deleteKey(indexPath);
            
            log.info("Successfully deleted index config {} from etcd", indexName);
            
        } catch (Exception e) {
            log.error("Failed to delete index config {} from etcd: {}", indexName, e.getMessage(), e);
            throw new Exception("Failed to delete index config from etcd", e);
        }
    }
    
    // =================================================================
    // CLUSTER OPERATIONS
    // =================================================================
    
    @Override
    public void initialize() throws Exception {
        log.info("Initialize called - already done in constructor");
    }
    
    @Override
    public void close() throws Exception {
        log.info("Closing etcd metadata store");
        
        try {
            if (etcdClient != null) {
                etcdClient.close();
                log.info("etcd client closed successfully");
            }
        } catch (Exception e) {
            log.error("Error closing etcd client: {}", e.getMessage(), e);
            throw new Exception("Failed to close etcd client", e);
        }
    }
    
    @Override
    public String getClusterName() {
        return clusterName;
    }
    
    /**
     * Get the path resolver for external use
     */
    public EtcdPathResolver getPathResolver() {
        return pathResolver;
    }
}
