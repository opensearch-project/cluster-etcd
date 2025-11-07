package io.clustercontroller.health;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.api.models.requests.ClusterInformationRequest;
import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.ShardState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.ClusterHealthInfo;
import io.clustercontroller.models.ClusterInformation;
import io.clustercontroller.models.IndexHealthInfo;
import io.clustercontroller.models.ShardHealthInfo;
import io.clustercontroller.models.ClusterControllerAssignment;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.util.EnvironmentUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.clustercontroller.config.Constants.LEVEL_INDICES;
import static io.clustercontroller.config.Constants.LEVEL_SHARDS;


/**
 * Manages cluster health monitoring and statistics collection.
 * 
 * Provides comprehensive health assessment of the cluster by aggregating
 * information from individual nodes, indices, and shards. Calculates overall
 * cluster status and detailed health metrics for monitoring and alerting.
 * 
 * Health calculation considers:
 * - Node availability and resource utilization
 * - Shard allocation status (active, relocating, unassigned)
 * - Index-level health and performance metrics
 * - Cluster-wide statistics and capacity planning data
 */
@Slf4j
public class ClusterHealthManager {
    
    private final MetadataStore metadataStore;
    private final ObjectMapper objectMapper;
    
   public ClusterHealthManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.objectMapper = new ObjectMapper();
   }
   
   /**
    * Helper class to hold shard count aggregates
    */
   private static class ShardCounts {
       int active = 0;
       int relocating = 0;
       int initializing = 0;
       int unassigned = 0;
       int failed = 0;
       
       int getTotal() {
           return active + relocating + initializing + unassigned + failed;
       }
   }
   
   /**
    * Helper class to hold node count aggregates
    */
   private static class NodeCounts {
       int total = 0;
       int coordinator = 0;
       int data = 0;
       int active = 0;
       Map<HealthState, Integer> byHealth = new HashMap<>();
       
       NodeCounts() {
           byHealth.put(HealthState.GREEN, 0);
           byHealth.put(HealthState.YELLOW, 0);
           byHealth.put(HealthState.RED, 0);
       }
   }
   
   /**
    * Shared method to count shards by state across all nodes.
    * Optionally filter by index name.
    */
   private ShardCounts countShardsByState(Map<String, SearchUnitActualState> actualStates, String indexFilter) {
       ShardCounts counts = new ShardCounts();
       
       for (SearchUnitActualState actualState : actualStates.values()) {
           if (actualState.getNodeRouting() != null) {
               for (Map.Entry<String, List<SearchUnitActualState.ShardRoutingInfo>> entry : actualState.getNodeRouting().entrySet()) {
                   // Apply index filter if specified
                   if (indexFilter != null && !indexFilter.equals(entry.getKey())) {
                       continue;
                   }
                   
                   for (SearchUnitActualState.ShardRoutingInfo shard : entry.getValue()) {
                       switch (shard.getState()) {
                           case STARTED:
                               counts.active++;
                               break;
                           case RELOCATING:
                               counts.relocating++;
                               break;
                           case INITIALIZING:
                               counts.initializing++;
                               break;
                           case UNASSIGNED:
                               counts.unassigned++;
                               break;
                           case FAILED:
                               counts.failed++;
                               break;
                       }
                   }
               }
           }
       }
       
       return counts;
   }
   
   /**
    * Shared method to count nodes by role and health state
    */
   private NodeCounts countNodesByRoleAndHealth(Map<String, SearchUnitActualState> actualStates) {
       NodeCounts counts = new NodeCounts();
       counts.total = actualStates.size();
       
       for (SearchUnitActualState unit : actualStates.values()) {
           // Count by role
           NodeRole role = NodeRole.fromString(unit.getRole());
           if (NodeRole.COORDINATOR.equals(role)) {
               counts.coordinator++;
           } else {
               counts.data++;
           }
           
           // Count by health state
           HealthState health = unit.deriveNodeState();
           counts.byHealth.put(health, counts.byHealth.get(health) + 1);
           
           // Count active nodes (GREEN or YELLOW)
           if (health.isHealthy()) {
               counts.active++;
           }
       }
       
       return counts;
   }
  
   public String getClusterHealth(String clusterId, String level) throws Exception {
       log.info("Getting cluster health for cluster '{}' with level: {}", clusterId, level);
      
       try {
           // Get all search units and their actual states
           Map<String, SearchUnitActualState> actualStates = metadataStore.getAllSearchUnitActualStates(clusterId);

           if (actualStates.isEmpty()) {
               throw new Exception("No search units found for cluster '" + clusterId + "'");
           }

           List<Index> indices = metadataStore.getAllIndexConfigs(clusterId);

           log.info("Found {} search units, {} actual states, {} indices for cluster '{}'",
                   actualStates.size(), actualStates.size(), indices.size(), clusterId);
          
           // Calculate cluster health
           ClusterHealthInfo healthResponse = calculateClusterHealth(clusterId, actualStates, indices, level);
          
           // Convert to JSON
           String healthJson = objectMapper.writeValueAsString(healthResponse);
           log.info("Successfully calculated cluster health for cluster '{}': nodes={}, dataNodes={}, activeNodes={}, nodesByHealth={}",
                   clusterId, healthResponse.getNumberOfNodes(), healthResponse.getNumberOfDataNodes(),
                   healthResponse.getActiveNodes(), healthResponse.getNodesByHealth());
           return healthJson;
          
       } catch (Exception e) {
           log.error("Failed to get cluster health for cluster '{}': {}", clusterId, e.getMessage(), e);
           throw new Exception("Failed to calculate cluster health: " + e.getMessage(), e);
       }
   }
  
   /**
    * Calculate comprehensive cluster health based on nodes, indices, and shards
    */
   private ClusterHealthInfo calculateClusterHealth(String clusterId,
           Map<String, SearchUnitActualState> actualStates, List<Index> indices, String level) {
      
      ClusterHealthInfo response = new ClusterHealthInfo();
      response.setClusterName(clusterId);
      
       // Calculate node statistics
       calculateNodeHealth(response, actualStates);
      
       // Calculate shard statistics
       calculateShardHealth(response, actualStates);
      
       // Calculate index statistics
       calculateIndexHealth(response, indices);
      
      // Determine overall cluster status
      response.setStatus(determineOverallClusterStatus(response));
      
      // Add detailed information based on level
      if (LEVEL_INDICES.equals(level) || LEVEL_SHARDS.equals(level)) {
          addDetailedIndexInfo(response, indices, actualStates, level);
      }
      
      log.info("Calculated cluster health: status={}, nodes={}/{}/{}, active_shards={}",
          response.getStatus(), response.getNumberOfDataNodes(), response.getNumberOfNodes(),
          response.getActiveNodes(), response.getActiveShards());
      
       return response;
   }
  
   /**
    * Calculate node health statistics
    */
   private void calculateNodeHealth(ClusterHealthInfo response,
        Map<String, SearchUnitActualState> actualStates) {
      
      NodeCounts counts = countNodesByRoleAndHealth(actualStates);
      
      response.setNumberOfNodes(counts.total);
      response.setNumberOfDataNodes(counts.data);
      response.setNumberOfCoordinatorNodes(counts.coordinator);
      response.setActiveNodes(counts.active);
      response.setNodesByHealth(counts.byHealth);
   }
  
   /**
    * Calculate shard health statistics
    */
   private void calculateShardHealth(ClusterHealthInfo response, Map<String, SearchUnitActualState> actualStates) {
      ShardCounts counts = countShardsByState(actualStates, null);
      
      response.setActiveShards(counts.active);
      response.setRelocatingShards(counts.relocating);
      response.setInitializingShards(counts.initializing);
      response.setUnassignedShards(counts.unassigned);
      response.setFailedShards(counts.failed);
      response.setTotalShards(counts.getTotal());
   }
  
   /**
    * Calculate index health statistics
    */
   private void calculateIndexHealth(ClusterHealthInfo response, List<Index> indices) {
      response.setNumberOfIndices(indices.size());
      response.setTotalShards(Math.max(response.getTotalShards(),
          indices.stream().mapToInt(index -> index.getSettings().getNumberOfShards()).sum()));
   }
  
   /**
    * Determine overall cluster health status
    */
   private HealthState determineOverallClusterStatus(ClusterHealthInfo response) {
      // RED if any failed shards or no active nodes
      if (response.getFailedShards() > 0 || response.getActiveNodes() == 0) {
          return HealthState.RED;
      }
      
      // RED if more than half of data nodes are unhealthy
      int unhealthyDataNodes = response.getNodesByHealth().get(HealthState.RED);
      if (response.getNumberOfDataNodes() > 0 && unhealthyDataNodes > response.getNumberOfDataNodes() / 2) {
          return HealthState.RED;
      }
      
      // YELLOW if any unassigned shards or relocating shards
      if (response.getUnassignedShards() > 0 || response.getRelocatingShards() > 0) {
          return HealthState.YELLOW;
      }
      
      // YELLOW if any nodes are in YELLOW state
      if (response.getNodesByHealth().get(HealthState.YELLOW) > 0) {
          return HealthState.YELLOW;
      }
      
      // GREEN if all conditions are good
      return HealthState.GREEN;
   }
  
   /**
    * Add detailed index information based on level
    */
  private void addDetailedIndexInfo(ClusterHealthInfo response, List<Index> indices,
        Map<String, SearchUnitActualState> actualStates, String level) {
      
      for (Index index : indices) {
          String indexName = index.getIndexName();
          
          // Calculate full index health including shard details
          IndexHealthInfo indexHealth = calculateSingleIndexHealth(indexName, index, actualStates);
          
          // If not requesting shard-level details, remove them
          if (!LEVEL_SHARDS.equals(level)) {
              indexHealth.setShards(null);
          }
         
          response.getIndices().put(indexName, indexHealth);
      }
   }
  
   public String getIndexHealth(String clusterId, String indexName, String level) throws Exception {
       log.info("Getting health for index '{}' in cluster '{}' with level: {}", indexName, clusterId, level);
      
       try {
           // Get all search unit actual states and the specific index config
           Map<String, SearchUnitActualState> actualStates = metadataStore.getAllSearchUnitActualStates(clusterId);
           Optional<String> indexConfigJson = metadataStore.getIndexConfig(clusterId, indexName);
          
           if (!indexConfigJson.isPresent()) {
               throw new IllegalArgumentException("Index '" + indexName + "' not found in cluster '" + clusterId + "'");
           }
          
           // Parse index config from JSON
           Index index = objectMapper.readValue(indexConfigJson.get(), Index.class);
          
           log.info("Found index '{}' with {} actual states", indexName, actualStates.size());
          
           // Calculate index health
           IndexHealthInfo indexHealth = calculateSingleIndexHealth(indexName, index, actualStates);
           
           // Remove shard details if not requested
           if (!LEVEL_SHARDS.equals(level)) {
               indexHealth.setShards(null);
           }
          
           // Convert to JSON
           String healthJson = objectMapper.writeValueAsString(indexHealth);
           log.info("Successfully calculated health for index '{}': status={}, activeShards={}/{}",
                   indexName, indexHealth.getStatus(), indexHealth.getActiveShards(), indexHealth.getNumberOfShards());
           return healthJson;
          
       } catch (IllegalArgumentException e) {
           log.error("Index not found: {}", e.getMessage());
           throw e;
       } catch (Exception e) {
           log.error("Failed to get health for index '{}' in cluster '{}': {}", indexName, clusterId, e.getMessage(), e);
           throw new Exception("Failed to calculate index health: " + e.getMessage(), e);
       }
   }
  
   /**
    * Calculate health for a single index
    */
   private IndexHealthInfo calculateSingleIndexHealth(String indexName, Index index,
           Map<String, SearchUnitActualState> actualStates) {
      
       IndexHealthInfo indexHealth = new IndexHealthInfo();
      
       // Get expected shard configuration
       Integer numberOfShards = index.getSettings() != null ? index.getSettings().getNumberOfShards() : 1;
       List<Integer> replicaCount = index.getSettings() != null ? index.getSettings().getShardReplicaCount() : null;
      
       indexHealth.setNumberOfShards(numberOfShards != null ? numberOfShards : 1);
       indexHealth.setNumberOfReplicas(replicaCount != null ? replicaCount.size() : 0);
      
       // Count shards by state for this index
       Map<String, ShardHealthInfo> shardHealthMap = new HashMap<>();
       int activeShards = 0;
       int relocatingShards = 0;
       int initializingShards = 0;
       int unassignedShards = 0;
      
       // Collect shard information from all nodes
       for (SearchUnitActualState unit : actualStates.values()) {
           if (unit.getNodeRouting() != null && unit.getNodeRouting().containsKey(indexName)) {
               List<SearchUnitActualState.ShardRoutingInfo> shards = unit.getNodeRouting().get(indexName);
              
               for (SearchUnitActualState.ShardRoutingInfo shard : shards) {
                   String shardKey = String.valueOf(shard.getShardId());
                  
                   // Create or update shard health info
                   ShardHealthInfo shardHealth = shardHealthMap.computeIfAbsent(shardKey, k -> {
                       ShardHealthInfo sh = new ShardHealthInfo();
                       sh.setShardId(shard.getShardId());
                       sh.setStatus(HealthState.RED); // Default to RED, will update based on state
                       sh.setPrimaryActive(false);
                       sh.setActiveReplicas(0);
                       sh.setRelocatingReplicas(0);
                       sh.setInitializingReplicas(0);
                       sh.setUnassignedReplicas(0);
                       return sh;
                   });
                  
                   // Update counts based on shard state
                   if (ShardState.STARTED.equals(shard.getState())) {
                       activeShards++;
                       if (shard.getRole().equals("primary")) {
                           shardHealth.setPrimaryActive(true);
                           shardHealth.setStatus(HealthState.GREEN);
                       } else {
                           shardHealth.setActiveReplicas(shardHealth.getActiveReplicas() + 1);
                       }
                   } else if (ShardState.RELOCATING.equals(shard.getState())) {
                       relocatingShards++;
                       if (!shard.getRole().equals("primary")) {
                           shardHealth.setRelocatingReplicas(shardHealth.getRelocatingReplicas() + 1);
                       }
                       if (shardHealth.getStatus() == HealthState.RED) {
                           shardHealth.setStatus(HealthState.YELLOW);
                       }
                   } else if (ShardState.INITIALIZING.equals(shard.getState())) {
                       initializingShards++;
                       if (!shard.getRole().equals("primary")) {
                           shardHealth.setInitializingReplicas(shardHealth.getInitializingReplicas() + 1);
                       }
                       if (shardHealth.getStatus() == HealthState.RED) {
                           shardHealth.setStatus(HealthState.YELLOW);
                       }
                   } else if (ShardState.UNASSIGNED.equals(shard.getState())) {
                       unassignedShards++;
                       if (!shard.getRole().equals("primary")) {
                           shardHealth.setUnassignedReplicas(shardHealth.getUnassignedReplicas() + 1);
                       }
                   }
               }
           }
       }
      
       indexHealth.setActiveShards(activeShards);
       indexHealth.setRelocatingShards(relocatingShards);
       indexHealth.setInitializingShards(initializingShards);
       indexHealth.setUnassignedShards(unassignedShards);
       indexHealth.setShards(shardHealthMap);
      
       // Determine overall index status
       if (unassignedShards > 0 || activeShards == 0) {
           indexHealth.setStatus(HealthState.RED);
       } else if (relocatingShards > 0 || initializingShards > 0) {
           indexHealth.setStatus(HealthState.YELLOW);
       } else {
           indexHealth.setStatus(HealthState.GREEN);
       }
      
       return indexHealth;
   }
  
    public String getClusterStats(String clusterId) {
        log.info("Getting cluster statistics");
        // TODO: Implement cluster statistics aggregation
        throw new UnsupportedOperationException("Cluster stats not yet implemented");
    }

    /**
     * Get cluster information for the specified cluster.
     */
    public String getClusterInformation(String clusterId) throws Exception {
        log.info("Getting cluster information for cluster '{}'", clusterId);
        
        try {
            ClusterInformation clusterInfo = new ClusterInformation();
            clusterInfo.setClusterName(clusterId);
            clusterInfo.setClusterUuid(clusterId);
            
            // Get the controller name assigned to this cluster from etcd
            ClusterControllerAssignment assignedController = metadataStore.getAssignedController(clusterId);
            if (assignedController != null) {
                clusterInfo.setName(assignedController.getController());
                log.debug("Set controller name '{}' for cluster '{}'", assignedController.getController(), clusterId);
            } else {
                log.warn("No controller assigned to cluster '{}'", clusterId);
                throw new Exception("Cluster is not associated with a controller");
            }
            
            // Read cluster version information from cluster registry metadata
            try {
                ClusterInformation.Version version = metadataStore.getClusterVersion(clusterId);
                if (version != null) {
                    clusterInfo.setVersion(version);
                    log.debug("Set cluster version from registry for cluster '{}': {}", 
                        clusterId, version.getNumber());
                } else {
                    log.debug("No version information found in cluster registry for cluster '{}'", clusterId);
                }
            } catch (Exception e) {
                log.warn("Failed to read cluster version from registry for cluster '{}': {}", 
                    clusterId, e.getMessage());
            }

            return objectMapper.writeValueAsString(clusterInfo);
        } catch (Exception e) {
            log.error("Failed to get cluster information for cluster '{}': {}", clusterId, e.getMessage(), e);
            throw new Exception("Failed to get cluster information: " + e.getMessage(), e);
        }
    }
    /**
     * Set/update cluster version information for the specified cluster.
     */
    public void setClusterInformation(String clusterId, ClusterInformationRequest request) throws Exception {
        log.info("Setting cluster information for cluster '{}'", clusterId);
        
        try {
            // Extract just the version object from the request
            ClusterInformation.Version version = request.getVersion();
            
            // Validate version is present
            if (version == null) {
                throw new IllegalArgumentException("Version information is required");
            }
            
            // Store only the version field at the cluster registry path
            // Path: /multi-cluster/clusters/<cluster-id>/metadata
            metadataStore.setClusterVersion(clusterId, version);
            log.info("Successfully set cluster version for cluster '{}': {}", 
                clusterId, version.getNumber());
            
        } catch (IllegalArgumentException e) {
            log.error("Invalid cluster information for cluster '{}': {}", clusterId, e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Failed to set cluster information for cluster '{}': {}", clusterId, e.getMessage(), e);
            throw new Exception("Failed to set cluster information: " + e.getMessage(), e);
        }
    }
}
