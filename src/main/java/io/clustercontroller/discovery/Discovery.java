package io.clustercontroller.discovery;

import io.clustercontroller.config.Constants;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.models.NodeAttributes;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles cluster topology discovery.
 * Internal component used by TaskManager.
 * 
 * Note: This is a shared component across all clusters in multi-cluster mode.
 * Methods accept clusterName as a parameter instead of storing it as a field.
 */
@Slf4j
public class Discovery {
    
    private final MetadataStore metadataStore;
    
    public Discovery(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }
    

    public void discoverSearchUnits(String clusterName) throws Exception {
        log.info("Discovery - Starting search unit discovery process for cluster: {}", clusterName);
        
        // Discover and update search units from Etcd actual-states
        discoverSearchUnitsFromEtcd(clusterName);
        
        // Clean up stale search units before processing
        cleanupStaleSearchUnits(clusterName);
        
        // Process all search units to ensure they're up-to-date
        processAllSearchUnits(clusterName);
        
        log.info("Discovery - Completed search unit discovery process for cluster: {}", clusterName);
    }
    
    /**
     * Process all search units to ensure they're current
     */
    private void processAllSearchUnits(String clusterName) {
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
    private void discoverSearchUnitsFromEtcd(String clusterName) {
        try {
            log.info("Discovery - Discovering search units from Etcd...");
            // fetch search units from actual-state paths
            List<SearchUnit> etcdSearchUnits = fetchSearchUnitsFromEtcd(clusterName); 
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
    public List<SearchUnit> fetchSearchUnitsFromEtcd(String clusterName) {
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
    private void cleanupStaleSearchUnits(String clusterName) {
        try {
            log.info("Discovery - Starting cleanup of stale search units...");
            
            // Get all existing search units from metadata store
            List<SearchUnit> allSearchUnits = metadataStore.getAllSearchUnits(clusterName);
            int deletedCount = 0;
            
            for (SearchUnit searchUnit : allSearchUnits) {
                String unitName = searchUnit.getName();
                
                try {
                    // Check if actual state exists
                    SearchUnitActualState actualState = 
                            metadataStore.getSearchUnitActualState(clusterName, unitName);
                    
                    boolean shouldDelete = false;
                    String reason = "";
                    
                    if (actualState == null) {
                        // Case 1: Missing actual state
                        shouldDelete = true;
                        reason = "missing actual state";
                    } else {
                        // Case 2: Check if timestamp is older than configured timeout
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

    public void monitorClusterHealth() {
        log.info("Monitoring cluster health");
        // TODO: Implement cluster health monitoring logic
    }
    
    public void updateClusterTopology() {
        log.info("Updating cluster topology state");
        // TODO: Implement cluster topology update logic
    }
    
}
