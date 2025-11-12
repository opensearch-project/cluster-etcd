package io.clustercontroller.proxy;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Selects a healthy coordinator node
 * Queries etcd for coordinators from the /coordinators/ path, filters for healthy ones, and picks one in rotation.
 */
@Slf4j
public class CoordinatorSelector {

    private final MetadataStore metadataStore;
    private final AtomicInteger roundRobinCounter;

    public CoordinatorSelector(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.roundRobinCounter = new AtomicInteger(0);
    }

    /**
     * Select a healthy coordinator for the given cluster using round-robin.
     *
     * @param clusterId Cluster ID to find coordinators for
     * @return Selected SearchUnit (coordinator)
     * @throws Exception if no healthy coordinators are found
     */
    public SearchUnit selectCoordinator(String clusterId) throws Exception {
        log.debug("Selecting coordinator for cluster: {}", clusterId);

        // Step 1: Get all coordinators from MetadataStore
        List<SearchUnit> coordinators = metadataStore.getAllCoordinators(clusterId);
        
        if (coordinators.isEmpty()) {
            throw new Exception("No coordinator nodes found for cluster: " + clusterId);
        }
        
        log.debug("Found {} coordinators in cluster '{}'", coordinators.size(), clusterId);

        // Step 2: Filter for healthy coordinators
        List<SearchUnit> healthyCoordinators = new ArrayList<>();
        for (SearchUnit coordinator : coordinators) {
            if (isHealthy(clusterId, coordinator)) {
                healthyCoordinators.add(coordinator);
            }
        }

        if (healthyCoordinators.isEmpty()) {
            throw new Exception("No healthy coordinator nodes found for cluster: " + clusterId);
        }

        log.debug("Found {} healthy coordinators in cluster '{}'", healthyCoordinators.size(), clusterId);

        int size = healthyCoordinators.size();
        int index = Math.floorMod(roundRobinCounter.getAndIncrement(), size);
        SearchUnit selected = healthyCoordinators.get(index);

        log.info("Selected coordinator '{}' for cluster '{}' (round-robin index: {})", 
                selected.getName(), clusterId, index);

        return selected;
    }

    /**
     * Check if a coordinator is healthy.
     * 
     * Basic health checks for coordinators from /coordinators/ path:
     * 1. Has valid host
     * 2. Has valid port
     *
     * @param clusterId Cluster ID
     * @param coordinator SearchUnit to check
     * @return true if coordinator is healthy, false otherwise
     */
    private boolean isHealthy(String clusterId, SearchUnit coordinator) {
        // Basic validation - ensure coordinator has required fields
        if (coordinator.getHost() == null || coordinator.getHost().isEmpty()) {
            log.debug("Coordinator '{}' has no host", coordinator.getName());
            return false;
        }
        
        if (coordinator.getPortHttp() <= 0) {
            log.debug("Coordinator '{}' has invalid port", coordinator.getName());
            return false;
        }
        
        log.debug("Coordinator '{}' is healthy at {}:{}", 
                coordinator.getName(), coordinator.getHost(), coordinator.getPortHttp());
        return true;
    }

    /**
     * Build the full HTTP URL for a coordinator.
     *
     * @param coordinator SearchUnit
     * @return Full URL (e.g., "http://10.0.0.5:9200")
     */
    public String buildCoordinatorUrl(SearchUnit coordinator) {
        return "http://" + coordinator.getHost() + ":" + coordinator.getPortHttp();
    }

