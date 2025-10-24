package io.clustercontroller.proxy;

import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Selects a healthy coordinator node using round-robin load balancing.
 * Queries etcd for coordinators, filters for healthy ones, and picks one in rotation.
 */
@Slf4j
public class CoordinatorSelector {

    private static final String COORDINATOR_ROLE = "COORDINATOR";
    private static final long HEARTBEAT_TIMEOUT_MS = 30000; // 30 seconds

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

        // Step 1: Get all search units for cluster
        List<SearchUnit> allUnits = metadataStore.getAllSearchUnits(clusterId);
        log.debug("Found {} total search units in cluster '{}'", allUnits.size(), clusterId);

        // Step 2: Filter for coordinators
        List<SearchUnit> coordinators = allUnits.stream()
                .filter(unit -> COORDINATOR_ROLE.equals(unit.getRole()))
                .collect(Collectors.toList());
        
        if (coordinators.isEmpty()) {
            throw new Exception("No coordinator nodes found for cluster: " + clusterId);
        }
        
        log.debug("Found {} coordinators in cluster '{}'", coordinators.size(), clusterId);

        // Step 3: Filter for healthy coordinators
        List<SearchUnit> healthyCoordinators = coordinators.stream()
                .filter(this::isHealthy)
                .collect(Collectors.toList());

        if (healthyCoordinators.isEmpty()) {
            throw new Exception("No healthy coordinator nodes found for cluster: " + clusterId);
        }

        log.debug("Found {} healthy coordinators in cluster '{}'", healthyCoordinators.size(), clusterId);

        // Step 4: Select using round-robin
        int index = roundRobinCounter.getAndIncrement() % healthyCoordinators.size();
        SearchUnit selected = healthyCoordinators.get(index);

        log.info("Selected coordinator '{}' for cluster '{}' (round-robin index: {})", 
                selected.getName(), clusterId, index);

        return selected;
    }

    /**
     * Check if a coordinator is healthy based on heartbeat age and resource usage.
     *
     * @param coordinator SearchUnit to check
     * @return true if coordinator is healthy, false otherwise
     */
    private boolean isHealthy(SearchUnit coordinator) {
        try {
            // Get actual state from etcd
            SearchUnitActualState actualState = metadataStore.getSearchUnitActualState(
                    coordinator.getClusterName(), 
                    coordinator.getName()
            );

            if (actualState == null) {
                log.warn("No actual state found for coordinator '{}'", coordinator.getName());
                return false;
            }

            // Check heartbeat age
            long heartbeatAge = System.currentTimeMillis() - actualState.getTimestamp();
            if (heartbeatAge > HEARTBEAT_TIMEOUT_MS) {
                log.debug("Coordinator '{}' has stale heartbeat: {} ms old", 
                        coordinator.getName(), heartbeatAge);
                return false;
            }

            // Check resource usage (using built-in isHealthy method)
            if (!actualState.isHealthy()) {
                log.debug("Coordinator '{}' is unhealthy (resource constraints)", 
                        coordinator.getName());
                return false;
            }

            log.debug("Coordinator '{}' is healthy (heartbeat: {} ms old)", 
                    coordinator.getName(), heartbeatAge);
            return true;

        } catch (Exception e) {
            log.error("Error checking health for coordinator '{}': {}", 
                    coordinator.getName(), e.getMessage());
            return false;
        }
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
}

