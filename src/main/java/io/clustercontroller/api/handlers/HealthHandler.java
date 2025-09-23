package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.health.ClusterHealthManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST API handler for cluster health and statistics operations with multi-cluster support.
 *
 * Provides endpoints for monitoring cluster health, node status, and
 * performance statistics. Health information can be retrieved at different
 * levels of granularity (cluster, indices, or shards).
 *
 * Multi-cluster supported operations:
 * - GET /{clusterId}/_cluster/health - Overall cluster health status
 * - GET /{clusterId}/_cluster/health/{index} - Health status for specific index
 * - GET /{clusterId}/_cluster/stats - Cluster performance statistics
 *
 * Health status values: GREEN (healthy), YELLOW (degraded), RED (critical)
 */
@Slf4j
@RestController
@RequestMapping("/{clusterId}/_cluster")
public class HealthHandler {

    private final ClusterHealthManager healthManager;
    private final ObjectMapper objectMapper;

    public HealthHandler(ClusterHealthManager healthManager, ObjectMapper objectMapper) {
        this.healthManager = healthManager;
        this.objectMapper = objectMapper;
    }

    /**
     * Get overall cluster health status for the specified cluster.
     * GET /{clusterId}/_cluster/health
     * GET /{clusterId}/_cluster/health?level=cluster|indices|shards
     */
    @GetMapping("/health")
    public ResponseEntity<Object> getClusterHealth(
            @PathVariable String clusterId,
            @RequestParam(value = "level", defaultValue = "cluster") String level) {
        try {
            log.info("Getting cluster health for cluster '{}' with level: {}", clusterId, level);
            String healthJson = healthManager.getClusterHealth(clusterId, level);
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Cluster health"));
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Cluster health"));
        } catch (Exception e) {
            log.error("Error getting cluster health for cluster '{}': {}", clusterId, e.getMessage());
            return ResponseEntity.status(500).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get health status for a specific index in the specified cluster.
     * GET /{clusterId}/_cluster/health/{index}
     */
    @GetMapping("/health/{index}")
    public ResponseEntity<Object> getIndexHealth(
            @PathVariable String clusterId,
            @PathVariable String index) {
        try {
            log.info("Getting health for index '{}' in cluster '{}'", index, clusterId);
            String healthJson = healthManager.getIndexHealth(clusterId, index, "indices");
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Index health"));
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Index health"));
        } catch (Exception e) {
            log.error("Error getting health for index '{}' in cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(500).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get cluster performance statistics for the specified cluster.
     * GET /{clusterId}/_cluster/stats
     */
    @GetMapping("/stats")
    public ResponseEntity<Object> getClusterStats(@PathVariable String clusterId) {
        try {
            log.info("Getting cluster statistics for cluster '{}'", clusterId);
            String statsJson = healthManager.getClusterStats(clusterId);
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Cluster stats"));
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Cluster stats"));
        } catch (Exception e) {
            log.error("Error getting cluster stats for cluster '{}': {}", clusterId, e.getMessage());
            return ResponseEntity.status(500).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
}