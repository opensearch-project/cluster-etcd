package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.health.ClusterHealthManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST API handler for cluster health and statistics operations.
 * 
 * Provides endpoints for monitoring cluster health, node status, and
 * performance statistics. Health information can be retrieved at different
 * levels of granularity (cluster, indices, or shards).
 * 
 * Supported operations:
 * - GET /_cluster/health - Overall cluster health status
 * - GET /_cluster/health/{index} - Health status for specific index
 * - GET /_cluster/stats - Cluster performance statistics
 * 
 * Health status values: GREEN (healthy), YELLOW (degraded), RED (critical)
 */
@Slf4j
@RestController
@RequestMapping("/_cluster")
public class HealthHandler {
    
    private final ClusterHealthManager healthManager;
    private final ObjectMapper objectMapper;
    
    public HealthHandler(ClusterHealthManager healthManager, ObjectMapper objectMapper) {
        this.healthManager = healthManager;
        this.objectMapper = objectMapper;
    }
    
    @GetMapping("/health")
    public ResponseEntity<Object> getClusterHealth(
            @RequestParam(value = "level", defaultValue = "cluster") String level) {
        try {
            log.info("Getting cluster health with level: {}", level);
            String healthJson = healthManager.getClusterHealth(level);
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Cluster health"));
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Cluster health"));
        } catch (Exception e) {
            log.error("Error getting cluster health: {}", e.getMessage());
            return ResponseEntity.status(500).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
    
    @GetMapping("/health/{index}")
    public ResponseEntity<Object> getIndexHealth(@PathVariable String index) {
        try {
            log.info("Getting health for index: {}", index);
            String healthJson = healthManager.getIndexHealth(index, "indices");
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Index health"));
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Index health"));
        } catch (Exception e) {
            log.error("Error getting health for index {}: {}", index, e.getMessage());
            return ResponseEntity.status(500).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
    
    @GetMapping("/stats")
    public ResponseEntity<Object> getClusterStats() {
        try {
            log.info("Getting cluster statistics");
            String statsJson = healthManager.getClusterStats();
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Cluster stats"));
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Cluster stats"));
        } catch (Exception e) {
            log.error("Error getting cluster stats: {}", e.getMessage());
            return ResponseEntity.status(500).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
}
