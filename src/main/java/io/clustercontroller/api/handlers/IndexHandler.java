package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.requests.IndexRequest;
import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.api.models.responses.IndexResponse;
import io.clustercontroller.indices.IndexManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST API handler for index lifecycle operations with multi-cluster support.
 * 
 * Provides endpoints for creating, reading, updating, and deleting indices,
 * as well as managing index settings and mappings. All endpoints follow
 * standard REST conventions and return JSON responses.
 * 
 * Multi-cluster supported operations:
 * - PUT /{clusterId}/{index} - Create a new index in specified cluster
 * - GET /{clusterId}/{index} - Retrieve index information from cluster
 * - DELETE /{clusterId}/{index} - Delete an index from cluster
 * - GET /{clusterId}/{index}/_settings - Get index settings from cluster
 * - PUT /{clusterId}/{index}/_settings - Update index settings in cluster
 * - GET /{clusterId}/{index}/_mapping - Get index mappings from cluster
 * - PUT /{clusterId}/{index}/_mapping - Update index mappings in cluster
 */
@Slf4j
@RestController
@RequestMapping("/{clusterId}")
public class IndexHandler {
    
    private final IndexManager indexManager;
    private final ObjectMapper objectMapper;
    
    public IndexHandler(IndexManager indexManager, ObjectMapper objectMapper) {
        this.indexManager = indexManager;
        this.objectMapper = objectMapper;
    }
    
    /**
     * Create a new index in the specified cluster.
     * PUT /{clusterId}/{index}
     */
    @PutMapping("/{index}")
    public ResponseEntity<Object> createIndex(
            @PathVariable String clusterId,
            @PathVariable String index,
            @RequestBody(required = false) IndexRequest request) {
        try {
            log.info("Creating index '{}' in cluster '{}'", index, clusterId);
            String indexConfig = (request != null) ? objectMapper.writeValueAsString(request) : "{}";
            indexManager.createIndex(clusterId, index, indexConfig);
            return ResponseEntity.status(HttpStatus.CREATED).body(IndexResponse.builder()
                .acknowledged(true)
                .shardsAcknowledged(true)
                .index(index)
                .build());
        } catch (UnsupportedOperationException e) {
            log.error("Error creating index '{}' in cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Index creation"));
        } catch (Exception e) {
            log.error("Error creating index '{}' in cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
    
    /**
     * Get index information from the specified cluster.
     * GET /{clusterId}/{index}
     */
    @GetMapping("/{index}")
    public ResponseEntity<Object> getIndex(
            @PathVariable String clusterId,
            @PathVariable String index) {
        try {
            log.info("Getting index '{}' from cluster '{}'", index, clusterId);
            String indexInfo = indexManager.getIndex(clusterId, index);
            return ResponseEntity.ok(indexInfo);
        } catch (IllegalArgumentException e) {
            log.error("Invalid request for index '{}' from cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ErrorResponse.badRequest(e.getMessage()));
        } catch (Exception e) {
            log.error("Error getting index '{}' from cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
    
    /**
     * Delete an index from the specified cluster.
     * DELETE /{clusterId}/{index}
     */
    @DeleteMapping("/{index}")
    public ResponseEntity<Object> deleteIndex(
            @PathVariable String clusterId,
            @PathVariable String index) {
        try {
            log.info("Deleting index '{}' from cluster '{}'", index, clusterId);
            indexManager.deleteIndex(clusterId, index);
            return ResponseEntity.ok(IndexResponse.builder()
                .acknowledged(true)
                .shardsAcknowledged(true)
                .index(index)
                .build());
        } catch (UnsupportedOperationException e) {
            log.error("Error deleting index '{}' from cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Index deletion"));
        } catch (Exception e) {
            log.error("Error deleting index '{}' from cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get index settings from the specified cluster.
     * GET /{clusterId}/{index}/_settings
     */
    @GetMapping("/{index}/_settings")
    public ResponseEntity<Object> getIndexSettings(
            @PathVariable String clusterId,
            @PathVariable String index) {
        try {
            log.info("Getting settings for index '{}' from cluster '{}'", index, clusterId);
            String settings = indexManager.getSettings(clusterId, index);
            return ResponseEntity.ok(settings);
        } catch (UnsupportedOperationException e) {
            log.error("Error getting settings for index '{}' from cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Get index settings"));
        } catch (Exception e) {
            log.error("Error getting settings for index '{}' from cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Update index settings in the specified cluster.
     * PUT /{clusterId}/{index}/_settings
     */
    @PutMapping("/{index}/_settings")
    public ResponseEntity<Object> updateIndexSettings(
            @PathVariable String clusterId,
            @PathVariable String index,
            @RequestBody String settingsJson) {
        try {
            log.info("Updating settings for index '{}' in cluster '{}'", index, clusterId);
            indexManager.updateSettings(clusterId, index, settingsJson);
            return ResponseEntity.ok(IndexResponse.builder()
                .acknowledged(true)
                .shardsAcknowledged(true)
                .index(index)
                .build());
        } catch (UnsupportedOperationException e) {
            log.error("Error updating settings for index '{}' in cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Update index settings"));
        } catch (Exception e) {
            log.error("Error updating settings for index '{}' in cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get index mappings from the specified cluster.
     * GET /{clusterId}/{index}/_mapping
     */
    @GetMapping("/{index}/_mapping")
    public ResponseEntity<Object> getIndexMapping(
            @PathVariable String clusterId,
            @PathVariable String index) {
        try {
            log.info("Getting mapping for index '{}' from cluster '{}'", index, clusterId);
            String mapping = indexManager.getMapping(clusterId, index);
            return ResponseEntity.ok(mapping);
        } catch (UnsupportedOperationException e) {
            log.error("Error getting mapping for index '{}' from cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Get index mapping"));
        } catch (Exception e) {
            log.error("Error getting mapping for index '{}' from cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Update index mappings in the specified cluster.
     * PUT /{clusterId}/{index}/_mapping
     */
    @PutMapping("/{index}/_mapping")
    public ResponseEntity<Object> updateIndexMapping(
            @PathVariable String clusterId,
            @PathVariable String index,
            @RequestBody String mappingsJson) {
        try {
            log.info("Updating mapping for index '{}' in cluster '{}'", index, clusterId);
            indexManager.updateMapping(clusterId, index, mappingsJson);
            return ResponseEntity.ok(IndexResponse.builder()
                .acknowledged(true)
                .shardsAcknowledged(true)
                .index(index)
                .build());
        } catch (UnsupportedOperationException e) {
            log.error("Error updating mapping for index '{}' in cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Update index mapping"));
        } catch (Exception e) {
            log.error("Error updating mapping for index '{}' in cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
}