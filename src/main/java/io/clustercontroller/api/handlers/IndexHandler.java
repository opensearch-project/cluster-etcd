package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.requests.IndexRequest;
import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.api.models.responses.IndexResponse;
import io.clustercontroller.indices.IndexManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST API handler for index lifecycle operations.
 * 
 * Provides endpoints for creating, reading, updating, and deleting indices,
 * as well as managing index settings and mappings. All endpoints follow
 * standard REST conventions and return JSON responses.
 * 
 * Supported operations:
 * - PUT /{index} - Create a new index
 * - GET /{index} - Retrieve index information
 * - DELETE /{index} - Delete an index
 * - GET /{index}/_settings - Get index settings
 * - PUT /{index}/_settings - Update index settings
 * - GET /{index}/_mapping - Get index mappings
 * - PUT /{index}/_mapping - Update index mappings
 */
@Slf4j
@RestController
@RequestMapping("/")
public class IndexHandler {
    
    private final IndexManager indexManager;
    private final ObjectMapper objectMapper;
    
    public IndexHandler(IndexManager indexManager, ObjectMapper objectMapper) {
        this.indexManager = indexManager;
        this.objectMapper = objectMapper;
    }
    
    @PutMapping("/{index}")
    public ResponseEntity<Object> createIndex(
            @PathVariable String index,
            @RequestBody(required = false) IndexRequest request) {
        try {
            log.info("Creating index: {}", index);
            if (request == null) {
                request = IndexRequest.builder().build();
            }
            String jsonConfig = objectMapper.writeValueAsString(request);
            indexManager.createIndex(jsonConfig);
            return ResponseEntity.status(201).body(IndexResponse.createSuccess(index));
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Index creation"));
        } catch (Exception e) {
            log.error("Error creating index {}: {}", index, e.getMessage());
            return ResponseEntity.status(500).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
    
    @GetMapping("/{index}")
    public ResponseEntity<Object> getIndex(@PathVariable String index) {
        try {
            log.info("Getting index: {}", index);
            String indexInfo = indexManager.getIndex(index);
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Get index"));
        } catch (Exception e) {
            log.error("Error getting index {}: {}", index, e.getMessage());
            return ResponseEntity.status(500).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
    
    @DeleteMapping("/{index}")
    public ResponseEntity<Object> deleteIndex(@PathVariable String index) {
        try {
            log.info("Deleting index: {}", index);
            indexManager.deleteIndex(index);
            return ResponseEntity.ok(IndexResponse.deleteSuccess(index));
        } catch (UnsupportedOperationException e) {
            return ResponseEntity.status(501).body(ErrorResponse.notImplemented("Index deletion"));
        } catch (Exception e) {
            log.error("Error deleting index {}: {}", index, e.getMessage());
            return ResponseEntity.status(500).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
}