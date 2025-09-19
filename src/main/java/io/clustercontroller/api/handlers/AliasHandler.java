package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.requests.AliasRequest;
import io.clustercontroller.api.models.responses.AliasResponse;
import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.indices.AliasManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST API handler for index alias operations.
 *
 * Provides endpoints for creating, reading, updating, and deleting index aliases.
 * Aliases allow referring to one or more indices by alternative names, enabling
 * zero-downtime reindexing and simplified index management.
 *
 * Supported operations:
 * - PUT /{index}/_alias/{alias} - Create or update an alias
 * - DELETE /{index}/_alias/{alias} - Remove an alias from an index
 * - GET /{alias} - Get information about an alias
 * - GET /_alias/{alias} - Get alias information (alternative endpoint)
 * - GET /{index}/_alias - Get all aliases for an index
 */
@Slf4j
@RestController
@RequestMapping("/")
public class AliasHandler {

    private final AliasManager aliasManager;
    private final ObjectMapper objectMapper;

    public AliasHandler(AliasManager aliasManager, ObjectMapper objectMapper) {
        this.aliasManager = aliasManager;
        this.objectMapper = objectMapper;
    }

    /**
     * Create or update an alias for an index.
     * PUT /{index}/_alias/{alias}
     */
    @PutMapping("/{index}/_alias/{alias}")
    public ResponseEntity<Object> createAlias(@PathVariable String index, @PathVariable String alias, 
                                            @RequestBody(required = false) AliasRequest request) {
        try {
            log.info("Creating alias '{}' for index '{}'", alias, index);
            String aliasConfig = (request != null) ? objectMapper.writeValueAsString(request) : "{}";
            aliasManager.createAlias(alias, index, aliasConfig);
            return ResponseEntity.ok(AliasResponse.builder()
                .acknowledged(true)
                .alias(alias)
                .index(index)
                .build());
        } catch (UnsupportedOperationException e) {
            log.error("Error creating alias '{}' for index '{}': {}", alias, index, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Alias creation"));
        } catch (Exception e) {
            log.error("Error creating alias '{}' for index '{}': {}", alias, index, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Remove an alias from an index.
     * DELETE /{index}/_alias/{alias}
     */
    @DeleteMapping("/{index}/_alias/{alias}")
    public ResponseEntity<Object> deleteAlias(@PathVariable String index, @PathVariable String alias) {
        try {
            log.info("Deleting alias '{}' from index '{}'", alias, index);
            aliasManager.deleteAlias(alias, index);
            return ResponseEntity.ok(AliasResponse.builder()
                .acknowledged(true)
                .alias(alias)
                .index(index)
                .build());
        } catch (UnsupportedOperationException e) {
            log.error("Error deleting alias '{}' from index '{}': {}", alias, index, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Alias deletion"));
        } catch (Exception e) {
            log.error("Error deleting alias '{}' from index '{}': {}", alias, index, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get alias information.
     * GET /_alias/{alias}
     */
    @GetMapping("/_alias/{alias}")
    public ResponseEntity<Object> getAlias(@PathVariable String alias) {
        try {
            log.info("Getting alias information for '{}'", alias);
            String aliasInfo = aliasManager.getAlias(alias);
            return ResponseEntity.ok(aliasInfo);
        } catch (UnsupportedOperationException e) {
            log.error("Error getting alias '{}': {}", alias, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Get alias"));
        } catch (Exception e) {
            log.error("Error getting alias '{}': {}", alias, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get all aliases for an index.
     * GET /{index}/_alias
     */
    @GetMapping("/{index}/_alias")
    public ResponseEntity<Object> getIndexAliases(@PathVariable String index) {
        try {
            log.info("Getting all aliases for index '{}'", index);
            // This would typically return all aliases for the specific index
            String aliasInfo = aliasManager.getAlias(index); // Simplified for now
            return ResponseEntity.ok(aliasInfo);
        } catch (UnsupportedOperationException e) {
            log.error("Error getting aliases for index '{}': {}", index, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Get index aliases"));
        } catch (Exception e) {
            log.error("Error getting aliases for index '{}': {}", index, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
}
