package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.requests.AliasRequest;
import io.clustercontroller.api.models.requests.AliasActionsRequest;
import io.clustercontroller.api.models.requests.AliasAction;
import io.clustercontroller.api.models.responses.AliasResponse;
import io.clustercontroller.api.models.responses.AliasActionsResponse;
import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.indices.AliasManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * REST API handler for index alias operations with multi-cluster support.
 *
 * Provides endpoints for creating, reading, updating, and deleting index aliases.
 * Aliases allow referring to one or more indices by alternative names, enabling
 * zero-downtime reindexing and simplified index management.
 *
 * Multi-cluster supported operations:
 * - PUT /{clusterId}/{index}/_alias/{alias} - Create or update an alias
 * - DELETE /{clusterId}/{index}/_alias/{alias} - Remove an alias from an index
 * - GET /{clusterId}/_alias/{alias} - Get information about an alias
 * - GET /{clusterId}/{index}/_alias - Get all aliases for an index
 * - POST /{clusterId}/_aliases - Bulk add/remove operations for aliases
 */
@Slf4j
@RestController
@RequestMapping("/{clusterId}")
public class AliasHandler {

    private final AliasManager aliasManager;
    private final ObjectMapper objectMapper;

    public AliasHandler(AliasManager aliasManager, ObjectMapper objectMapper) {
        this.aliasManager = aliasManager;
        this.objectMapper = objectMapper;
    }

    /**
     * Create or update an alias for an index in the specified cluster.
     * PUT /{clusterId}/{index}/_alias/{alias}
     */
    @PutMapping("/{index}/_alias/{alias}")
    public ResponseEntity<Object> createAlias(
            @PathVariable String clusterId,
            @PathVariable String index, 
            @PathVariable String alias, 
            @RequestBody(required = false) AliasRequest request) {
        try {
            log.info("Creating alias '{}' for index '{}' in cluster '{}'", alias, index, clusterId);
            String aliasConfig = (request != null) ? objectMapper.writeValueAsString(request) : "{}";
            aliasManager.createAlias(clusterId, alias, index, aliasConfig);
            return ResponseEntity.ok(AliasResponse.builder()
                .acknowledged(true)
                .alias(alias)
                .index(index)
                .build());
        } catch (UnsupportedOperationException e) {
            log.error("Error creating alias '{}' for index '{}' in cluster '{}': {}", alias, index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Alias creation"));
        } catch (Exception e) {
            log.error("Error creating alias '{}' for index '{}' in cluster '{}': {}", alias, index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Remove an alias from an index in the specified cluster.
     * DELETE /{clusterId}/{index}/_alias/{alias}
     */
    @DeleteMapping("/{index}/_alias/{alias}")
    public ResponseEntity<Object> deleteAlias(
            @PathVariable String clusterId,
            @PathVariable String index, 
            @PathVariable String alias) {
        try {
            log.info("Deleting alias '{}' from index '{}' in cluster '{}'", alias, index, clusterId);
            aliasManager.deleteAlias(clusterId, alias, index);
            return ResponseEntity.ok(AliasResponse.builder()
                .acknowledged(true)
                .alias(alias)
                .index(index)
                .build());
        } catch (UnsupportedOperationException e) {
            log.error("Error deleting alias '{}' from index '{}' in cluster '{}': {}", alias, index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Alias deletion"));
        } catch (Exception e) {
            log.error("Error deleting alias '{}' from index '{}' in cluster '{}': {}", alias, index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get alias information from the specified cluster.
     * GET /{clusterId}/_alias/{alias}
     */
    @GetMapping("/_alias/{alias}")
    public ResponseEntity<Object> getAlias(
            @PathVariable String clusterId,
            @PathVariable String alias) {
        try {
            log.info("Getting alias information for '{}' from cluster '{}'", alias, clusterId);
            String aliasInfo = aliasManager.getAlias(clusterId, alias);
            return ResponseEntity.ok(aliasInfo);
        } catch (UnsupportedOperationException e) {
            log.error("Error getting alias '{}' from cluster '{}': {}", alias, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Get alias"));
        } catch (Exception e) {
            log.error("Error getting alias '{}' from cluster '{}': {}", alias, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get all aliases for an index in the specified cluster.
     * GET /{clusterId}/{index}/_alias
     */
    @GetMapping("/{index}/_alias")
    public ResponseEntity<Object> getIndexAliases(
            @PathVariable String clusterId,
            @PathVariable String index) {
        try {
            log.info("Getting all aliases for index '{}' from cluster '{}'", index, clusterId);
            String aliasInfo = aliasManager.getAlias(clusterId, index);
            return ResponseEntity.ok(aliasInfo);
        } catch (UnsupportedOperationException e) {
            log.error("Error getting aliases for index '{}' from cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Get index aliases"));
        } catch (Exception e) {
            log.error("Error getting aliases for index '{}' from cluster '{}': {}", index, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
    
    /**
     * Update aliases using bulk operations (add/remove multiple aliases) in the specified cluster.
     * POST /{clusterId}/_aliases
     * 
     * Request body example:
     * {
     *   "actions": [
     *     {"add": {"index": "logs_2024", "alias": "current_logs"}},
     *     {"remove": {"index": "logs_2023", "alias": "current_logs"}}
     *   ]
     * }
     */
    @PostMapping("/_aliases")
    public ResponseEntity<Object> updateAlias(
            @PathVariable String clusterId,
            @RequestBody AliasActionsRequest request) {
        try {
            if (request.getActions() == null || request.getActions().isEmpty()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(ErrorResponse.badRequest("Actions array is required and cannot be empty"));
            }
            
            log.info("Executing bulk alias operations in cluster '{}', {} actions", clusterId, request.getActions().size());
            
            // Convert AliasAction objects to the format expected by AliasManager
            List<Map<String, Map<String, String>>> actions = request.getActions().stream()
                .map(action -> {
                    Map<String, Map<String, String>> actionMap = new HashMap<>();
                    
                    if (action.getAdd() != null) {
                        Map<String, String> addDetails = new HashMap<>();
                        addDetails.put("index", action.getAdd().getIndex());
                        addDetails.put("alias", action.getAdd().getAlias());
                        actionMap.put("add", addDetails);
                    } else if (action.getRemove() != null) {
                        Map<String, String> removeDetails = new HashMap<>();
                        removeDetails.put("index", action.getRemove().getIndex());
                        removeDetails.put("alias", action.getRemove().getAlias());
                        actionMap.put("remove", removeDetails);
                    }
                    
                    return actionMap;
                })
                .collect(Collectors.toList());
            
            Map<String, Object> result = aliasManager.applyAliasActions(clusterId, actions);
            
            // Build response
            AliasActionsResponse response = AliasActionsResponse.builder()
                .acknowledged((Boolean) result.get("acknowledged"))
                .actionsCompleted((Integer) result.get("actionsCompleted"))
                .actionsFailed((Integer) result.get("actionsFailed"))
                .build();
            
            // If there were failures, include error details in the response
            if (result.containsKey("errors")) {
                Map<String, Object> responseWithErrors = new HashMap<>();
                responseWithErrors.put("acknowledged", response.isAcknowledged());
                responseWithErrors.put("actions_completed", response.getActionsCompleted());
                responseWithErrors.put("actions_failed", response.getActionsFailed());
                responseWithErrors.put("errors", result.get("errors"));
                return ResponseEntity.ok(responseWithErrors);
            }
            
            return ResponseEntity.ok(response);
            
        } catch (IllegalArgumentException e) {
            log.error("Invalid request for bulk alias operations in cluster '{}': {}", clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ErrorResponse.badRequest(e.getMessage()));
        } catch (Exception e) {
            log.error("Error executing bulk alias operations in cluster '{}': {}", clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
}