package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.requests.TemplateRequest;
import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.api.models.responses.TemplateResponse;
import io.clustercontroller.templates.TemplateManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST API handler for index template operations with multi-cluster support.
 *
 * Provides endpoints for creating, reading, updating, and deleting index templates.
 * Templates define default settings, mappings, and aliases that are automatically
 * applied to new indices matching specified patterns.
 *
 * Multi-cluster supported operations:
 * - PUT /{clusterId}/_index_template/{name} - Create or update an index template
 * - GET /{clusterId}/_index_template/{name} - Get a specific index template
 * - DELETE /{clusterId}/_index_template/{name} - Delete an index template
 * - GET /{clusterId}/_index_template - Get all index templates
 */
@Slf4j
@RestController
@RequestMapping("/{clusterId}/_index_template")
public class TemplateHandler {

    private final TemplateManager templateManager;
    private final ObjectMapper objectMapper;

    public TemplateHandler(TemplateManager templateManager, ObjectMapper objectMapper) {
        this.templateManager = templateManager;
        this.objectMapper = objectMapper;
    }

    /**
     * Create or update an index template in the specified cluster.
     * PUT /{clusterId}/_index_template/{name}
     */
    @PutMapping("/{name}")
    public ResponseEntity<Object> createTemplate(
            @PathVariable String clusterId,
            @PathVariable String name, 
            @RequestBody TemplateRequest request) {
        try {
            log.info("Creating index template '{}' in cluster '{}'", name, clusterId);
            String templateConfig = objectMapper.writeValueAsString(request);
            templateManager.putTemplate(clusterId, name, templateConfig);
            return ResponseEntity.ok(TemplateResponse.builder()
                .acknowledged(true)
                .template(name)
                .build());
        } catch (IllegalArgumentException e) {
            log.error("Invalid request for template '{}' in cluster '{}': {}", name, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ErrorResponse.badRequest(e.getMessage()));
        } catch (Exception e) {
            log.error("Error creating template '{}' in cluster '{}': {}", name, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get a specific index template from the specified cluster.
     * GET /{clusterId}/_index_template/{name}
     */
    @GetMapping("/{name}")
    public ResponseEntity<Object> getTemplate(
            @PathVariable String clusterId,
            @PathVariable String name) {
        try {
            log.info("Getting index template '{}' from cluster '{}'", name, clusterId);
            String templateInfo = templateManager.getTemplate(clusterId, name);
            return ResponseEntity.ok(templateInfo);
        } catch (IllegalArgumentException e) {
            log.error("Template '{}' not found in cluster '{}': {}", name, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ErrorResponse.notFound(e.getMessage()));
        } catch (Exception e) {
            log.error("Error getting template '{}' from cluster '{}': {}", name, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Delete an index template from the specified cluster.
     * DELETE /{clusterId}/_index_template/{name}
     */
    @DeleteMapping("/{name}")
    public ResponseEntity<Object> deleteTemplate(
            @PathVariable String clusterId,
            @PathVariable String name) {
        try {
            log.info("Deleting index template '{}' from cluster '{}'", name, clusterId);
            templateManager.deleteTemplate(clusterId, name);
            return ResponseEntity.ok(TemplateResponse.builder()
                .acknowledged(true)
                .template(name)
                .build());
        } catch (IllegalArgumentException e) {
            log.error("Template '{}' not found in cluster '{}': {}", name, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ErrorResponse.notFound(e.getMessage()));
        } catch (Exception e) {
            log.error("Error deleting template '{}' from cluster '{}': {}", name, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get all index templates from the specified cluster.
     * GET /{clusterId}/_index_template
     */
    @GetMapping
    public ResponseEntity<Object> getAllTemplates(@PathVariable String clusterId) {
        try {
            log.info("Getting all index templates from cluster '{}'", clusterId);
            String templatesInfo = templateManager.getAllTemplates(clusterId);
            return ResponseEntity.ok(templatesInfo);
        } catch (IllegalArgumentException e) {
            log.error("Invalid request for cluster '{}': {}", clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ErrorResponse.badRequest(e.getMessage()));
        } catch (Exception e) {
            log.error("Error getting all templates from cluster '{}': {}", clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
}