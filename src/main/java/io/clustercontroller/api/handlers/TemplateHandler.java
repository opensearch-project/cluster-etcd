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
 * REST API handler for index template operations.
 *
 * Provides endpoints for creating, reading, updating, and deleting index templates.
 * Templates define default settings, mappings, and aliases that are automatically
 * applied to new indices matching specified patterns.
 *
 * Supported operations:
 * - PUT /_index_template/{name} - Create or update an index template
 * - GET /_index_template/{name} - Get a specific index template
 * - DELETE /_index_template/{name} - Delete an index template
 * - GET /_index_template - Get all index templates
 */
@Slf4j
@RestController
@RequestMapping("/_index_template")
public class TemplateHandler {

    private final TemplateManager templateManager;
    private final ObjectMapper objectMapper;

    public TemplateHandler(TemplateManager templateManager, ObjectMapper objectMapper) {
        this.templateManager = templateManager;
        this.objectMapper = objectMapper;
    }

    /**
     * Create or update an index template.
     * PUT /_index_template/{name}
     */
    @PutMapping("/{name}")
    public ResponseEntity<Object> createTemplate(@PathVariable String name, @RequestBody TemplateRequest request) {
        try {
            log.info("Creating index template: {}", name);
            String templateConfig = objectMapper.writeValueAsString(request);
            templateManager.putTemplate(name, templateConfig);
            return ResponseEntity.ok(TemplateResponse.builder()
                .acknowledged(true)
                .template(name)
                .build());
        } catch (UnsupportedOperationException e) {
            log.error("Error creating template '{}': {}", name, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Template creation"));
        } catch (Exception e) {
            log.error("Error creating template '{}': {}", name, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get a specific index template.
     * GET /_index_template/{name}
     */
    @GetMapping("/{name}")
    public ResponseEntity<Object> getTemplate(@PathVariable String name) {
        try {
            log.info("Getting index template: {}", name);
            String templateInfo = templateManager.getTemplate(name);
            return ResponseEntity.ok(templateInfo);
        } catch (UnsupportedOperationException e) {
            log.error("Error getting template '{}': {}", name, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Get template"));
        } catch (Exception e) {
            log.error("Error getting template '{}': {}", name, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Delete an index template.
     * DELETE /_index_template/{name}
     */
    @DeleteMapping("/{name}")
    public ResponseEntity<Object> deleteTemplate(@PathVariable String name) {
        try {
            log.info("Deleting index template: {}", name);
            templateManager.deleteTemplate(name);
            return ResponseEntity.ok(TemplateResponse.builder()
                .acknowledged(true)
                .template(name)
                .build());
        } catch (UnsupportedOperationException e) {
            log.error("Error deleting template '{}': {}", name, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Template deletion"));
        } catch (Exception e) {
            log.error("Error deleting template '{}': {}", name, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Get all index templates.
     * GET /_index_template
     */
    @GetMapping
    public ResponseEntity<Object> getAllTemplates() {
        try {
            log.info("Getting all index templates");
            // This would typically return all templates - simplified for now
            String templatesInfo = templateManager.getTemplate("*"); // Wildcard pattern
            return ResponseEntity.ok(templatesInfo);
        } catch (UnsupportedOperationException e) {
            log.error("Error getting all templates: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(ErrorResponse.notImplemented("Get all templates"));
        } catch (Exception e) {
            log.error("Error getting all templates: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }
}
