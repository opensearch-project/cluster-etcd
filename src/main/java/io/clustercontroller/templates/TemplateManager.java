package io.clustercontroller.templates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.clustercontroller.api.models.requests.TemplateRequest;
import io.clustercontroller.models.Template;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Manages index template operations with multi-cluster support.
 * Provides methods for creating, deleting, and retrieving index templates.
 * Index templates allow defining default settings and mappings
 * for new indices that match a specified pattern.
 */
@Slf4j
public class TemplateManager {

    private final MetadataStore metadataStore;
    private final ObjectMapper objectMapper;

    public TemplateManager(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public boolean templateExists(String clusterId, String templateName) {
        log.info("Checking if template '{}' exists in cluster '{}'", templateName, clusterId);
        
        try {
            Optional<String> templateOpt = metadataStore.getTemplate(clusterId, templateName);
            return templateOpt.isPresent();
        } catch (Exception e) {
            log.error("Error checking if template '{}' exists in cluster '{}': {}", 
                templateName, clusterId, e.getMessage(), e);
            return false;
        }
    }

    public void putTemplate(String clusterId, String templateName, String templateConfig) throws Exception {
        log.info("Creating/updating template '{}' in cluster '{}'", templateName, clusterId);
        
        // Parse and validate the template request
        TemplateRequest request = parseTemplateRequest(templateConfig);
        
        if (request.getIndexTemplatePattern() == null || request.getIndexTemplatePattern().trim().isEmpty()) {
            throw new IllegalArgumentException("Template must have an index pattern");
        }
        
        // Convert to Template model (4 fields: instanceName, region, templateName, pattern)
        Template template = new Template();
        template.setInstanceName(request.getInstanceName());
        template.setRegion(request.getRegion());
        template.setIndexTemplateName(templateName);  
        template.setIndexTemplatePattern(request.getIndexTemplatePattern());
        
        String templateJson = objectMapper.writeValueAsString(template);
        
        if (templateExists(clusterId, templateName)) {
            log.info("Template '{}' already exists, updating", templateName);
            metadataStore.updateTemplate(clusterId, templateName, templateJson);
        } else {
            log.info("Creating new template '{}'", templateName);
            metadataStore.createTemplate(clusterId, templateName, templateJson);
        }
        
        log.info("Successfully created/updated template '{}' in cluster '{}'", templateName, clusterId);
    }

    public void deleteTemplate(String clusterId, String templateName) throws Exception {
        log.info("Deleting template '{}' from cluster '{}'", templateName, clusterId);
        
        if (!templateExists(clusterId, templateName)) {
            throw new IllegalArgumentException("Template '" + templateName + "' does not exist");
        }
        
        metadataStore.deleteTemplate(clusterId, templateName);
        log.info("Successfully deleted template '{}' from cluster '{}'", templateName, clusterId);
    }

    public String getTemplate(String clusterId, String templateName) throws Exception {
        log.info("Getting template '{}' from cluster '{}'", templateName, clusterId);
        
        Optional<String> templateOpt = metadataStore.getTemplate(clusterId, templateName);
        
        if (templateOpt.isEmpty()) {
            throw new IllegalArgumentException("Template '" + templateName + "' not found");
        }
        
        return templateOpt.get();
    }

    public String getAllTemplates(String clusterId) throws Exception {
        log.info("Getting all templates from cluster '{}'", clusterId);
        
        List<Template> templates = metadataStore.getAllTemplates(clusterId);
        
        // Build response map
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> templatesMap = new HashMap<>();
        
        for (Template template : templates) {
            // Use index_template_name as the key (which is set to the path name)
            String templateName = template.getIndexTemplateName();
            if (templateName != null) {
                templatesMap.put(templateName, template);
            }
        }
        
        response.put("index_templates", templatesMap);
        
        return objectMapper.writeValueAsString(response);
    }
    
    private TemplateRequest parseTemplateRequest(String templateConfig) throws Exception {
        try {
            return objectMapper.readValue(templateConfig, TemplateRequest.class);
        } catch (Exception e) {
            log.error("Failed to parse template request: {}", e.getMessage(), e);
            throw new IllegalArgumentException("Invalid template configuration: " + e.getMessage(), e);
        }
    }
}