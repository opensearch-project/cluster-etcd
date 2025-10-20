package io.clustercontroller.templates;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.clustercontroller.api.models.requests.TemplateRequest;
import io.clustercontroller.models.Template;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

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
            metadataStore.getTemplate(clusterId, templateName);
            return true;
        } catch (IllegalArgumentException e) {
            return false;  // Template not found
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
        
        if (request.getIndexPatterns() == null || request.getIndexPatterns().isEmpty()) {
            throw new IllegalArgumentException("Template must have at least one index pattern");
        }
        
        // Convert to Template model (mirrors OpenSearch structure)
        Template template = new Template();
        template.setIndexPatterns(request.getIndexPatterns());
        template.setPriority(request.getPriority() != null ? request.getPriority() : 0);
        
        // Convert template body
        if (request.getTemplate() != null) {
            Template.TemplateDefinition templateDef = new Template.TemplateDefinition();
            templateDef.setSettings(request.getTemplate().getSettings());
            templateDef.setMappings(request.getTemplate().getMappings());
            templateDef.setAliases(request.getTemplate().getAliases());
            template.setTemplate(templateDef);
        }
        
        // Optional cluster-specific fields
        template.setInstanceName(request.getInstanceName());
        template.setRegion(request.getRegion());
        
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
        
        Template template = metadataStore.getTemplate(clusterId, templateName);
        return objectMapper.writeValueAsString(template);
    }

    public String getAllTemplates(String clusterId) throws Exception {
        log.info("Getting all templates from cluster '{}'", clusterId);
        
        List<Template> templates = metadataStore.getAllTemplates(clusterId);
        
        // Build response similar to OpenSearch GET _index_template API
        // Returns a list of templates with their patterns and configurations
        Map<String, Object> response = new HashMap<>();
        response.put("index_templates", templates);
        
        return objectMapper.writeValueAsString(response);
    }
    
    /**
     * Find all templates that match the given index name and return them sorted by priority.
     * Templates are matched based on their index_patterns.
     * Supports wildcards like "logs-*" matching "logs-2023-01" etc.
     * 
     * @param clusterId The cluster ID
     * @param indexName The index name to match
     * @return List of matching templates sorted by priority (highest first), empty list if none match
     */
    public List<Template> findMatchingTemplates(String clusterId, String indexName) throws Exception {
        log.debug("Finding templates matching index '{}' in cluster '{}'", indexName, clusterId);
        
        List<Template> allTemplates = metadataStore.getAllTemplates(clusterId);
        List<Template> matchingTemplates = allTemplates.stream()
            .filter(template -> {
                if (template.getIndexPatterns() == null) {
                    return false;
                }
                // Check if any pattern in the template matches the index name
                return template.getIndexPatterns().stream()
                    .anyMatch(pattern -> matchesPattern(indexName, pattern));
            })
            .sorted(Comparator.comparingInt(t -> -Optional.ofNullable(t.getPriority()).orElse(0))) // Sort by priority descending
            .collect(Collectors.toList());
        
        log.info("Found {} matching templates for index '{}' in cluster '{}'", 
            matchingTemplates.size(), indexName, clusterId);
        
        if (!matchingTemplates.isEmpty()) {
            matchingTemplates.forEach(t -> log.debug("  - Template with patterns {} (priority: {})", 
                t.getIndexPatterns(), t.getPriority()));
        }
        
        return matchingTemplates;
    }
    
    /**
     * Merge multiple templates' settings and mappings according to priority.
     * Higher priority templates override lower priority ones.
     * 
     * @param templates List of templates sorted by priority (highest first)
     * @return Merged template definition with combined settings, mappings, and aliases
     */
    public Template.TemplateDefinition mergeTemplates(List<Template> templates) {
        log.debug("Merging {} templates", templates.size());
        
        Template.TemplateDefinition merged = new Template.TemplateDefinition();
        merged.setSettings(new HashMap<>());
        merged.setMappings(new HashMap<>());
        merged.setAliases(new HashMap<>());
        
        // Apply templates in reverse order (lowest priority first) so higher priority overrides
        for (int i = templates.size() - 1; i >= 0; i--) {
            Template template = templates.get(i);
            if (template.getTemplate() == null) {
                continue;
            }
            
            Template.TemplateDefinition templateDef = template.getTemplate();
            
            // Merge settings (deep merge)
            if (templateDef.getSettings() != null) {
                mergeMaps(merged.getSettings(), templateDef.getSettings());
            }
            
            // Merge mappings (deep merge)
            if (templateDef.getMappings() != null) {
                mergeMaps(merged.getMappings(), templateDef.getMappings());
            }
            
            // Merge aliases (simple override)
            if (templateDef.getAliases() != null) {
                merged.getAliases().putAll(templateDef.getAliases());
            }
            
            log.debug("Applied template with priority {}", template.getPriority());
        }
        
        return merged;
    }
    
    /**
     * Check if an index name matches a template pattern.
     * Supports wildcard patterns like "logs-*" or "*-archive".
     * 
     * @param indexName The index name to check
     * @param pattern The template pattern (supports * wildcard)
     * @return true if the index name matches the pattern
     */
    private boolean matchesPattern(String indexName, String pattern) {
        // Convert wildcard pattern to regex
        // Escape special regex characters except *
        String regex = pattern
            .replace("\\", "\\\\")
            .replace(".", "\\.")
            .replace("+", "\\+")
            .replace("?", "\\?")
            .replace("(", "\\(")
            .replace(")", "\\)")
            .replace("[", "\\[")
            .replace("]", "\\]")
            .replace("{", "\\{")
            .replace("}", "\\}")
            .replace("^", "\\^")
            .replace("$", "\\$")
            .replace("|", "\\|")
            .replace("*", ".*");  // Convert * to .* for regex
        
        return indexName.matches(regex);
    }
    
    /**
     * Deep merge source map into target map.
     * For nested maps, recursively merge. For other values, source overrides target.
     */
    @SuppressWarnings("unchecked")
    private void mergeMaps(Map<String, Object> target, Map<String, Object> source) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = entry.getKey();
            Object sourceValue = entry.getValue();
            
            if (sourceValue instanceof Map && target.get(key) instanceof Map) {
                // Both are maps, recursively merge
                mergeMaps((Map<String, Object>) target.get(key), (Map<String, Object>) sourceValue);
            } else {
                // Override with source value
                target.put(key, sourceValue);
            }
        }
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