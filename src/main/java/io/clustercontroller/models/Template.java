package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Index template model stored in etcd at:
 * <cluster-name>/templates/<template-name>/conf
 * 
 * Mirrors OpenSearch index template structure:
 * - index_patterns: Array of patterns to match index names (e.g., ["logs-*", "metrics-*"])
 * - priority: Higher priority templates override lower priority ones (default: 0)
 * - template: Contains settings, mappings, and aliases to apply to matching indices
 * 
 * Additional cluster-specific fields:
 * - instanceName: Target instance name (optional)
 * - region: Target region (optional)
 */
@Data
@NoArgsConstructor
public class Template {
    
    @JsonProperty("index_patterns")
    private List<String> indexPatterns;
    
    @JsonProperty("priority")
    private Integer priority;
    
    @JsonProperty("template")
    private TemplateDefinition template;
    
    // Optional cluster-specific fields
    @JsonProperty("instance_name")
    private String instanceName;
    
    @JsonProperty("region")
    private String region;
    
    /**
     * The template definition containing settings, mappings, and aliases.
     * This mirrors OpenSearch's template object structure.
     */
    @Data
    @NoArgsConstructor
    public static class TemplateDefinition {
        @JsonProperty("settings")
        private Map<String, Object> settings;
        
        @JsonProperty("mappings")
        private Map<String, Object> mappings;
        
        @JsonProperty("aliases")
        private Map<String, Object> aliases;
    }
}

