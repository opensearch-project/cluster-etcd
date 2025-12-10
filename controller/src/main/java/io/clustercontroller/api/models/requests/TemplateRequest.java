package io.clustercontroller.api.models.requests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Request model for index template creation.
 *
 * Example usage:
 * <pre>
 * {
 *   "index_patterns": ["logs-*", "metrics-*"],
 *   "priority": 100,
 *   "template": {
 *     "settings": {
 *       "number_of_shards": 2,
 *       "number_of_replicas": 1
 *     },
 *     "mappings": {
 *       "properties": {
 *         "timestamp": {"type": "date"},
 *         "message": {"type": "text"}
 *       }
 *     },
 *     "aliases": {
 *       "my_logs": {}
 *     }
 *   },
 *   "instance_name": "prod-cluster",
 *   "region": "us-west-2"
 * }
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TemplateRequest {
    private List<String> indexPatterns;
    private Integer priority;
    private TemplateDefinition template;
    
    // Optional cluster-specific fields
    private String instanceName;
    private String region;
    
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class TemplateDefinition {
        private Map<String, Object> settings;
        private Map<String, Object> mappings;
        private Map<String, Object> aliases;
    }
}
