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
 * Request model for index template creation and update operations.
 *
 * Allows specifying template patterns, settings, mappings, and aliases that will be
 * automatically applied to new indices matching the specified patterns.
 * Uses Jackson annotations for automatic JSON serialization/deserialization
 * with snake_case property naming for compatibility.
 *
 * Example usage:
 * <pre>
 * {
 *   "index_patterns": ["logs-*", "metrics-*"],
 *   "priority": 100,
 *   "template": {
 *     "settings": {"number_of_shards": 1},
 *     "mappings": {"properties": {"timestamp": {"type": "date"}}},
 *     "aliases": {"current": {}}
 *   }
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
