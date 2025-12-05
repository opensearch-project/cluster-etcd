package io.clustercontroller.api.models.requests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Request model for index creation and configuration operations.
 * 
 * Supports index creation with custom settings, field mappings, and aliases.
 * Uses Jackson annotations for automatic JSON serialization/deserialization
 * with snake_case property naming for compatibility.
 * 
 * Example usage:
 * <pre>
 * {
 *   "settings": {"number_of_replicas": 1, "refresh_interval": "30s"},
 *   "mappings": {"properties": {"title": {"type": "text"}}},
 *   "aliases": {"my_alias": {}}
 * }
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class IndexRequest {
    private Map<String, Object> settings;
    private Map<String, Object> mappings;
    private Map<String, Object> aliases;
}