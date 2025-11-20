package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for an alias
 * Stored at /aliases/{clusterId}/{aliasName}/conf
 */
@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Alias {
    
    @JsonProperty("alias_name")
    private String aliasName;
    
    @JsonProperty("target_indices")
    private Object targetIndices; // Can be String (single) or List<String> (multiple)
    
    @JsonProperty("created_at")
    private String createdAt = java.time.OffsetDateTime.now().toString();
    
    @JsonProperty("updated_at")
    private String updatedAt = java.time.OffsetDateTime.now().toString();
    
    /**
     * Get target indices as a list (whether stored as String or List)
     */
    public List<String> getTargetIndicesAsList() {
        if (targetIndices instanceof String) {
            List<String> list = new ArrayList<>();
            list.add((String) targetIndices);
            return list;
        } else if (targetIndices instanceof List) {
            return (List<String>) targetIndices;
        }
        return new ArrayList<>();
    }
}

