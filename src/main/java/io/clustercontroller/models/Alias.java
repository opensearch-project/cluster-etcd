package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for an alias
 * Stored at /aliases/{clusterId}/{aliasName}/conf
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Alias {
    
    @JsonProperty("alias_name")
    private String aliasName;
    
    @JsonProperty("target_indices")
    private Object targetIndices; // Can be String (single) or List<String> (multiple)
    
    @JsonProperty("created_at")
    private String createdAt;
    
    @JsonProperty("updated_at")
    private String updatedAt;
    
    public Alias() {
    }
    
    public Alias(String aliasName, Object targetIndices) {
        this.aliasName = aliasName;
        this.targetIndices = targetIndices;
    }
    
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

