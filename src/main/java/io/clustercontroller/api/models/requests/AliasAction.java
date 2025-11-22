package io.clustercontroller.api.models.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Represents a single action in an alias operation.
 * Each action can be either "add" or "remove".
 */
@Data
@NoArgsConstructor
public class AliasAction {
    
    @JsonProperty("add")
    private AliasActionDetails add;
    
    @JsonProperty("remove")
    private AliasActionDetails remove;
    
    @Data
    @NoArgsConstructor
    public static class AliasActionDetails {
        @JsonProperty("index")
        private String index;
        
        @JsonProperty("alias")
        private String alias;
    }
}

