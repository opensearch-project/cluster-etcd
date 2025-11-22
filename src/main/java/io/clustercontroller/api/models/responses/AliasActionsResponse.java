package io.clustercontroller.api.models.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

/**
 * Response model for alias actions.
 */
@Data
@Builder
public class AliasActionsResponse {
    
    @JsonProperty("acknowledged")
    private boolean acknowledged;
    
    @JsonProperty("actions_completed")
    private int actionsCompleted;
    
    @JsonProperty("actions_failed")
    private int actionsFailed;
}

