package io.clustercontroller.api.models.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Request model for alias actions (add/remove operations).
 * 
 * Example:
 * {
 *   "actions": [
 *     {"add": {"index": "logs_2024", "alias": "current_logs"}},
 *     {"remove": {"index": "logs_2023", "alias": "current_logs"}}
 *   ]
 * }
 */
@Data
@NoArgsConstructor
public class AliasActionsRequest {
    
    @JsonProperty("actions")
    private List<AliasAction> actions;
}

