package io.clustercontroller.api.models.requests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Request model for bulk alias operations.
 * Contains a list of actions to add or remove aliases.
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
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class BulkAliasRequest {
    
    private List<AliasAction> actions;
}
