package io.clustercontroller.api.models.responses;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Response model for cluster health operations.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class HealthResponse {
    private String clusterName;
    private String status;
    private Boolean timedOut;
    private Integer numberOfNodes;
    private Integer numberOfDataNodes;
    private Integer activePrimaryShards;
    private Integer activeShards;
    private Integer relocatingShards;
    private Integer initializingShards;
    private Integer unassignedShards;
    private Map<String, Object> indices;
}
