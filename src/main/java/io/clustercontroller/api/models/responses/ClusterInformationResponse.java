package io.clustercontroller.api.models.responses;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response model for cluster information update operations.
 * 
 * Returns a simple acknowledgment that the cluster information (version)
 * has been successfully updated in the cluster metadata.
 * 
 * Example response:
 * <pre>
 * {
 *   "acknowledged": true
 * }
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class ClusterInformationResponse {
    
    /**
     * Indicates whether the cluster information update was acknowledged.
     */
    private boolean acknowledged;
    
    /**
     * Creates a successful response.
     * 
     * @return ClusterInformationResponse with acknowledged=true
     */
    public static ClusterInformationResponse success() {
        return ClusterInformationResponse.builder()
            .acknowledged(true)
            .build();
    }
}

