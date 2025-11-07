package io.clustercontroller.api.models.requests;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.clustercontroller.models.ClusterInformation;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Request model for updating cluster information.
 * 
 * This model is used to update cluster version in clustermetadata.
 * 
 * Example usage:
 * <pre>
 * {
 *   "version": {
 *     "number": "3.2.0",
 *     "distribution": "opensearch",
 *     "build_type": "tar",
 *     "build_hash": "abc123def456",
 *     "build_date": "2024-01-01T00:00:00Z",
 *     "lucene_version": "9.9.0"
 *   }
 * }
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class ClusterInformationRequest {
    /**
     * Version and build metadata.
     * This is the only field that gets persisted to cluster metadata.
     */
    @JsonProperty("version")
    private ClusterInformation.Version version;
}

