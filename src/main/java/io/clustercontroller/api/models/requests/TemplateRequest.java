package io.clustercontroller.api.models.requests;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Request model for index template creation.
 * Based on createOrUpdateIndexTemplate signature with 4 fields:
 * instanceName, region, indexTemplateName, indexTemplatePattern
 *
 * Example usage:
 * <pre>
 * {
 *   "instance_name": "prod-cluster",
 *   "region": "us-west-2",
 *   "index_template_name": "my-template",
 *   "index_template_pattern": "logs-*"
 * }
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class TemplateRequest {
    private String instanceName;
    private String region;
    private String indexTemplateName;
    private String indexTemplatePattern;
}
