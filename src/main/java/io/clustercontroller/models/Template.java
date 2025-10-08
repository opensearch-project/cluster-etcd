package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Template model stored in etcd at:
 * <cluster-name>/templates/<template-name>/conf
 * 
 * Based on createOrUpdateIndexTemplate with 4 fields:
 * instanceName, region, indexTemplateName, indexTemplatePattern
 */
@Data
@NoArgsConstructor
public class Template {
    
    @JsonProperty("instance_name")
    private String instanceName;
    
    @JsonProperty("region")
    private String region;
    
    @JsonProperty("index_template_name")
    private String indexTemplateName;
    
    @JsonProperty("index_template_pattern")
    private String indexTemplatePattern;
}

