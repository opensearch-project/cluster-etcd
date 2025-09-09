package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.enums.HealthState;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * SearchUnit entity representing a search unit in the cluster.
 */
@Data
public class SearchUnit {
    
    @JsonProperty("id")
    private String id;
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("cluster_name")
    private String clusterName;
    
    @JsonProperty("role")
    private String role; // "primary", "replica", "coordinator"
    
    @JsonProperty("host")
    private String host;
    
    @JsonProperty("port_http")
    private int portHttp = 9200;
    
    @JsonProperty("port_transport")
    private int portTransport = 9300;
    
    @JsonProperty("zone")
    private String zone;
    
    @JsonProperty("shard_id")
    private String shardId;
    
    @JsonProperty("state_admin")
    private String stateAdmin; // "NORMAL", "DRAINED", etc.
    
    @JsonProperty("state_pulled")
    private HealthState statePulled; // GREEN, YELLOW, RED
    
    @JsonProperty("node_attributes")
    private Map<String, Object> nodeAttributes;
    
    public SearchUnit() {
        this.nodeAttributes = new HashMap<>();
    }
    
    public SearchUnit(String name, String role, String host) {
        this();
        this.name = name;
        this.role = role;
        this.host = host;
        this.stateAdmin = "NORMAL";
    }
}
