package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ClusterControllerAssignment {
    @JsonProperty("controller")
    private String controller;

    @JsonProperty("cluster")
    private String cluster;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("lease")
    private String lease;
}
