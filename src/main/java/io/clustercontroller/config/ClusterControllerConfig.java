package io.clustercontroller.config;

import lombok.Data;
import lombok.AllArgsConstructor;

/**
 * Configuration for cluster controller.
 */
@Data
@AllArgsConstructor
public class ClusterControllerConfig {
    
    private final String clusterName;
    private final String[] etcdEndpoints;
    private final long taskIntervalSeconds;
}
