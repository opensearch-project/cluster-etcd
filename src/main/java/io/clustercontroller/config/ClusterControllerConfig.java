package io.clustercontroller.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static io.clustercontroller.config.Constants.*;

/**
 * Configuration for cluster controller.
 * Loads configuration from application.yml with fallbacks to constants.
 */
@Slf4j
@Getter
public class ClusterControllerConfig {
    
    private final String clusterName;
    private final String[] etcdEndpoints;
    private final long taskIntervalSeconds;
    
    private static final String CONFIG_FILE = "application.yml";
    
    public ClusterControllerConfig() {
        Map<String, Object> config = loadYamlConfig();
        
        // Parse configuration values
        this.clusterName = parseClusterName(config);
        this.etcdEndpoints = parseEndpoints(config);
        this.taskIntervalSeconds = parseTaskIntervalSeconds(config);
        
        log.info("Loaded cluster controller config - cluster: {}, etcd endpoints: {}, task interval: {}s", 
                clusterName, String.join(", ", etcdEndpoints), taskIntervalSeconds);
    }
    
    private Map<String, Object> loadYamlConfig() {
        Yaml yaml = new Yaml();
        
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (is != null) {
                Map<String, Object> config = yaml.load(is);
                log.info("Loaded configuration from {}", CONFIG_FILE);
                return config != null ? config : Map.of();
            } else {
                log.warn("Configuration file {} not found, using defaults", CONFIG_FILE);
            }
        } catch (IOException e) {
            log.warn("Failed to load configuration file {}, using defaults: {}", CONFIG_FILE, e.getMessage());
        }
        
        return Map.of();
    }
    
    @SuppressWarnings("unchecked")
    private String parseClusterName(Map<String, Object> config) {
        try {
            Map<String, Object> clusterConfig = (Map<String, Object>) config.get("cluster");
            if (clusterConfig != null) {
                String name = (String) clusterConfig.get("name");
                if (name != null && !name.trim().isEmpty()) {
                    return name.trim();
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse cluster name from config, using default: {}", e.getMessage());
        }
        
        return DEFAULT_CLUSTER_NAME;
    }
    
    @SuppressWarnings("unchecked")
    private String[] parseEndpoints(Map<String, Object> config) {
        try {
            Map<String, Object> etcdConfig = (Map<String, Object>) config.get("etcd");
            if (etcdConfig != null) {
                List<String> endpointsList = (List<String>) etcdConfig.get("endpoints");
                if (endpointsList != null && !endpointsList.isEmpty()) {
                    return endpointsList.toArray(new String[0]);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse etcd endpoints from config, using defaults: {}", e.getMessage());
        }
        
        return new String[]{DEFAULT_ETCD_ENDPOINT};
    }
    
    @SuppressWarnings("unchecked")
    private long parseTaskIntervalSeconds(Map<String, Object> config) {
        try {
            Map<String, Object> taskConfig = (Map<String, Object>) config.get("task");
            if (taskConfig != null) {
                Object intervalObj = taskConfig.get("intervalSeconds");
                if (intervalObj instanceof Number) {
                    return ((Number) intervalObj).longValue();
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse task interval from config, using default: {}", e.getMessage());
        }
        
        return DEFAULT_TASK_INTERVAL_SECONDS;
    }
}
