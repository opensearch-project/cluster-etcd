package io.clustercontroller.config;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static io.clustercontroller.config.Constants.*;

/**
 * Configuration for cluster controller.
 * Loads configuration from application.yml with fallbacks to constants.
 * <p>
 * TODO: Refactor to use Spring's @ConfigurationProperties instead of manual SnakeYAML parsing
 * This would allow Spring to handle all property resolution including environment variables.
 */
@Slf4j
@Getter
public class ClusterControllerConfig {
    
    private final String clusterName;
    private final String[] etcdEndpoints;
    private final long taskIntervalSeconds;

    // Default classpath location
    private static final String DEFAULT_CONFIG_FILE_CLASSPATH = "application.yml";
    // Environment variable to check for external config file path
    private static final String EXTERNAL_CONFIG_ENV_VAR = "CONTROLLER_CONFIG_FILE";

    public ClusterControllerConfig() {
        ConfigModel config = loadYamlConfig();
        
        // Parse configuration values with null-safe defaults
        this.clusterName = parseClusterName(config);
        this.etcdEndpoints = parseEndpoints(config);
        this.taskIntervalSeconds = parseTaskIntervalSeconds(config);
        
        log.info("Loaded cluster controller config - cluster: {}, etcd endpoints: {}, task interval: {}s", 
                clusterName, String.join(", ", etcdEndpoints), taskIntervalSeconds);
    }


    private ConfigModel loadYamlConfig() {
        Yaml yaml = new Yaml(new Constructor(ConfigModel.class));
        InputStream inputStream = null;
        String loadedFrom = "";

        // 1. Check environment variable for external config file path
        String externalConfigPath = System.getenv(EXTERNAL_CONFIG_ENV_VAR);
        if (externalConfigPath != null && !externalConfigPath.trim().isEmpty()) {
            log.info("External config file path specified via {}: {}", EXTERNAL_CONFIG_ENV_VAR, externalConfigPath);
            try {
                if (Files.exists(Paths.get(externalConfigPath))) {
                    inputStream = new FileInputStream(externalConfigPath);
                    loadedFrom = "external file (" + externalConfigPath + ")";
                } else {
                    log.warn("External config file specified but not found at path: {}. Falling back.", externalConfigPath);
                }
            } catch (IOException e) {
                log.warn("Error opening external config file {}: {}. Falling back.", externalConfigPath, e.getMessage());
            } catch (SecurityException se) {
                log.warn("Permission denied accessing external config file {}: {}. Falling back.", externalConfigPath, se.getMessage());
            }
        } else {
            log.debug("{} environment variable not set, looking for config on classpath.", EXTERNAL_CONFIG_ENV_VAR);
        }

        // 2. If external file wasn't loaded, try classpath
        if (inputStream == null) {
            log.info("Loading config from classpath: {}", DEFAULT_CONFIG_FILE_CLASSPATH);
            inputStream = getClass().getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE_CLASSPATH);
            loadedFrom = "classpath (" + DEFAULT_CONFIG_FILE_CLASSPATH + ")";
            if (inputStream == null) {
                log.warn("Config file not found on classpath: {}. Using defaults.", DEFAULT_CONFIG_FILE_CLASSPATH);
                return new ConfigModel(); // Return empty config if not found anywhere
            }
        }

        // 3. Load from the determined InputStream
        try {
            ConfigModel config = yaml.load(inputStream);
            log.info("Successfully loaded configuration from {}", loadedFrom);
            return config != null ? config : new ConfigModel();
        } catch (Exception e) {
            log.warn("Failed to parse configuration from {}: {}. Using defaults.", loadedFrom, e.getMessage());
            return new ConfigModel(); // Return empty config on parse error
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                log.error("Error closing config file input stream: {}", e.getMessage());
            }
        }
    }

    private String parseClusterName(ConfigModel config) {
        try {
            if (config.getCluster() != null && config.getCluster().getName() != null) {
                String name = config.getCluster().getName().trim();
                if (!name.isEmpty()) {
                    return name;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse cluster name from config, using default: {}", e.getMessage());
        }
        
        return DEFAULT_CLUSTER_NAME;
    }
    
    private String[] parseEndpoints(ConfigModel config) {
        try {
            if (config.getEtcd() != null && config.getEtcd().getEndpoints() != null) {
                var endpoints = config.getEtcd().getEndpoints();
                if (!endpoints.isEmpty()) {
                    return endpoints.toArray(new String[0]);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse etcd endpoints from config, using defaults: {}", e.getMessage());
        }
        
        return new String[]{DEFAULT_ETCD_ENDPOINT};
    }
    
    private long parseTaskIntervalSeconds(ConfigModel config) {
        try {
            if (config.getTask() != null && config.getTask().getIntervalSeconds() != null) {
                return config.getTask().getIntervalSeconds();
            }
        } catch (Exception e) {
            log.warn("Failed to parse task interval from config, using default: {}", e.getMessage());
        }
        
        return DEFAULT_TASK_INTERVAL_SECONDS;
    }
    
    /**
     * Configuration model for the application.yml file.
     */
    @Data
    public static class ConfigModel {
        private Cluster cluster;
        private Etcd etcd;
        private Task task;
        private Controller controller; // Multi-cluster controller config (used by Spring @Value)
    }
    
    @Data
    public static class Cluster {
        private String name;
    }
    
    @Data
    public static class Etcd {
        private List<String> endpoints;
    }
    
    @Data
    public static class Task {
        private Long intervalSeconds;
    }
    
    @Data
    public static class Controller {
        private String id;
        private Ttl ttl;
        private Keepalive keepalive;
    }
    
    @Data
    public static class Ttl {
        private Integer seconds;
    }
    
    @Data
    public static class Keepalive {
        private Interval interval;
    }
    
    @Data
    public static class Interval {
        private Integer seconds;
    }
}
