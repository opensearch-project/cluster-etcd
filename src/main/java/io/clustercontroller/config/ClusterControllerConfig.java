package io.clustercontroller.config;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

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
        
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (is != null) {
                ConfigModel config = yaml.load(is);
                log.info("Loaded configuration from {}", CONFIG_FILE);
                return config != null ? config : new ConfigModel();
            } else {
                log.warn("Configuration file {} not found, using defaults", CONFIG_FILE);
            }
        } catch (IOException e) {
            log.warn("Failed to load configuration file {}, using defaults: {}", CONFIG_FILE, e.getMessage());
        } catch (Exception e) {
            log.warn("Failed to parse configuration file {}, using defaults: {}", CONFIG_FILE, e.getMessage());
        }
        
        return new ConfigModel();
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
}
