package io.clustercontroller.config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * General application configuration class.
 * Currently contains etcd endpoints and connection settings.
 */
@Slf4j
@Getter
public class ApplicationConfig {
    
    private final String[] endpoints;
    
    private static final String CONFIG_FILE = "application.yml";
    private static final String DEFAULT_ENDPOINTS = "http://localhost:2379";
    
    public ApplicationConfig() {
        Map<String, Object> config = loadYamlConfig();
        
        // Parse endpoints
        this.endpoints = parseEndpoints(config);
        
        log.info("Loaded application config - etcd endpoints: {}", 
                String.join(", ", endpoints));
    }
    
    private Map<String, Object> loadYamlConfig() {
        Yaml yaml = new Yaml();
        
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (is != null) {
                Map<String, Object> config = yaml.load(is);
                log.info("Loaded application configuration from {}", CONFIG_FILE);
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
        
        // Fallback to default
        return new String[]{DEFAULT_ENDPOINTS};
    }
}