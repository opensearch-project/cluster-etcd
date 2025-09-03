package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.clustercontroller.config.Constants;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.ShardState;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchUnitActualState {

    @JsonProperty("nodeName")
    private String nodeName;
    
    @JsonProperty("address") 
    private String address;
    
    @JsonProperty("port")
    private int port;
    
    @JsonProperty("nodeId")
    private String nodeId;
    
    @JsonProperty("ephemeralId")
    private String ephemeralId;
    
    // Resource usage metrics
    @JsonProperty("memoryUsedMB")
    private long memoryUsedMB;
    
    @JsonProperty("memoryMaxMB")
    private long memoryMaxMB;
    
    @JsonProperty("memoryUsedPercent")
    private int memoryUsedPercent;
    
    @JsonProperty("heapUsedMB")
    private long heapUsedMB;
    
    @JsonProperty("heapMaxMB")
    private long heapMaxMB;
    
    @JsonProperty("heapUsedPercent")
    private int heapUsedPercent;
    
    @JsonProperty("diskTotalMB")
    private long diskTotalMB;
    
    @JsonProperty("diskAvailableMB")
    private long diskAvailableMB;
    
    @JsonProperty("cpuUsedPercent")
    private int cpuUsedPercent;
    
    // Heartbeat and timing
    @JsonProperty("heartbeatIntervalMillis")
    private long heartbeatIntervalMillis;
    
    @JsonProperty("timestamp")
    private long timestamp;
    
    // Shard routing information - the key part for controller logic
    @JsonProperty("nodeRouting")
    private Map<String, List<ShardRoutingInfo>> nodeRouting; // index-name -> list of shard routing info
    
    // Node role and shard information (populated by worker)
    @JsonProperty("role")
    private String role; // "primary", "replica", "coordinator"
    
        @JsonProperty("shard_id")
    private String shardId; // "shard-1", "shard-2", etc.
    
    @JsonProperty("cluster_name")
    private String clusterName; // "search-cluster", "analytics-cluster", etc.

    
    public SearchUnitActualState() {
        this.nodeRouting = new HashMap<>();
    }
    
    // ========== UTILITY METHODS ==========
    
    /**
     * Determine if the search unit is healthy based on node state
     */
    public boolean isHealthy() {
        // Consider node healthy if memory and disk usage are reasonable
        // TODO: come up with more comprehensive health check logic
        return memoryUsedPercent < Constants.HEALTH_CHECK_MEMORY_THRESHOLD_PERCENT 
            && diskAvailableMB > Constants.HEALTH_CHECK_DISK_THRESHOLD_MB;
    }
    
    /**
     * Determines the admin state of this search unit based on its health status.
     * 
     * @return NORMAL if the unit is healthy, DRAIN if unhealthy
     */
    public String deriveAdminState() {
        return isHealthy() ? Constants.ADMIN_STATE_NORMAL : Constants.ADMIN_STATE_DRAIN;
    }
    
    /**
     * Derive node state directly as the final status representation
     * Determined by the health of the node AND the presence of active shards
     * TODO: Determine if we should report RED/YELLOW/GREEN from os directly instead of deriving it from the node state
     * Returns: GREEN (healthy+active), YELLOW (healthy+inactive), RED (unhealthy)
     */
    public HealthState deriveNodeState() {
        // First check if node is healthy based on resource usage
        if (!isHealthy()) {
            return HealthState.RED;
        }
        
        // Then check routing info for active shards
        if (nodeRouting != null && !nodeRouting.isEmpty()) {
            boolean hasActiveShards = nodeRouting.values().stream()
                    .flatMap(List::stream)
                    .anyMatch(routing -> ShardState.STARTED.equals(routing.getState()));
            return hasActiveShards ? HealthState.GREEN : HealthState.YELLOW;
        }
        
        // If healthy but no routing info (e.g., coordinator nodes), consider it green/active
        return HealthState.GREEN;
    }

    
    /**
     * Shard routing information for a single shard on this node
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShardRoutingInfo {
        @JsonProperty("shardId")
        private int shardId;
        
        @JsonProperty("primary")
        private boolean primary;
        
        @JsonProperty("state")
        private ShardState state; // e.g., STARTED, INITIALIZING, RELOCATING
        
        @JsonProperty("relocating")
        private boolean relocating;
        
        @JsonProperty("allocationId")
        private String allocationId;
        
        @JsonProperty("currentNodeId")
        private String currentNodeId;
        
        public ShardRoutingInfo() {}
        
        public ShardRoutingInfo(int shardId, boolean primary, ShardState state) {
            this.shardId = shardId;
            this.primary = primary;
            this.state = state;
            this.relocating = false;
        }
    }
} 