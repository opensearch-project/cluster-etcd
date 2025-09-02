package com.uber.esdock.osgateway.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    
    // Legacy fields for backward compatibility
    @JsonProperty("node_state")
    private String nodeState; // e.g. "GREEN", "YELLOW", "RED" - derived from node health
    
    @JsonProperty("last_updated")
    private String lastUpdated;
    
    @JsonProperty("version")
    private long version;
    
    public SearchUnitActualState() {
        this.nodeRouting = new HashMap<>();
        this.version = 1;
    }
    
    // ========== GETTERS ==========
    
    // Node identification getters
    public String getNodeName() { return nodeName; }
    public String getAddress() { return address; }
    public int getPort() { return port; }
    public String getNodeId() { return nodeId; }
    public String getEphemeralId() { return ephemeralId; }
    
    // Resource usage getters
    public long getMemoryUsedMB() { return memoryUsedMB; }
    public long getMemoryMaxMB() { return memoryMaxMB; }
    public int getMemoryUsedPercent() { return memoryUsedPercent; }
    public long getHeapUsedMB() { return heapUsedMB; }
    public long getHeapMaxMB() { return heapMaxMB; }
    public int getHeapUsedPercent() { return heapUsedPercent; }
    public long getDiskTotalMB() { return diskTotalMB; }
    public long getDiskAvailableMB() { return diskAvailableMB; }
    public int getCpuUsedPercent() { return cpuUsedPercent; }
    
    // Timing getters
    public long getHeartbeatIntervalMillis() { return heartbeatIntervalMillis; }
    public long getTimestamp() { return timestamp; }
    
    // Main routing data getter
    public Map<String, List<ShardRoutingInfo>> getNodeRouting() { 
        return nodeRouting; 
    }
    
    // Legacy field getters
    public String getNodeState() { return nodeState; }
    public String getLastUpdated() { return lastUpdated; }
    public long getVersion() { return version; }
    
    // ========== SETTERS ==========
    
    // Node identification setters
    public void setNodeName(String nodeName) { this.nodeName = nodeName; }
    public void setAddress(String address) { this.address = address; }
    public void setPort(int port) { this.port = port; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public void setEphemeralId(String ephemeralId) { this.ephemeralId = ephemeralId; }
    
    // Resource usage setters
    public void setMemoryUsedMB(long memoryUsedMB) { this.memoryUsedMB = memoryUsedMB; }
    public void setMemoryMaxMB(long memoryMaxMB) { this.memoryMaxMB = memoryMaxMB; }
    public void setMemoryUsedPercent(int memoryUsedPercent) { this.memoryUsedPercent = memoryUsedPercent; }
    public void setHeapUsedMB(long heapUsedMB) { this.heapUsedMB = heapUsedMB; }
    public void setHeapMaxMB(long heapMaxMB) { this.heapMaxMB = heapMaxMB; }
    public void setHeapUsedPercent(int heapUsedPercent) { this.heapUsedPercent = heapUsedPercent; }
    public void setDiskTotalMB(long diskTotalMB) { this.diskTotalMB = diskTotalMB; }
    public void setDiskAvailableMB(long diskAvailableMB) { this.diskAvailableMB = diskAvailableMB; }
    public void setCpuUsedPercent(int cpuUsedPercent) { this.cpuUsedPercent = cpuUsedPercent; }
    
    // Timing setters
    public void setHeartbeatIntervalMillis(long heartbeatIntervalMillis) { this.heartbeatIntervalMillis = heartbeatIntervalMillis; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    // Main routing data setter
    public void setNodeRouting(Map<String, List<ShardRoutingInfo>> nodeRouting) { 
        this.nodeRouting = nodeRouting != null ? nodeRouting : new HashMap<>(); 
    }
    
    // Legacy field setters
    public void setNodeState(String nodeState) { this.nodeState = nodeState; }
    public void setLastUpdated(String lastUpdated) { this.lastUpdated = lastUpdated; }
    public void setVersion(long version) { this.version = version; }
    
    // ========== UTILITY METHODS ==========

    @Override
    public String toString() {
        return "SearchUnitActualState{" +
                "nodeName='" + nodeName + '\'' +
                ", address='" + address + ":" + port + '\'' +
                ", nodeId='" + nodeId + '\'' +
                ", memoryUsed=" + memoryUsedPercent + "%" +
                ", heapUsed=" + heapUsedPercent + "%" +
                ", cpuUsed=" + cpuUsedPercent + "%" +
                ", nodeRouting=" + nodeRouting.keySet() +
                ", lastUpdated='" + lastUpdated + '\'' +
                ", version=" + version +
                '}';
    }
    
    /**
     * Shard routing information for a single shard on this node
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShardRoutingInfo {
        @JsonProperty("shardId")
        private int shardId;
        
        @JsonProperty("primary")
        private boolean primary;
        
        @JsonProperty("state")
        private String state; // e.g., "STARTED", "INITIALIZING", "RELOCATING"
        
        @JsonProperty("relocating")
        private boolean relocating;
        
        @JsonProperty("allocationId")
        private String allocationId;
        
        @JsonProperty("currentNodeId")
        private String currentNodeId;
        
        public ShardRoutingInfo() {}
        
        public ShardRoutingInfo(int shardId, boolean primary, String state) {
            this.shardId = shardId;
            this.primary = primary;
            this.state = state;
            this.relocating = false;
        }
        
        // ========== GETTERS ==========
        public int getShardId() { return shardId; }
        public boolean isPrimary() { return primary; }
        public String getState() { return state; }
        public boolean isRelocating() { return relocating; }
        public String getAllocationId() { return allocationId; }
        public String getCurrentNodeId() { return currentNodeId; }
        
        // ========== SETTERS ==========
        public void setShardId(int shardId) { this.shardId = shardId; }
        public void setPrimary(boolean primary) { this.primary = primary; }
        public void setState(String state) { this.state = state; }
        public void setRelocating(boolean relocating) { this.relocating = relocating; }
        public void setAllocationId(String allocationId) { this.allocationId = allocationId; }
        public void setCurrentNodeId(String currentNodeId) { this.currentNodeId = currentNodeId; }
        
        @Override
        public String toString() {
            return "ShardRoutingInfo{" +
                    "shardId=" + shardId +
                    ", primary=" + primary +
                    ", state='" + state + '\'' +
                    ", relocating=" + relocating +
                    '}';
        }
    }
} 