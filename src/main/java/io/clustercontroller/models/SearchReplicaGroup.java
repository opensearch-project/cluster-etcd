package io.clustercontroller.models;

import lombok.Data;
import java.util.List;

/**
 * Represents a logical group of replica nodes.
 * 
 * A GROUP is a fixed set of identical replicas (typically 3 or 5 nodes)
 * that are treated as a single allocation unit for bin-packing purposes.
 * 
 * All nodes in a group must:
 * - Have the same role (either all SEARCH_REPLICA or all INGEST_REPLICA)
 * - Belong to the same shard pool
 * - Be healthy and available for allocation
 */
@Data
public class SearchReplicaGroup {
    
    /**
     * Unique identifier for this group (e.g., "replica-group-0-shard-0")
     */
    private String groupId;
    
    /**
     * Role of all nodes in this group (SEARCH_REPLICA or INGEST_REPLICA)
     */
    private String role;
    
    /**
     * Shard ID that this group belongs to
     */
    private String shardId;
    
    /**
     * List of SearchUnit node IDs in this group
     */
    private List<String> nodeIds;
    
    /**
     * List of actual SearchUnit objects in this group
     */
    private List<SearchUnit> nodes;
    
    /**
     * Current number of shards allocated to this group
     */
    private int currentShardCount;
    
    /**
     * Maximum capacity of this group (total shards it can hold)
     */
    private int maxCapacity;
    
    /**
     * Whether this group has available capacity
     */
    public boolean hasCapacity() {
        return currentShardCount < maxCapacity;
    }
    
    /**
     * Available capacity in this group
     */
    public int availableCapacity() {
        return Math.max(0, maxCapacity - currentShardCount);
    }
    
    /**
     * Group size (number of nodes)
     */
    public int size() {
        return nodes != null ? nodes.size() : 0;
    }
}

