package io.clustercontroller.models;

import lombok.Data;
import java.util.List;

/**
 * Represents a logical group of nodes.
 * 
 * A GROUP is a fixed set of nodes (typically 3 or 5 nodes) with the same role
 * that are treated as a single allocation unit for bin-packing purposes.
 * 
 * All nodes in a group must:
 * - Have the same role (PRIMARY or REPLICA)
 * - Have the same GROUP ID (currently extracted from shard pool)
 */
@Data
public class NodesGroup {
    
    /**
     * Unique identifier for this group (extracted from nodes' GROUP ID)
     */
    private String groupId;
    
    /**
     * Role of all nodes in this group (e.g., "PRIMARY", "SEARCH_REPLICA")
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
     * Current load on this group (number of shards allocated to this group)
     * Used for bin-packing: select groups with lower load
     */
    private int currentLoad;
    
    /**
     * Group size (number of nodes)
     */
    public int size() {
        return nodes != null ? nodes.size() : 0;
    }
}

