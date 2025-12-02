package io.clustercontroller.enums;

/**
 * Enum representing the state of a shard in the cluster.
 * 
 * <ul>
 *   <li><strong>STARTED</strong> - Shard is active and ready to handle requests</li>
 *   <li><strong>INITIALIZING</strong> - Shard is being initialized or recovered</li>
 *   <li><strong>RELOCATING</strong> - Shard is being moved from one node to another</li>
 *   <li><strong>FAILED</strong> - Shard has failed and needs attention</li>
 *   <li><strong>UNASSIGNED</strong> - Shard is not yet assigned to any node</li>
 * </ul>
 */
public enum ShardState {
    /**
     * Shard is active and ready to handle requests.
     */
    STARTED,
    
    /**
     * Shard is being initialized or recovered.
     */
    INITIALIZING,
    
    /**
     * Shard is being moved from one node to another.
     */
    RELOCATING,
    
    /**
     * Shard has failed and needs attention.
     */
    FAILED,
    
    /**
     * Shard is not yet assigned to any node.
     */
    UNASSIGNED
}
