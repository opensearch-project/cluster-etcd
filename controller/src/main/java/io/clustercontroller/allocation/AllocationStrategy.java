package io.clustercontroller.allocation;

/**
 * Strategy for shard allocation planning.
 */
public enum AllocationStrategy {
    /**
     * Respects the replica count specified in index configuration.
     * Allocates exactly the number of replicas defined in the index config.
     */
    RESPECT_REPLICA_COUNT,
    
    /**
     * Uses all available eligible nodes for allocation.
     * Ignores replica count and allocates to all nodes that pass decider checks.
     */
    USE_ALL_AVAILABLE_NODES
}