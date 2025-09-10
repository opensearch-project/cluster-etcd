package io.clustercontroller.enums;

/**
 * Enum representing the health state of a search unit or node.
 * 
 * <ul>
 *   <li><strong>GREEN</strong> - Node is healthy and functioning normally with active shards</li>
 *   <li><strong>YELLOW</strong> - Node is healthy but inactive (no active shards or degraded performance)</li>
 *   <li><strong>RED</strong> - Node is unhealthy due to resource constraints or failures</li>
 * </ul>
 */
public enum HealthState {
    /**
     * Node is healthy and functioning normally with active shards.
     */
    GREEN,
    
    /**
     * Node is healthy but inactive (no active shards or degraded performance).
     */
    YELLOW,
    
    /**
     * Node is unhealthy due to resource constraints or failures.
     */
    RED
}
