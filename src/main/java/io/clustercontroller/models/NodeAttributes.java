package io.clustercontroller.models;

import java.util.Map;

/**
 * Node attribute constants and predefined configurations for different node roles.
 * Provides centralized management of node attributes used in cluster configuration.
 */
public final class NodeAttributes {
    
    private NodeAttributes() {
        // Utility class
    }
    
    // Node attribute keys
    public static final String NODE_DATA = "node.data";
    public static final String NODE_INGEST = "node.ingest";
    public static final String NODE_MASTER = "node.master";
    
    // Role constants for consistency
    public static final String ROLE_COORDINATOR = "coordinator";
    public static final String ROLE_PRIMARY = "primary";
    public static final String ROLE_REPLICA = "replica";
    
    // Predefined attribute maps for each role
    public static final Map<String, String> COORDINATOR_ATTRIBUTES = Map.of(
        NODE_DATA, "false",
        NODE_INGEST, "false",
        NODE_MASTER, "true"
    );
    
    public static final Map<String, String> PRIMARY_ATTRIBUTES = Map.of(
        NODE_DATA, "true",
        NODE_INGEST, "true",
        NODE_MASTER, "false"
    );
    
    public static final Map<String, String> REPLICA_ATTRIBUTES = Map.of(
        NODE_DATA, "true",
        NODE_INGEST, "false",
        NODE_MASTER, "false"
    );
    
    /**
     * Get node attributes for a given role.
     * 
     * @param role the node role
     * @return Map of node attributes for the role, or empty map if role is unknown or null
     */
    public static Map<String, String> getAttributesForRole(String role) {
        if (role == null) {
            return Map.of();
        }
        
        return switch (role) {
            case ROLE_COORDINATOR -> COORDINATOR_ATTRIBUTES;
            case ROLE_PRIMARY -> PRIMARY_ATTRIBUTES;
            case ROLE_REPLICA -> REPLICA_ATTRIBUTES;
            default -> Map.of(); // Return empty map for unknown roles
        };
    }
}
