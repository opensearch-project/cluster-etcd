package io.clustercontroller.enums;

/**
 * Node roles used by AllocationDeciders.
 * 
 * PRIMARY: ingest + search, REPLICA: search only, COORDINATOR: routing only
 */
public enum NodeRole {
    PRIMARY("PRIMARY"),
    REPLICA("SEARCH_REPLICA"), 
    COORDINATOR("COORDINATOR");
    
    private final String value;
    
    NodeRole(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    public static NodeRole fromString(String value) {
        if (value == null) return null;
        
        String trimmed = value.trim().toUpperCase();
        
        // Handle common role name variations
        switch (trimmed) {
            case "PRIMARY":
                return PRIMARY;
            case "SEARCH_REPLICA":
            case "REPLICA":  // Support both "replica" and "search_replica"
                return REPLICA;
            case "COORDINATOR":
                return COORDINATOR;
            default:
                return null; // Return null for unknown roles
        }
    }
}
