package io.clustercontroller.enums;

/**
 * Node roles used by AllocationDeciders.
 * 
 * PRIMARY: ingest + search, REPLICA: search only, COORDINATOR: routing only
 */
public enum NodeRole {
    PRIMARY("primary"),
    REPLICA("replica"), 
    COORDINATOR("coordinator");
    
    private final String value;
    
    NodeRole(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    public static NodeRole fromString(String value) {
        if (value == null) return null;
        
        String normalized = value.toLowerCase().trim();
        for (NodeRole role : NodeRole.values()) {
            if (role.value.equals(normalized)) {
                return role;
            }
        }
        return null; // Return null for unknown roles instead of throwing exception
    }
}
