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
        
        String trimmed = value.trim();
        for (NodeRole role : NodeRole.values()) {
            if (role.value.equals(trimmed)) {
                return role;
            }
        }
        return null; // Return null for unknown roles instead of throwing exception
    }
}
