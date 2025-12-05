package io.clustercontroller.allocation.deciders;

import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * Zone anti-affinity decider for multi-writer scenarios.
 * 
 * Ensures nodes are allocated in different zones when possible.
 * Used for PRIMARY allocation when multiple ingester groups are selected.
 * 
 * Decision logic:
 * - If node's zone is NOT in usedZones → YES (prefer this)
 * - If node's zone IS in usedZones → NO (avoid same zone)
 * - If node has no zone info → YES (allow by default)
 * 
 * Note: This is best-effort. If all nodes are in same zone, caller
 * can fall back to ignoring this decider.
 */
@Slf4j
public class ZoneAntiAffinityDecider implements AllocationDecider {
    
    private final Set<String> usedZones;
    private boolean enabled = true;
    
    /**
     * Constructor with used zones context.
     * 
     * @param usedZones Set of zones already used by other selected nodes
     */
    public ZoneAntiAffinityDecider(Set<String> usedZones) {
        this.usedZones = usedZones;
    }
    
    @Override
    public Decision canAllocate(String shardId, SearchUnit node, String indexName, NodeRole targetRole) {
        // Only apply for PRIMARY role (ingesters)
        if (targetRole != NodeRole.PRIMARY) {
            return Decision.YES;  // N/A for replicas
        }
        
        // If no zones tracked yet, allow any zone
        if (usedZones == null || usedZones.isEmpty()) {
            log.trace("ZoneAntiAffinity: No zones used yet, allowing node {} (zone={})", 
                     node.getName(), node.getZone());
            return Decision.YES;
        }
        
        // If node has no zone info, allow by default
        if (node.getZone() == null || node.getZone().isEmpty()) {
            log.debug("ZoneAntiAffinity: Node {} has no zone info, allowing by default", 
                     node.getName());
            return Decision.YES;
        }
        
        // Check if node's zone is already used
        if (usedZones.contains(node.getZone())) {
            log.debug("ZoneAntiAffinity: Node {} (zone={}) is in already-used zones {}, rejecting", 
                     node.getName(), node.getZone(), usedZones);
            return Decision.NO;  // Same zone - reject
        }
        
        // Different zone - prefer this!
        log.trace("ZoneAntiAffinity: Node {} (zone={}) is in different zone from {}, accepting", 
                 node.getName(), node.getZone(), usedZones);
        return Decision.YES;
    }
    
    @Override
    public String getName() {
        return "ZoneAntiAffinityDecider";
    }
    
    @Override
    public boolean isEnabled() {
        return enabled;
    }
    
    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}

