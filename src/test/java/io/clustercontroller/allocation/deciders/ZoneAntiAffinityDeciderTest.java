package io.clustercontroller.allocation.deciders;

import io.clustercontroller.enums.Decision;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ZoneAntiAffinityDeciderTest {

    @Test
    void testCanAllocate_NodeInDifferentZone_ReturnsYes() {
        // Given: usedZones contains us-east-1
        Set<String> usedZones = new HashSet<>();
        usedZones.add("us-east-1");
        
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(usedZones);
        
        SearchUnit node = createNode("node-1", "us-west-2");
        
        // When
        Decision decision = decider.canAllocate("0", node, "test-index", NodeRole.PRIMARY);
        
        // Then
        assertThat(decision)
            .as("Should allow node in different zone")
            .isEqualTo(Decision.YES);
    }
    
    @Test
    void testCanAllocate_NodeInSameZone_ReturnsNo() {
        // Given: usedZones contains us-east-1
        Set<String> usedZones = new HashSet<>();
        usedZones.add("us-east-1");
        
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(usedZones);
        
        SearchUnit node = createNode("node-1", "us-east-1");
        
        // When
        Decision decision = decider.canAllocate("0", node, "test-index", NodeRole.PRIMARY);
        
        // Then
        assertThat(decision)
            .as("Should reject node in same zone")
            .isEqualTo(Decision.NO);
    }
    
    @Test
    void testCanAllocate_NodeWithNullZone_ReturnsYes() {
        // Given
        Set<String> usedZones = new HashSet<>();
        usedZones.add("us-east-1");
        
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(usedZones);
        
        SearchUnit node = createNode("node-1", null);
        
        // When
        Decision decision = decider.canAllocate("0", node, "test-index", NodeRole.PRIMARY);
        
        // Then
        assertThat(decision)
            .as("Should allow node with null zone (no zone info)")
            .isEqualTo(Decision.YES);
    }
    
    @Test
    void testCanAllocate_NodeWithEmptyZone_ReturnsYes() {
        // Given
        Set<String> usedZones = new HashSet<>();
        usedZones.add("us-east-1");
        
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(usedZones);
        
        SearchUnit node = createNode("node-1", "");
        
        // When
        Decision decision = decider.canAllocate("0", node, "test-index", NodeRole.PRIMARY);
        
        // Then
        assertThat(decision)
            .as("Should allow node with empty zone (no zone info)")
            .isEqualTo(Decision.YES);
    }
    
    @Test
    void testCanAllocate_EmptyUsedZones_ReturnsYes() {
        // Given: No zones used yet (single-writer case)
        Set<String> usedZones = Collections.emptySet();
        
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(usedZones);
        
        SearchUnit node = createNode("node-1", "us-east-1");
        
        // When
        Decision decision = decider.canAllocate("0", node, "test-index", NodeRole.PRIMARY);
        
        // Then
        assertThat(decision)
            .as("Should allow any zone when no zones are used yet (single-writer case)")
            .isEqualTo(Decision.YES);
    }
    
    @Test
    void testCanAllocate_NullUsedZones_ReturnsYes() {
        // Given: Null usedZones (single-writer case)
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(null);
        
        SearchUnit node = createNode("node-1", "us-east-1");
        
        // When
        Decision decision = decider.canAllocate("0", node, "test-index", NodeRole.PRIMARY);
        
        // Then
        assertThat(decision)
            .as("Should allow any zone when usedZones is null")
            .isEqualTo(Decision.YES);
    }
    
    @Test
    void testCanAllocate_MultipleZonesUsed_ChecksCorrectly() {
        // Given: Multiple zones already used
        Set<String> usedZones = new HashSet<>();
        usedZones.add("us-east-1");
        usedZones.add("us-west-2");
        
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(usedZones);
        
        // When/Then: Node in used zone should be rejected
        SearchUnit nodeInUsedZone1 = createNode("node-1", "us-east-1");
        assertThat(decider.canAllocate("0", nodeInUsedZone1, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
        
        SearchUnit nodeInUsedZone2 = createNode("node-2", "us-west-2");
        assertThat(decider.canAllocate("0", nodeInUsedZone2, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.NO);
        
        // When/Then: Node in different zone should be allowed
        SearchUnit nodeInDifferentZone = createNode("node-3", "eu-west-1");
        assertThat(decider.canAllocate("0", nodeInDifferentZone, "test-index", NodeRole.PRIMARY))
            .isEqualTo(Decision.YES);
    }
    
    @Test
    void testCanAllocate_ForReplicaRole_ReturnsYes() {
        // Given: Zone anti-affinity only applies to PRIMARY role
        Set<String> usedZones = new HashSet<>();
        usedZones.add("us-east-1");
        
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(usedZones);
        
        SearchUnit replicaNode = createNode("replica-1", "us-east-1");
        
        // When
        Decision decision = decider.canAllocate("0", replicaNode, "test-index", NodeRole.REPLICA);
        
        // Then
        assertThat(decision)
            .as("Zone anti-affinity should not apply to REPLICA role")
            .isEqualTo(Decision.YES);
    }
    
    @Test
    void testGetName() {
        // Given
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(Collections.emptySet());
        
        // When
        String name = decider.getName();
        
        // Then
        assertThat(name)
            .as("Decider name should be 'ZoneAntiAffinityDecider'")
            .isEqualTo("ZoneAntiAffinityDecider");
    }
    
    @Test
    void testIsEnabled_DefaultTrue() {
        // Given
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(Collections.emptySet());
        
        // When
        boolean enabled = decider.isEnabled();
        
        // Then
        assertThat(enabled)
            .as("Decider should be enabled by default")
            .isTrue();
    }
    
    @Test
    void testSetEnabled() {
        // Given
        ZoneAntiAffinityDecider decider = new ZoneAntiAffinityDecider(Collections.emptySet());
        
        // When
        decider.setEnabled(false);
        
        // Then
        assertThat(decider.isEnabled())
            .as("Decider should be disabled after setEnabled(false)")
            .isFalse();
        
        // When
        decider.setEnabled(true);
        
        // Then
        assertThat(decider.isEnabled())
            .as("Decider should be enabled after setEnabled(true)")
            .isTrue();
    }
    
    // Helper method
    private SearchUnit createNode(String name, String zone) {
        SearchUnit node = new SearchUnit();
        node.setId(name);
        node.setName(name);
        node.setRole("PRIMARY");
        node.setZone(zone);
        node.setStatePulled(HealthState.GREEN);
        return node;
    }
}

