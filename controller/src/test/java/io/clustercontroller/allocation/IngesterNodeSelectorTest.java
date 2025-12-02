package io.clustercontroller.allocation;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.models.NodesGroup;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class IngesterNodeSelectorTest {

    @Test
    void testSelectNodeFromGroup_WithHealthyNodes_SelectsOne() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy(100L);
        IngesterNodeSelector selector = new IngesterNodeSelector(strategy);
        
        List<SearchUnit> nodes = Arrays.asList(
            createHealthyNode("node-1", "us-east-1"),
            createHealthyNode("node-2", "us-west-2"),
            createHealthyNode("node-3", "eu-west-1")
        );
        
        NodesGroup group = createGroup("group-1", nodes);
        Set<String> usedZones = Collections.emptySet();
        
        // When
        SearchUnit selected = selector.selectNodeFromGroup(group, "0", "test-index", usedZones);
        
        // Then
        assertThat(selected)
            .as("Should select one healthy node")
            .isNotNull()
            .isIn(nodes);
    }
    
    @Test
    void testSelectNodeFromGroup_FiltersUnhealthyNodes() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        IngesterNodeSelector selector = new IngesterNodeSelector(strategy);
        
        List<SearchUnit> nodes = Arrays.asList(
            createUnhealthyNode("unhealthy-1", "us-east-1"),
            createHealthyNode("healthy-1", "us-west-2"),
            createUnhealthyNode("unhealthy-2", "eu-west-1"),
            createHealthyNode("healthy-2", "ap-south-1")
        );
        
        NodesGroup group = createGroup("group-1", nodes);
        Set<String> usedZones = Collections.emptySet();
        
        // When
        SearchUnit selected = selector.selectNodeFromGroup(group, "0", "test-index", usedZones);
        
        // Then
        assertThat(selected)
            .as("Should only select from healthy nodes")
            .isIn(createHealthyNode("healthy-1", "us-west-2"), 
                  createHealthyNode("healthy-2", "ap-south-1"))
            .satisfies(node -> assertThat(node.getStatePulled()).isEqualTo(HealthState.GREEN));
    }
    
    @Test
    void testSelectNodeFromGroup_PreferssDifferentZone() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        IngesterNodeSelector selector = new IngesterNodeSelector(strategy);
        
        List<SearchUnit> nodes = Arrays.asList(
            createHealthyNode("node-1", "us-east-1"),  // Same zone as used
            createHealthyNode("node-2", "us-west-2"),  // Different zone
            createHealthyNode("node-3", "us-east-1")   // Same zone as used
        );
        
        NodesGroup group = createGroup("group-1", nodes);
        
        Set<String> usedZones = new HashSet<>();
        usedZones.add("us-east-1");  // Already used zone
        
        // When
        SearchUnit selected = selector.selectNodeFromGroup(group, "0", "test-index", usedZones);
        
        // Then
        assertThat(selected)
            .as("Should prefer node in different zone")
            .isEqualTo(createHealthyNode("node-2", "us-west-2"));
        
        assertThat(selected.getZone())
            .as("Selected node should be in different zone")
            .isEqualTo("us-west-2")
            .isNotEqualTo("us-east-1");
    }
    
    @Test
    void testSelectNodeFromGroup_FallbackToSameZone_WhenNoOtherOption() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        IngesterNodeSelector selector = new IngesterNodeSelector(strategy);
        
        // All nodes in same zone
        List<SearchUnit> nodes = Arrays.asList(
            createHealthyNode("node-1", "us-east-1"),
            createHealthyNode("node-2", "us-east-1"),
            createHealthyNode("node-3", "us-east-1")
        );
        
        NodesGroup group = createGroup("group-1", nodes);
        
        Set<String> usedZones = new HashSet<>();
        usedZones.add("us-east-1");  // Already used zone
        
        // When
        SearchUnit selected = selector.selectNodeFromGroup(group, "0", "test-index", usedZones);
        
        // Then
        assertThat(selected)
            .as("Should fall back to selecting node in same zone when no other option")
            .isNotNull()
            .isIn(nodes);
        
        assertThat(selected.getZone())
            .as("Selected node will be in same zone (fallback)")
            .isEqualTo("us-east-1");
    }
    
    @Test
    void testSelectNodeFromGroup_WithEmptyUsedZones_AllowsAnyZone() {
        // Given: Single-writer case (no zones used yet)
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy(100L);
        IngesterNodeSelector selector = new IngesterNodeSelector(strategy);
        
        List<SearchUnit> nodes = Arrays.asList(
            createHealthyNode("node-1", "us-east-1"),
            createHealthyNode("node-2", "us-west-2"),
            createHealthyNode("node-3", "eu-west-1")
        );
        
        NodesGroup group = createGroup("group-1", nodes);
        Set<String> usedZones = Collections.emptySet();  // No zones used yet
        
        // When
        SearchUnit selected = selector.selectNodeFromGroup(group, "0", "test-index", usedZones);
        
        // Then
        assertThat(selected)
            .as("Should select any node when no zones are used yet")
            .isNotNull()
            .isIn(nodes);
    }
    
    @Test
    void testSelectNodeFromGroup_WithNoHealthyNodes_ReturnsNull() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        IngesterNodeSelector selector = new IngesterNodeSelector(strategy);
        
        List<SearchUnit> nodes = Arrays.asList(
            createUnhealthyNode("unhealthy-1", "us-east-1"),
            createUnhealthyNode("unhealthy-2", "us-west-2")
        );
        
        NodesGroup group = createGroup("group-1", nodes);
        Set<String> usedZones = Collections.emptySet();
        
        // When
        SearchUnit selected = selector.selectNodeFromGroup(group, "0", "test-index", usedZones);
        
        // Then
        assertThat(selected)
            .as("Should return null when no healthy nodes available")
            .isNull();
    }
    
    @Test
    void testSelectNodeFromGroup_WithEmptyGroup_ReturnsNull() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        IngesterNodeSelector selector = new IngesterNodeSelector(strategy);
        
        NodesGroup group = createGroup("group-1", Collections.emptyList());
        Set<String> usedZones = Collections.emptySet();
        
        // When
        SearchUnit selected = selector.selectNodeFromGroup(group, "0", "test-index", usedZones);
        
        // Then
        assertThat(selected)
            .as("Should return null when group is empty")
            .isNull();
    }
    
    @Test
    void testSelectNodeFromGroup_WithNullNodes_ReturnsNull() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        IngesterNodeSelector selector = new IngesterNodeSelector(strategy);
        
        NodesGroup group = createGroup("group-1", null);
        Set<String> usedZones = Collections.emptySet();
        
        // When
        SearchUnit selected = selector.selectNodeFromGroup(group, "0", "test-index", usedZones);
        
        // Then
        assertThat(selected)
            .as("Should return null when nodes list is null")
            .isNull();
    }
    
    @Test
    void testSelectNodeFromGroup_UsesProvidedStrategy() {
        // Given: Custom strategy that always returns first eligible node
        IngesterNodeSelectionStrategy customStrategy = new IngesterNodeSelectionStrategy() {
            @Override
            public SearchUnit selectNode(List<SearchUnit> eligibleNodes, NodesGroup group, 
                                        String shardId, String indexName) {
                return eligibleNodes.isEmpty() ? null : eligibleNodes.get(0);
            }
            
            @Override
            public String getStrategyName() {
                return "AlwaysFirst";
            }
        };
        
        IngesterNodeSelector selector = new IngesterNodeSelector(customStrategy);
        
        List<SearchUnit> nodes = Arrays.asList(
            createHealthyNode("node-1", "us-east-1"),
            createHealthyNode("node-2", "us-west-2"),
            createHealthyNode("node-3", "eu-west-1")
        );
        
        NodesGroup group = createGroup("group-1", nodes);
        Set<String> usedZones = Collections.emptySet();
        
        // When
        SearchUnit selected = selector.selectNodeFromGroup(group, "0", "test-index", usedZones);
        
        // Then
        assertThat(selected)
            .as("Should use custom strategy (always returns first)")
            .isEqualTo(nodes.get(0));
    }
    
    // Helper methods
    private NodesGroup createGroup(String groupId, List<SearchUnit> nodes) {
        NodesGroup group = new NodesGroup();
        group.setGroupId(groupId);
        group.setRole("PRIMARY");
        group.setNodes(nodes);
        return group;
    }
    
    private SearchUnit createHealthyNode(String name, String zone) {
        SearchUnit node = new SearchUnit();
        node.setId(name);
        node.setName(name);
        node.setRole("PRIMARY");
        node.setZone(zone);
        node.setStatePulled(HealthState.GREEN);
        node.setStateAdmin("NORMAL");
        return node;
    }
    
    private SearchUnit createUnhealthyNode(String name, String zone) {
        SearchUnit node = new SearchUnit();
        node.setId(name);
        node.setName(name);
        node.setRole("PRIMARY");
        node.setZone(zone);
        node.setStatePulled(HealthState.RED);  // Unhealthy
        node.setStateAdmin("NORMAL");
        return node;
    }
}

