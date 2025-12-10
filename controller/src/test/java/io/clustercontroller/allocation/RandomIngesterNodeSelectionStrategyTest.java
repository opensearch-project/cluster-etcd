package io.clustercontroller.allocation;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.models.NodesGroup;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RandomIngesterNodeSelectionStrategyTest {

    @Test
    void testSelectNode_WithMultipleNodes_ReturnsOne() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        
        List<SearchUnit> eligibleNodes = Arrays.asList(
            createNode("node-1"),
            createNode("node-2"),
            createNode("node-3")
        );
        
        NodesGroup group = createGroup("group-1", eligibleNodes);
        
        // When
        SearchUnit selected = strategy.selectNode(eligibleNodes, group, "0", "test-index");
        
        // Then
        assertThat(selected)
            .as("Should select one node from eligible nodes")
            .isNotNull()
            .isIn(eligibleNodes);
    }
    
    @Test
    void testSelectNode_WithSingleNode_ReturnsThatNode() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        
        SearchUnit onlyNode = createNode("only-node");
        List<SearchUnit> eligibleNodes = Collections.singletonList(onlyNode);
        NodesGroup group = createGroup("group-1", eligibleNodes);
        
        // When
        SearchUnit selected = strategy.selectNode(eligibleNodes, group, "0", "test-index");
        
        // Then
        assertThat(selected)
            .as("Should return the only available node")
            .isEqualTo(onlyNode);
    }
    
    @Test
    void testSelectNode_WithEmptyList_ReturnsNull() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        
        List<SearchUnit> eligibleNodes = Collections.emptyList();
        NodesGroup group = createGroup("group-1", eligibleNodes);
        
        // When
        SearchUnit selected = strategy.selectNode(eligibleNodes, group, "0", "test-index");
        
        // Then
        assertThat(selected)
            .as("Should return null when no eligible nodes")
            .isNull();
    }
    
    @Test
    void testSelectNode_WithNullList_ReturnsNull() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        
        NodesGroup group = createGroup("group-1", Collections.emptyList());
        
        // When
        SearchUnit selected = strategy.selectNode(null, group, "0", "test-index");
        
        // Then
        assertThat(selected)
            .as("Should return null when eligible nodes list is null")
            .isNull();
    }
    
    @Test
    void testSelectNode_WithSeededRandom_IsDeterministic() {
        // Given: Two strategies with same seed
        long seed = 12345L;
        RandomIngesterNodeSelectionStrategy strategy1 = new RandomIngesterNodeSelectionStrategy(seed);
        RandomIngesterNodeSelectionStrategy strategy2 = new RandomIngesterNodeSelectionStrategy(seed);
        
        List<SearchUnit> eligibleNodes = Arrays.asList(
            createNode("node-1"),
            createNode("node-2"),
            createNode("node-3"),
            createNode("node-4"),
            createNode("node-5")
        );
        
        NodesGroup group = createGroup("group-1", eligibleNodes);
        
        // When: Select multiple times with each strategy
        SearchUnit selected1a = strategy1.selectNode(eligibleNodes, group, "0", "test-index");
        SearchUnit selected1b = strategy1.selectNode(eligibleNodes, group, "1", "test-index");
        
        SearchUnit selected2a = strategy2.selectNode(eligibleNodes, group, "0", "test-index");
        SearchUnit selected2b = strategy2.selectNode(eligibleNodes, group, "1", "test-index");
        
        // Then: Both strategies with same seed should produce same sequence
        assertThat(selected1a)
            .as("First selection should be deterministic with same seed")
            .isEqualTo(selected2a);
        
        assertThat(selected1b)
            .as("Second selection should be deterministic with same seed")
            .isEqualTo(selected2b);
    }
    
    @Test
    void testGetStrategyName() {
        // Given
        RandomIngesterNodeSelectionStrategy strategy = new RandomIngesterNodeSelectionStrategy();
        
        // When
        String name = strategy.getStrategyName();
        
        // Then
        assertThat(name)
            .as("Strategy name should be 'Random'")
            .isEqualTo("Random");
    }
    
    // Helper methods
    private NodesGroup createGroup(String groupId, List<SearchUnit> nodes) {
        NodesGroup group = new NodesGroup();
        group.setGroupId(groupId);
        group.setRole("PRIMARY");
        group.setNodes(nodes);
        return group;
    }
    
    private SearchUnit createNode(String name) {
        SearchUnit node = new SearchUnit();
        node.setId(name);
        node.setName(name);
        node.setRole("PRIMARY");
        node.setStatePulled(HealthState.GREEN);
        return node;
    }
}

