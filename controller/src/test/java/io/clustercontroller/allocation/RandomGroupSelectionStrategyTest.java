package io.clustercontroller.allocation;

import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.NodesGroup;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for RandomGroupSelectionStrategy.
 */
class RandomGroupSelectionStrategyTest {

    private RandomGroupSelectionStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new RandomGroupSelectionStrategy();
    }

    @Test
    void testSelectGroupsWhenEnoughAvailable() {
        // Given
        List<NodesGroup> groups = createTestGroups(5);
        int numGroupsNeeded = 3;

        // When
        List<NodesGroup> selected = strategy.selectGroups(groups, NodeRole.REPLICA, numGroupsNeeded);

        // Then
        assertThat(selected).hasSize(3);
        assertThat(selected).allMatch(group -> groups.contains(group));
    }

    @Test
    void testSelectGroupsWhenMoreRequestedThanAvailable() {
        // Given
        List<NodesGroup> groups = createTestGroups(3);
        int numGroupsNeeded = 5;

        // When
        List<NodesGroup> selected = strategy.selectGroups(groups, NodeRole.REPLICA, numGroupsNeeded);

        // Then
        assertThat(selected).hasSize(3);
        assertThat(selected).containsExactlyInAnyOrderElementsOf(groups);
    }

    @Test
    void testSelectGroupsWithEmptyList() {
        // Given
        List<NodesGroup> groups = List.of();
        int numGroupsNeeded = 3;

        // When
        List<NodesGroup> selected = strategy.selectGroups(groups, NodeRole.REPLICA, numGroupsNeeded);

        // Then
        assertThat(selected).isEmpty();
    }

    @Test
    void testSelectGroupsWithZeroRequested() {
        // Given
        List<NodesGroup> groups = createTestGroups(5);
        int numGroupsNeeded = 0;

        // When
        List<NodesGroup> selected = strategy.selectGroups(groups, NodeRole.REPLICA, numGroupsNeeded);

        // Then
        assertThat(selected).isEmpty();
    }

    @Test
    void testSelectGroupsWithOneGroup() {
        // Given
        List<NodesGroup> groups = createTestGroups(1);
        int numGroupsNeeded = 1;

        // When
        List<NodesGroup> selected = strategy.selectGroups(groups, NodeRole.REPLICA, numGroupsNeeded);

        // Then
        assertThat(selected).hasSize(1);
        assertThat(selected.get(0)).isEqualTo(groups.get(0));
    }

    @Test
    void testRandomnessOfSelection() {
        // Given: Create a larger pool to have meaningful randomness
        List<NodesGroup> groups = createTestGroups(10);
        int numGroupsNeeded = 3;
        Set<String> allSelectedGroupIds = new HashSet<>();

        // When: Run selection multiple times
        for (int i = 0; i < 20; i++) {
            List<NodesGroup> selected = strategy.selectGroups(groups, NodeRole.REPLICA, numGroupsNeeded);
            selected.forEach(group -> allSelectedGroupIds.add(group.getGroupId()));
        }

        // Then: Over 20 runs, we should have selected more than just 3 groups (randomness)
        // This proves the selection is not deterministic
        assertThat(allSelectedGroupIds).hasSizeGreaterThan(3);
    }

    @Test
    void testStrategyName() {
        // When
        String name = strategy.getStrategyName();

        // Then
        assertThat(name).isEqualTo("Random");
    }

    @Test
    void testSelectGroupsForPrimaryRole() {
        // Given
        List<NodesGroup> groups = createTestGroups(5);
        int numGroupsNeeded = 2;

        // When
        List<NodesGroup> selected = strategy.selectGroups(groups, NodeRole.PRIMARY, numGroupsNeeded);

        // Then
        assertThat(selected).hasSize(2);
        assertThat(selected).allMatch(group -> groups.contains(group));
    }

    @Test
    void testNoDuplicatesInSelection() {
        // Given
        List<NodesGroup> groups = createTestGroups(10);
        int numGroupsNeeded = 5;

        // When
        List<NodesGroup> selected = strategy.selectGroups(groups, NodeRole.REPLICA, numGroupsNeeded);

        // Then
        assertThat(selected).hasSize(5);
        Set<String> uniqueGroupIds = new HashSet<>();
        selected.forEach(group -> uniqueGroupIds.add(group.getGroupId()));
        assertThat(uniqueGroupIds).hasSize(5); // No duplicates
    }

    /**
     * Helper method to create test groups.
     */
    private List<NodesGroup> createTestGroups(int count) {
        List<NodesGroup> groups = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            NodesGroup group = new NodesGroup();
            group.setGroupId("group-" + i);
            group.setRole("REPLICA");
            group.setShardId("shard-" + i);
            
            // Create some dummy nodes for the group
            List<SearchUnit> nodes = new ArrayList<>();
            for (int j = 0; j < 3; j++) {
                SearchUnit node = new SearchUnit();
                node.setId("node-" + i + "-" + j);
                node.setShardId("shard-" + i);
                nodes.add(node);
            }
            group.setNodes(nodes);
            group.setCurrentLoad(0);
            
            groups.add(group);
        }
        return groups;
    }
}

