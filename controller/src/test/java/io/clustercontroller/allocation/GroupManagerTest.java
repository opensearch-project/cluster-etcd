package io.clustercontroller.allocation;

import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.NodesGroup;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for GroupManager.
 */
@ExtendWith(MockitoExtension.class)
class GroupManagerTest {

    @Mock
    private GroupSelectionStrategy mockStrategy;

    private GroupManager groupManager;

    @BeforeEach
    void setUp() {
        groupManager = new GroupManager(mockStrategy);
    }

    @Test
    void testDiscoverGroupsWithMultipleGroups() {
        // Given: 2 replica groups with 3 nodes each
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createNodesForGroup("group-1", "SEARCH_REPLICA", 3));
        allNodes.addAll(createNodesForGroup("group-2", "SEARCH_REPLICA", 3));

        // When
        List<NodesGroup> groups = groupManager.discoverGroups(allNodes);

        // Then
        assertThat(groups).hasSize(2);
        assertThat(groups).allMatch(group -> group.getNodes().size() == 3);
        assertThat(groups).allMatch(group -> group.getRole().equals("SEARCH_REPLICA"));
        
        // Verify group IDs
        assertThat(groups).extracting(NodesGroup::getGroupId)
            .containsExactlyInAnyOrder("group-1", "group-2");
    }

    @Test
    void testDiscoverGroupsWithPrimaryGroups() {
        // Given: 2 primary groups with 5 nodes each
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createNodesForGroup("primary-group-1", "PRIMARY", 5));
        allNodes.addAll(createNodesForGroup("primary-group-2", "PRIMARY", 5));

        // When
        List<NodesGroup> groups = groupManager.discoverGroups(allNodes);

        // Then
        assertThat(groups).hasSize(2);
        assertThat(groups).allMatch(group -> group.getNodes().size() == 5);
        assertThat(groups).allMatch(group -> group.getRole().equals("PRIMARY"));
    }

    @Test
    void testDiscoverGroupsWithMixedRolesInSeparateGroups() {
        // Given: 1 primary group and 1 replica group
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createNodesForGroup("primary-group", "PRIMARY", 3));
        allNodes.addAll(createNodesForGroup("replica-group", "SEARCH_REPLICA", 3));

        // When
        List<NodesGroup> groups = groupManager.discoverGroups(allNodes);

        // Then
        assertThat(groups).hasSize(2);
        
        NodesGroup primaryGroup = groups.stream()
            .filter(g -> g.getGroupId().equals("primary-group"))
            .findFirst().orElseThrow();
        assertThat(primaryGroup.getRole()).isEqualTo("PRIMARY");
        assertThat(primaryGroup.getNodes()).hasSize(3);
        
        NodesGroup replicaGroup = groups.stream()
            .filter(g -> g.getGroupId().equals("replica-group"))
            .findFirst().orElseThrow();
        assertThat(replicaGroup.getRole()).isEqualTo("SEARCH_REPLICA");
        assertThat(replicaGroup.getNodes()).hasSize(3);
    }

    @Test
    void testDiscoverGroupsWithMixedRolesInSameGroup() {
        // Given: One group with mixed roles (invalid configuration)
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.add(createNode("node-1", "mixed-group", "PRIMARY"));
        allNodes.add(createNode("node-2", "mixed-group", "SEARCH_REPLICA"));
        allNodes.add(createNode("node-3", "mixed-group", "PRIMARY"));

        // When: Should log error and skip the group to avoid killing allocation thread
        List<NodesGroup> groups = groupManager.discoverGroups(allNodes);

        // Then: Group should be skipped (not fail)
        assertThat(groups).isEmpty();
    }

    @Test
    void testDiscoverGroupsWithSingleNode() {
        // Given: A group with only 1 node
        List<SearchUnit> allNodes = createNodesForGroup("single-node-group", "SEARCH_REPLICA", 1);

        // When
        List<NodesGroup> groups = groupManager.discoverGroups(allNodes);

        // Then
        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).getNodes()).hasSize(1);
        assertThat(groups.get(0).getGroupId()).isEqualTo("single-node-group");
    }

    @Test
    void testDiscoverGroupsWithEmptyNodeList() {
        // Given
        List<SearchUnit> allNodes = List.of();

        // When
        List<NodesGroup> groups = groupManager.discoverGroups(allNodes);

        // Then
        assertThat(groups).isEmpty();
    }

    @Test
    void testFilterGroupsByRoleForReplica() {
        // Given: 2 primary groups, 3 replica groups
        List<NodesGroup> allGroups = new ArrayList<>();
        allGroups.add(createGroup("primary-1", "PRIMARY", 3));
        allGroups.add(createGroup("primary-2", "PRIMARY", 3));
        allGroups.add(createGroup("replica-1", "SEARCH_REPLICA", 3));
        allGroups.add(createGroup("replica-2", "SEARCH_REPLICA", 3));
        allGroups.add(createGroup("replica-3", "SEARCH_REPLICA", 3));

        // When
        List<NodesGroup> replicaGroups = groupManager.filterGroupsByRole(allGroups, NodeRole.REPLICA);

        // Then
        assertThat(replicaGroups).hasSize(3);
        assertThat(replicaGroups).allMatch(g -> g.getRole().equals("SEARCH_REPLICA"));
    }

    @Test
    void testFilterGroupsByRoleForPrimary() {
        // Given: 2 primary groups, 3 replica groups
        List<NodesGroup> allGroups = new ArrayList<>();
        allGroups.add(createGroup("primary-1", "PRIMARY", 3));
        allGroups.add(createGroup("primary-2", "PRIMARY", 3));
        allGroups.add(createGroup("replica-1", "SEARCH_REPLICA", 3));
        allGroups.add(createGroup("replica-2", "SEARCH_REPLICA", 3));

        // When
        List<NodesGroup> primaryGroups = groupManager.filterGroupsByRole(allGroups, NodeRole.PRIMARY);

        // Then
        assertThat(primaryGroups).hasSize(2);
        assertThat(primaryGroups).allMatch(g -> g.getRole().equals("PRIMARY"));
    }

    @Test
    void testFilterGroupsByRoleWithNoMatches() {
        // Given: Only replica groups
        List<NodesGroup> allGroups = new ArrayList<>();
        allGroups.add(createGroup("replica-1", "SEARCH_REPLICA", 3));
        allGroups.add(createGroup("replica-2", "SEARCH_REPLICA", 3));

        // When: Filter for PRIMARY
        List<NodesGroup> primaryGroups = groupManager.filterGroupsByRole(allGroups, NodeRole.PRIMARY);

        // Then
        assertThat(primaryGroups).isEmpty();
    }

    @Test
    void testFilterGroupsByRoleWithNullRole() {
        // Given: A group with null role
        NodesGroup groupWithNullRole = createGroup("null-role-group", null, 3);
        List<NodesGroup> allGroups = List.of(groupWithNullRole);

        // When
        List<NodesGroup> filtered = groupManager.filterGroupsByRole(allGroups, NodeRole.REPLICA);

        // Then
        assertThat(filtered).isEmpty();
    }

    @Test
    void testSelectGroupsDelegatesToStrategy() {
        // Given
        List<NodesGroup> availableGroups = new ArrayList<>();
        availableGroups.add(createGroup("group-1", "SEARCH_REPLICA", 3));
        availableGroups.add(createGroup("group-2", "SEARCH_REPLICA", 3));
        availableGroups.add(createGroup("group-3", "SEARCH_REPLICA", 3));
        
        List<NodesGroup> expectedSelection = List.of(availableGroups.get(0), availableGroups.get(1));
        when(mockStrategy.selectGroups(availableGroups, NodeRole.REPLICA, 2))
            .thenReturn(expectedSelection);

        // When
        List<NodesGroup> selected = groupManager.selectGroups(availableGroups, NodeRole.REPLICA, 2);

        // Then
        assertThat(selected).isEqualTo(expectedSelection);
        verify(mockStrategy).selectGroups(availableGroups, NodeRole.REPLICA, 2);
    }

    @Test
    void testSelectGroupsWithZeroGroupsNeeded() {
        // Given
        List<NodesGroup> availableGroups = new ArrayList<>();
        availableGroups.add(createGroup("group-1", "SEARCH_REPLICA", 3));
        
        when(mockStrategy.selectGroups(availableGroups, NodeRole.REPLICA, 0))
            .thenReturn(List.of());

        // When
        List<NodesGroup> selected = groupManager.selectGroups(availableGroups, NodeRole.REPLICA, 0);

        // Then
        assertThat(selected).isEmpty();
        verify(mockStrategy).selectGroups(availableGroups, NodeRole.REPLICA, 0);
    }

    @Test
    void testDiscoverGroupsSetsCurrentLoadToZero() {
        // Given
        List<SearchUnit> allNodes = createNodesForGroup("group-1", "SEARCH_REPLICA", 3);

        // When
        List<NodesGroup> groups = groupManager.discoverGroups(allNodes);

        // Then
        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).getCurrentLoad()).isEqualTo(0);
    }

    @Test
    void testDiscoverGroupsSetsNodeIds() {
        // Given
        List<SearchUnit> allNodes = createNodesForGroup("group-1", "SEARCH_REPLICA", 3);

        // When
        List<NodesGroup> groups = groupManager.discoverGroups(allNodes);

        // Then
        assertThat(groups).hasSize(1);
        NodesGroup group = groups.get(0);
        assertThat(group.getNodeIds()).hasSize(3);
        assertThat(group.getNodeIds()).containsExactlyInAnyOrder(
            "node-group-1-0", "node-group-1-1", "node-group-1-2"
        );
    }

    /**
     * Helper method to create a list of nodes for a group.
     */
    private List<SearchUnit> createNodesForGroup(String groupId, String role, int count) {
        List<SearchUnit> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(createNode("node-" + groupId + "-" + i, groupId, role));
        }
        return nodes;
    }

    /**
     * Helper method to create a single SearchUnit node.
     */
    private SearchUnit createNode(String name, String shardId, String role) {
        SearchUnit node = new SearchUnit();
        node.setId(name);
        node.setName(name);
        node.setShardId(shardId); // GROUP ID = shardId
        node.setRole(role);
        node.setHost("localhost");
        node.setPortHttp(9200);
        return node;
    }

    /**
     * Helper method to create a NodesGroup.
     */
    private NodesGroup createGroup(String groupId, String role, int nodeCount) {
        NodesGroup group = new NodesGroup();
        group.setGroupId(groupId);
        group.setRole(role);
        group.setShardId(groupId);
        
        List<SearchUnit> nodes = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            SearchUnit node = new SearchUnit();
            node.setId(groupId + "-node-" + i);
            node.setName(groupId + "-node-" + i);
            node.setShardId(groupId);
            node.setRole(role);
            nodes.add(node);
        }
        group.setNodes(nodes);
        group.setCurrentLoad(0);
        
        return group;
    }
}

