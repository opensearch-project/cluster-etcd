package io.clustercontroller.allocation;

import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.ShardAllocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for GroupAwareBinPackingEngine.
 * 
 * Tests stable allocation, scale-up, downscaling warning, and group selection.
 */
class GroupAwareBinPackingEngineTest {

    private GroupAwareBinPackingEngine engine;

    @BeforeEach
    void setUp() {
        engine = new GroupAwareBinPackingEngine();
    }

    @Test
    void testStableAllocation_AlreadyAllocatedToCorrectNumberOfGroups() {
        // Given: Index config requires 2 groups for shard 0
        Index indexConfig = createIndexWithGroupCount(List.of(2));
        
        // Given: Planned allocation shows nodes from 2 groups (group-1, group-2)
        ShardAllocation currentPlanned = createPlannedAllocation(
            List.of("node-group-1-0", "node-group-1-1", "node-group-2-0", "node-group-2-1")
        );
        
        // Given: All nodes including 3 groups
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createHealthyNodesForGroup("group-1", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-2", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-3", "SEARCH_REPLICA", 3));

        // When: Allocation engine runs
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", indexConfig, allNodes, NodeRole.REPLICA, currentPlanned
        );

        // Then: Should return nodes from the existing 2 groups (stable allocation!)
        assertThat(result).isNotEmpty();
        List<String> resultGroupIds = result.stream()
            .map(SearchUnit::getShardId)
            .distinct()
            .toList();
        assertThat(resultGroupIds).containsExactlyInAnyOrder("group-1", "group-2");
    }

    @Test
    void testScaleUp_AddNewGroupWhenCurrentIsLessThanDesired() {
        // Given: Index config requires 2 groups for shard 0
        Index indexConfig = createIndexWithGroupCount(List.of(2));
        
        // Given: Planned allocation shows nodes from only 1 group (group-1)
        ShardAllocation currentPlanned = createPlannedAllocation(
            List.of("node-group-1-0", "node-group-1-1")
        );
        
        // Given: All nodes including 3 groups
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createHealthyNodesForGroup("group-1", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-2", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-3", "SEARCH_REPLICA", 3));

        // When: Allocation engine runs
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", indexConfig, allNodes, NodeRole.REPLICA, currentPlanned
        );

        // Then: Should return nodes from 2 groups (group-1 + one additional group)
        assertThat(result).isNotEmpty();
        List<String> resultGroupIds = result.stream()
            .map(SearchUnit::getShardId)
            .distinct()
            .toList();
        assertThat(resultGroupIds).hasSize(2);
        assertThat(resultGroupIds).contains("group-1"); // Existing group retained
        // One new group selected (group-2 or group-3)
    }

    @Test
    void testDownscaling_LogWarningAndKeepCurrentAllocation() {
        // Given: Index config requires only 1 group for shard 0
        Index indexConfig = createIndexWithGroupCount(List.of(1));
        
        // Given: Planned allocation shows nodes from 2 groups (group-1, group-2)
        ShardAllocation currentPlanned = createPlannedAllocation(
            List.of("node-group-1-0", "node-group-1-1", "node-group-2-0", "node-group-2-1")
        );
        
        // Given: All nodes including 3 groups
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createHealthyNodesForGroup("group-1", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-2", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-3", "SEARCH_REPLICA", 3));

        // When: Allocation engine runs (should log warning about downscaling not supported)
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", indexConfig, allNodes, NodeRole.REPLICA, currentPlanned
        );

        // Then: Should keep current allocation (both groups)
        assertThat(result).isNotEmpty();
        List<String> resultGroupIds = result.stream()
            .map(SearchUnit::getShardId)
            .distinct()
            .toList();
        assertThat(resultGroupIds).containsExactlyInAnyOrder("group-1", "group-2");
    }

    @Test
    void testInitialAllocation_NoPlannedAllocation() {
        // Given: Index config requires 2 groups for shard 0
        Index indexConfig = createIndexWithGroupCount(List.of(2));
        
        // Given: No planned allocation (null)
        ShardAllocation currentPlanned = null;
        
        // Given: All nodes including 3 groups
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createHealthyNodesForGroup("group-1", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-2", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-3", "SEARCH_REPLICA", 3));

        // When: Allocation engine runs (initial allocation)
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", indexConfig, allNodes, NodeRole.REPLICA, currentPlanned
        );

        // Then: Should select 2 groups randomly
        assertThat(result).isNotEmpty();
        List<String> resultGroupIds = result.stream()
            .map(SearchUnit::getShardId)
            .distinct()
            .toList();
        assertThat(resultGroupIds).hasSize(2);
    }

    @Test
    void testInitialAllocation_EmptyPlannedAllocation() {
        // Given: Index config requires 2 groups for shard 0
        Index indexConfig = createIndexWithGroupCount(List.of(2));
        
        // Given: Empty planned allocation
        ShardAllocation currentPlanned = new ShardAllocation();
        currentPlanned.setSearchSUs(List.of());
        
        // Given: All nodes including 3 groups
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createHealthyNodesForGroup("group-1", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-2", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-3", "SEARCH_REPLICA", 3));

        // When: Allocation engine runs
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", indexConfig, allNodes, NodeRole.REPLICA, currentPlanned
        );

        // Then: Should select 2 groups
        assertThat(result).isNotEmpty();
        List<String> resultGroupIds = result.stream()
            .map(SearchUnit::getShardId)
            .distinct()
            .toList();
        assertThat(resultGroupIds).hasSize(2);
    }

    @Test
    void testReplicaAllocation_IncludesAllNodesFromDiscovery() {
        // Given: Index config requires 1 group for shard 0
        Index indexConfig = createIndexWithGroupCount(List.of(1));
        
        // Given: No planned allocation
        ShardAllocation currentPlanned = null;
        
        // Given: group-1 has 2 GREEN nodes and 1 RED node
        // ALL nodes were found by Discovery → ALL should be allocated
        // (Discovery already filtered truly dead/unreachable nodes)
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.add(createHealthyNode("node-group-1-0", "group-1", "SEARCH_REPLICA"));
        allNodes.add(createHealthyNode("node-group-1-1", "group-1", "SEARCH_REPLICA"));
        allNodes.add(createUnhealthyNode("node-group-1-2", "group-1", "SEARCH_REPLICA"));

        // When: Allocation engine runs
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", indexConfig, allNodes, NodeRole.REPLICA, currentPlanned
        );

        // Then: Should return ALL nodes from group-1 (including RED node)
        // If Discovery found it → it's available → allocate to it
        assertThat(result).hasSize(3);
        assertThat(result).extracting(SearchUnit::getName)
            .containsExactlyInAnyOrder("node-group-1-0", "node-group-1-1", "node-group-1-2");
    }

    @Test
    void testReplicaAllocation_NoReplicaGroupsAvailable() {
        // Given: Index config requires 1 group for shard 0
        Index indexConfig = createIndexWithGroupCount(List.of(1));
        
        // Given: No planned allocation
        ShardAllocation currentPlanned = null;
        
        // Given: Only PRIMARY groups available (no REPLICA groups)
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createHealthyNodesForGroup("primary-group-1", "PRIMARY", 3));
        allNodes.addAll(createHealthyNodesForGroup("primary-group-2", "PRIMARY", 3));

        // When: Allocation engine runs
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", indexConfig, allNodes, NodeRole.REPLICA, currentPlanned
        );

        // Then: Should return empty (no REPLICA groups)
        assertThat(result).isEmpty();
    }

    @Test
    void testReplicaAllocation_NoGroupsDiscovered() {
        // Given: Index config requires 1 group for shard 0
        Index indexConfig = createIndexWithGroupCount(List.of(1));
        
        // Given: No planned allocation
        ShardAllocation currentPlanned = null;
        
        // Given: Empty node list
        List<SearchUnit> allNodes = List.of();

        // When: Allocation engine runs
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", indexConfig, allNodes, NodeRole.REPLICA, currentPlanned
        );

        // Then: Should return empty
        assertThat(result).isEmpty();
    }

    @Test
    void testPrimaryAllocation_UsesStandardDeciders() {
        // Given: PRIMARY nodes in different groups (NO SHARD AFFINITY with bin-packing!)
        // Group "0": primary-node-1, primary-node-2
        // Group "1": primary-node-3
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.add(createPrimaryNode("primary-node-1", "0", HealthState.GREEN)); // Group "0"
        allNodes.add(createPrimaryNode("primary-node-2", "0", HealthState.GREEN)); // Group "0"
        allNodes.add(createPrimaryNode("primary-node-3", "1", HealthState.GREEN)); // Group "1"

        // When: Allocation engine runs for PRIMARY shard 0 (defaults to single-writer = 1 ingester)
        // With bin-packing: randomly selects 1 group, then 1 node from that group
        // NO SHARD AFFINITY - can pick from ANY group!
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", null, allNodes, NodeRole.PRIMARY, null
        );

        // Then: Should return 1 PRIMARY node (single-writer default)
        // Can be from ANY group (no shard affinity!)
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getName())
            .as("Bin-packing: Can select from ANY PRIMARY group (no shard affinity)")
            .isIn("primary-node-1", "primary-node-2", "primary-node-3");  // Any PRIMARY node!
    }

    @Test
    void testPrimaryAllocation_FiltersUnhealthyNodes() {
        // Given: Primary nodes with mixed health states
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.add(createPrimaryNode("primary-node-1", "0", HealthState.GREEN)); // Healthy
        allNodes.add(createPrimaryNode("primary-node-2", "0", HealthState.RED)); // Unhealthy

        // When: Allocation engine runs for PRIMARY shard 0
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", null, allNodes, NodeRole.PRIMARY, null
        );

        // Then: Should filter out unhealthy nodes
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getName()).isEqualTo("primary-node-1");
    }

    @Test
    void testScaleUp_AllGroupsAlreadyAllocated() {
        // Given: Index config requires 3 groups for shard 0
        Index indexConfig = createIndexWithGroupCount(List.of(3));
        
        // Given: Planned allocation shows 2 groups already allocated
        ShardAllocation currentPlanned = createPlannedAllocation(
            List.of("node-group-1-0", "node-group-2-0")
        );
        
        // Given: Only 2 groups exist in total
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createHealthyNodesForGroup("group-1", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-2", "SEARCH_REPLICA", 3));

        // When: Allocation engine runs (tries to scale up but no more groups available)
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", indexConfig, allNodes, NodeRole.REPLICA, currentPlanned
        );

        // Then: Should return nodes from existing 2 groups (can't scale up further)
        assertThat(result).isNotEmpty();
        List<String> resultGroupIds = result.stream()
            .map(SearchUnit::getShardId)
            .distinct()
            .toList();
        assertThat(resultGroupIds).containsExactlyInAnyOrder("group-1", "group-2");
    }

    @Test
    void testReplicaAllocation_DefaultGroupCountIsOne() {
        // Given: Index config with null shardGroupsAllocateCount (defaults to 1)
        Index indexConfig = new Index();
        IndexSettings settings = new IndexSettings();
        settings.setNumGroupsPerShard(null); // Default should be 1
        indexConfig.setSettings(settings);
        
        // Given: No planned allocation
        ShardAllocation currentPlanned = null;
        
        // Given: 2 replica groups available
        List<SearchUnit> allNodes = new ArrayList<>();
        allNodes.addAll(createHealthyNodesForGroup("group-1", "SEARCH_REPLICA", 3));
        allNodes.addAll(createHealthyNodesForGroup("group-2", "SEARCH_REPLICA", 3));

        // When: Allocation engine runs
        List<SearchUnit> result = engine.getAvailableNodesForAllocation(
            0, "test-index", indexConfig, allNodes, NodeRole.REPLICA, currentPlanned
        );

        // Then: Should select 1 group (default)
        assertThat(result).isNotEmpty();
        List<String> resultGroupIds = result.stream()
            .map(SearchUnit::getShardId)
            .distinct()
            .toList();
        assertThat(resultGroupIds).hasSize(1);
    }

    /**
     * Helper: Create an Index with specific shard group counts.
     */
    private Index createIndexWithGroupCount(List<Integer> groupCounts) {
        Index index = new Index();
        IndexSettings settings = new IndexSettings();
        settings.setNumGroupsPerShard(groupCounts);
        index.setSettings(settings);
        return index;
    }

    /**
     * Helper: Create a planned allocation with specific node names.
     */
    private ShardAllocation createPlannedAllocation(List<String> searchSUs) {
        ShardAllocation allocation = new ShardAllocation();
        allocation.setSearchSUs(searchSUs);
        return allocation;
    }

    /**
     * Helper: Create healthy nodes for a group.
     */
    private List<SearchUnit> createHealthyNodesForGroup(String groupId, String role, int count) {
        List<SearchUnit> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodes.add(createHealthyNode("node-" + groupId + "-" + i, groupId, role));
        }
        return nodes;
    }

    /**
     * Helper: Create a healthy SearchUnit node.
     */
    private SearchUnit createHealthyNode(String name, String shardId, String role) {
        SearchUnit node = new SearchUnit();
        node.setId(name);
        node.setName(name);
        node.setShardId(shardId); // GROUP ID = shardId
        node.setRole(role);
        node.setHost("localhost");
        node.setPortHttp(9200);
        node.setStatePulled(HealthState.GREEN);
        node.setStateAdmin("NORMAL");
        return node;
    }

    /**
     * Helper: Create an unhealthy SearchUnit node.
     */
    private SearchUnit createUnhealthyNode(String name, String shardId, String role) {
        SearchUnit node = createHealthyNode(name, shardId, role);
        node.setStatePulled(HealthState.RED); // Unhealthy
        return node;
    }

    /**
     * Helper: Create a primary node.
     */
    private SearchUnit createPrimaryNode(String name, String shardId, HealthState health) {
        SearchUnit node = new SearchUnit();
        node.setId(name);
        node.setName(name);
        node.setShardId(shardId); // Shard pool
        node.setRole("PRIMARY");
        node.setHost("localhost");
        node.setPortHttp(9200);
        node.setStatePulled(health);
        node.setStateAdmin("NORMAL");
        return node;
    }
}

