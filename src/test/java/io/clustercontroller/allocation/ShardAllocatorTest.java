package io.clustercontroller.allocation;

import io.clustercontroller.allocation.AllocationDecisionEngine;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive tests for ShardAllocator functionality.
 */
@ExtendWith(MockitoExtension.class)
class ShardAllocatorTest {

    @Mock
    private MetadataStore metadataStore;

    @Mock
    private AllocationDecisionEngine allocationDecisionEngine;

    private ShardAllocator shardAllocator;

    private final String testClusterId = "test-cluster";
    private final String testIndexName = "test-index";
    private final String testShardId = "0";

    @BeforeEach
    void setUp() {
        shardAllocator = new ShardAllocator(metadataStore);
        // Inject the mock decision engine for testing
        shardAllocator.setAllocationDecisionEngine(allocationDecisionEngine);
    }

    @Test
    void testPlanShardAllocationWithNoIndexConfigs() throws Exception {
        // Given
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Collections.emptyList());

        // When
        shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.RESPECT_REPLICA_COUNT);

        // Then
        verify(metadataStore).getAllIndexConfigs(testClusterId);
        verifyNoMoreInteractions(metadataStore);
    }

    @Test
    void testPlanShardAllocationWithIndexConfigs() throws Exception {
        // Given
        List<Index> indexConfigs = Arrays.asList(
            createIndex("index1", Arrays.asList(1)),
            createIndex("index2", Arrays.asList(1))
        );
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(indexConfigs);
        
        // Mock no existing planned allocations
        when(metadataStore.getPlannedAllocation(anyString(), anyString(), anyString())).thenReturn(null);
        
        // Mock eligible nodes
        List<SearchUnit> eligibleIngestNodes = Arrays.asList(createSearchUnit("node1", "PRIMARY"));
        List<SearchUnit> eligibleSearchNodes = Arrays.asList(
            createSearchUnit("node2", "REPLICA"), 
            createSearchUnit("node3", "REPLICA")
        );
        when(allocationDecisionEngine.getAvailableNodesForAllocation(anyInt(), anyString(), any(), anyList(), any(), any()))
            .thenReturn(eligibleIngestNodes, eligibleSearchNodes);

        // When
        shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);

        // Then
        verify(metadataStore).getAllIndexConfigs(testClusterId);
        verify(metadataStore, atLeastOnce()).getPlannedAllocation(anyString(), anyString(), anyString());
        verify(metadataStore, atLeastOnce()).setPlannedAllocation(anyString(), anyString(), anyString(), any(ShardAllocation.class));
    }

    @Test
    void testPlanShardAllocationWithRecentAllocation() throws Exception {
        // Given
        List<Index> indexConfigs = Arrays.asList(createIndex("index1", Arrays.asList(1)));
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(indexConfigs);
        
        // Mock recent planned allocation (within 5 minutes)
        ShardAllocation recentAllocation = new ShardAllocation("0", "index1");
        recentAllocation.setAllocationTimestamp(System.currentTimeMillis() - Duration.ofMinutes(2).toMillis());
        recentAllocation.setIngestSUs(Arrays.asList("node1"));
        recentAllocation.setSearchSUs(Arrays.asList("node2", "node3"));
        when(metadataStore.getPlannedAllocation(anyString(), anyString(), anyString())).thenReturn(recentAllocation);

        // Mock eligible search nodes for SearchSU allocation (different from current)
        List<SearchUnit> eligibleSearchNodes = Arrays.asList(
            createSearchUnit("node4", "REPLICA"), 
            createSearchUnit("node5", "REPLICA")
        );
        when(allocationDecisionEngine.getAvailableNodesForAllocation(anyInt(), anyString(), any(), anyList(), any(), any()))
            .thenReturn(eligibleSearchNodes);

        // When
        shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.RESPECT_REPLICA_COUNT);

        // Then - Should update planned allocation with new SearchSUs even with recent allocation
        verify(metadataStore).getAllIndexConfigs(testClusterId);
        verify(metadataStore, atLeastOnce()).getPlannedAllocation(anyString(), anyString(), anyString());
        verify(metadataStore).setPlannedAllocation(anyString(), anyString(), anyString(), argThat(allocation -> {
            ShardAllocation shardAllocation = (ShardAllocation) allocation;
            // Should have SearchSUs allocated (may be different from original due to strategy)
            return !shardAllocation.getSearchSUs().isEmpty();
        }));
    }

    @Test
    void testPlanShardAllocationWithMultipleIngestSUsError() throws Exception {
        // Given
        List<Index> indexConfigs = Arrays.asList(createIndex("index1", Arrays.asList(1)));
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(indexConfigs);
        
        // Mock no existing planned allocation
        when(metadataStore.getPlannedAllocation(anyString(), anyString(), anyString())).thenReturn(null);
        
        // Mock multiple eligible ingest nodes (should cause error) and valid search nodes
        List<SearchUnit> eligibleIngestNodes = Arrays.asList(
            createSearchUnit("node1", "PRIMARY"), 
            createSearchUnit("node2", "PRIMARY")
        );
        List<SearchUnit> eligibleSearchNodes = Arrays.asList(createSearchUnit("node3", "REPLICA"));
        when(allocationDecisionEngine.getAvailableNodesForAllocation(anyInt(), anyString(), any(), anyList(), any(), any()))
            .thenReturn(eligibleIngestNodes, eligibleSearchNodes);

        // When
        shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.RESPECT_REPLICA_COUNT);

        // Then - Should update planned allocation with SearchSUs even if IngestSU allocation fails
        verify(metadataStore).getAllIndexConfigs(testClusterId);
        verify(metadataStore, atLeastOnce()).getPlannedAllocation(anyString(), anyString(), anyString());
        verify(metadataStore).setPlannedAllocation(anyString(), anyString(), anyString(), argThat(allocation -> {
            ShardAllocation shardAllocation = (ShardAllocation) allocation;
            // IngestSUs should be empty/null due to multiple IngestSUs error, but SearchSUs should be present
            return (shardAllocation.getIngestSUs() == null || shardAllocation.getIngestSUs().isEmpty()) &&
                   !shardAllocation.getSearchSUs().isEmpty();
        }));
    }

    @Test
    void testPlanShardAllocationWithCurrentMultipleIngestSUsError() throws Exception {
        // Given
        List<Index> indexConfigs = Arrays.asList(createIndex("index1", Arrays.asList(1)));
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(indexConfigs);
        
        // Mock existing planned allocation with multiple IngestSUs (should cause error)
        ShardAllocation currentAllocation = new ShardAllocation("0", "index1");
        currentAllocation.setIngestSUs(Arrays.asList("node1", "node2")); // Multiple IngestSUs!
        currentAllocation.setSearchSUs(Arrays.asList("node3"));
        when(metadataStore.getPlannedAllocation(anyString(), anyString(), anyString())).thenReturn(currentAllocation);

        // Mock valid search nodes for SearchSU allocation
        List<SearchUnit> eligibleSearchNodes = Arrays.asList(createSearchUnit("node4", "REPLICA"));
        when(allocationDecisionEngine.getAvailableNodesForAllocation(anyInt(), anyString(), any(), anyList(), any(), any()))
            .thenReturn(eligibleSearchNodes);

        // When
        shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.RESPECT_REPLICA_COUNT);

        // Then - Should update planned allocation with SearchSUs even if current IngestSU allocation has multiple nodes
        verify(metadataStore).getAllIndexConfigs(testClusterId);
        verify(metadataStore, atLeastOnce()).getPlannedAllocation(anyString(), anyString(), anyString());
        verify(metadataStore).setPlannedAllocation(anyString(), anyString(), anyString(), argThat(allocation -> {
            ShardAllocation shardAllocation = (ShardAllocation) allocation;
            // IngestSUs should be empty/null due to multiple IngestSUs error, but SearchSUs should be present
            return (shardAllocation.getIngestSUs() == null || shardAllocation.getIngestSUs().isEmpty()) &&
                   !shardAllocation.getSearchSUs().isEmpty();
        }));
    }

    @Test
    void testPlanShardAllocationWithValidIngestAllocation() throws Exception {
        // Given
        List<Index> indexConfigs = Arrays.asList(createIndex("index1", Arrays.asList(1)));
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(indexConfigs);
        
        // Mock no existing planned allocation
        when(metadataStore.getPlannedAllocation(anyString(), anyString(), anyString())).thenReturn(null);
        
        // Mock single eligible ingest node
        List<SearchUnit> eligibleIngestNodes = Arrays.asList(createSearchUnit("node1", "PRIMARY"));
        List<SearchUnit> eligibleSearchNodes = Arrays.asList(
            createSearchUnit("node2", "REPLICA"), 
            createSearchUnit("node3", "REPLICA")
        );
        when(allocationDecisionEngine.getAvailableNodesForAllocation(anyInt(), anyString(), any(), anyList(), any(), any()))
            .thenReturn(eligibleIngestNodes, eligibleSearchNodes);

        // When
        shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);

        // Then - Should update planned allocation
        verify(metadataStore).getAllIndexConfigs(testClusterId);
        verify(metadataStore, atLeastOnce()).getPlannedAllocation(anyString(), anyString(), anyString());
        verify(metadataStore).setPlannedAllocation(anyString(), anyString(), anyString(), argThat(allocation -> {
            ShardAllocation shardAllocation = (ShardAllocation) allocation;
            return shardAllocation.getIngestSUs().size() == 1 && 
                   shardAllocation.getIngestSUs().contains("node1") &&
                   shardAllocation.getSearchSUs().size() == 2 &&
                   shardAllocation.getSearchSUs().containsAll(Arrays.asList("node2", "node3"));
        }));
    }

    @Test
    void testReallocateShard() throws Exception {
        // Given
        List<String> targetNodes = Arrays.asList("node1", "node2");

        // When & Then - Should not throw exception (method is TODO placeholder)
        assertThatCode(() -> shardAllocator.reallocateShard(testClusterId, testIndexName, testShardId, targetNodes))
            .doesNotThrowAnyException();
    }

    @Test
    void testPlanShardAllocationHandlesException() throws Exception {
        // Given
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenThrow(new RuntimeException("Database error"));

        // When & Then - Should not throw exception
        assertThatCode(() -> shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.RESPECT_REPLICA_COUNT))
            .doesNotThrowAnyException();

        verify(metadataStore).getAllIndexConfigs(testClusterId);
    }

    @Test
    void testPlanShardAllocationWithDifferentStrategies() throws Exception {
        // Given
        List<Index> indexConfigs = Arrays.asList(createIndex("index1", Arrays.asList(1)));
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(indexConfigs);
        when(metadataStore.getPlannedAllocation(anyString(), anyString(), anyString())).thenReturn(null);
        
        List<SearchUnit> eligibleIngestNodes = Arrays.asList(createSearchUnit("node1", "PRIMARY"));
        List<SearchUnit> eligibleSearchNodes = Arrays.asList(
            createSearchUnit("node2", "REPLICA"), 
            createSearchUnit("node3", "REPLICA")
        );
        when(allocationDecisionEngine.getAvailableNodesForAllocation(anyInt(), anyString(), any(), anyList(), any(), any()))
            .thenReturn(eligibleIngestNodes, eligibleSearchNodes);

        // When - Test both strategies
        shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.RESPECT_REPLICA_COUNT);
        shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);

        // Then - Both should work
        verify(metadataStore, times(2)).getAllIndexConfigs(testClusterId);
        verify(metadataStore, atLeastOnce()).setPlannedAllocation(anyString(), anyString(), anyString(), any(ShardAllocation.class));
    }

    @Test
    void testPlanShardAllocationWithMultipleIndexesAndShards() throws Exception {
        // Given - Multiple indexes with different shard counts
        List<Index> indexConfigs = Arrays.asList(
            createIndex("index1", Arrays.asList(2, 1)), // 2 shards: shard0 with 2 replicas, shard1 with 1 replica
            createIndex("index2", Arrays.asList(1)),    // 1 shard: shard0 with 1 replica
            createIndex("index3", Arrays.asList(3, 2, 1)) // 3 shards: shard0 with 3 replicas, shard1 with 2 replicas, shard2 with 1 replica
        );
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(indexConfigs);
        
        // Mock no existing planned allocations
        when(metadataStore.getPlannedAllocation(anyString(), anyString(), anyString())).thenReturn(null);
        
        // Mock eligible nodes for different scenarios
        List<SearchUnit> eligibleIngestNodes = Arrays.asList(createSearchUnit("node1", "PRIMARY"));
        List<SearchUnit> eligibleSearchNodes = Arrays.asList(
            createSearchUnit("node2", "REPLICA"), 
            createSearchUnit("node3", "REPLICA"),
            createSearchUnit("node4", "REPLICA"),
            createSearchUnit("node5", "REPLICA")
        );
        
        // Return different node sets for different calls to simulate different shard allocations
        // Total shards: index1(2) + index2(1) + index3(3) = 6 shards
        when(allocationDecisionEngine.getAvailableNodesForAllocation(anyInt(), anyString(), any(), anyList(), any(), any()))
            .thenReturn(eligibleIngestNodes, eligibleSearchNodes, // index1/shard0
                       eligibleIngestNodes, eligibleSearchNodes, // index1/shard1
                       eligibleIngestNodes, eligibleSearchNodes, // index2/shard0
                       eligibleIngestNodes, eligibleSearchNodes, // index3/shard0
                       eligibleIngestNodes, eligibleSearchNodes, // index3/shard1
                       eligibleIngestNodes, eligibleSearchNodes); // index3/shard2

        // When
        shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);

        // Then - Should process all indexes and shards
        verify(metadataStore).getAllIndexConfigs(testClusterId);
        
        // Should call getPlannedAllocation for each shard (6 shards * 2 calls per shard = 12 calls)
        // Each shard calls getPlannedAllocation once in main loop and once in isRecentAllocation
        verify(metadataStore, times(12)).getPlannedAllocation(anyString(), anyString(), anyString());
        
        // Should call setPlannedAllocation for each shard (6 calls)
        verify(metadataStore, times(6)).setPlannedAllocation(anyString(), anyString(), anyString(), any(ShardAllocation.class));
        
        // Verify specific index/shard combinations were processed (each called twice: main loop + isRecentAllocation)
        verify(metadataStore, times(2)).getPlannedAllocation(testClusterId, "index1", "0");
        verify(metadataStore, times(2)).getPlannedAllocation(testClusterId, "index1", "1");
        verify(metadataStore, times(2)).getPlannedAllocation(testClusterId, "index2", "0");
        verify(metadataStore, times(2)).getPlannedAllocation(testClusterId, "index3", "0");
        verify(metadataStore, times(2)).getPlannedAllocation(testClusterId, "index3", "1");
        verify(metadataStore, times(2)).getPlannedAllocation(testClusterId, "index3", "2");
    }

    private SearchUnit createSearchUnit(String nodeId, String role) {
        SearchUnit searchUnit = new SearchUnit();
        searchUnit.setId(nodeId);
        searchUnit.setName(nodeId);
        searchUnit.setRole(role);
        searchUnit.setStatePulled(HealthState.GREEN);
        return searchUnit;
    }
    
    private Index createIndex(String indexName, List<Integer> shardReplicaCount) {
        Index index = new Index();
        index.setIndexName(indexName);
        index.setSettings(new io.clustercontroller.models.IndexSettings());
        index.getSettings().setShardReplicaCount(shardReplicaCount);
        index.getSettings().setNumberOfShards(shardReplicaCount.size());
        return index;
    }

    // ========== E2E Tests with Real StandardAllocationEngine ==========

    @Test
    void testE2E_UseAllAvailableNodes_IgnoresReplicaCount() throws Exception {
        // Given: Create ShardAllocator with REAL StandardAllocationEngine (not mocked!)
        ShardAllocator e2eShardAllocator = new ShardAllocator(metadataStore);
        // DON'T call setAllocationDecisionEngine - use default StandardAllocationEngine
        
        // Given: Index config with 2 replicas configured
        Index index = createIndex("e2e-index", Arrays.asList(2));  // Config says 2 replicas
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        
        // Given: No existing planned allocation
        when(metadataStore.getPlannedAllocation(testClusterId, "e2e-index", "0")).thenReturn(null);
        
        // Given: Mix of nodes - 3 healthy replicas available (more than config)
        // USE_ALL_AVAILABLE_NODES should allocate ALL healthy nodes, ignoring replica count config
        List<SearchUnit> allNodes = Arrays.asList(
            createHealthyPrimaryNode("primary-0-1", "0"),  // ✓ Should be selected for PRIMARY (only one!)
            createHealthyPrimaryNode("primary-1-1", "1"),  // ✗ Wrong shard pool
            createUnhealthyPrimaryNode("primary-0-bad", "0"), // ✗ Unhealthy
            createHealthyReplicaNode("replica-0-1", "0"),  // ✓ Should be selected (1/3 healthy)
            createHealthyReplicaNode("replica-0-2", "0"),  // ✓ Should be selected (2/3 healthy)
            createHealthyReplicaNode("replica-0-3", "0"),  // ✓ Should be selected (3/3 healthy)
            createHealthyReplicaNode("replica-1-1", "1"),  // ✗ Wrong shard pool
            createUnhealthyReplicaNode("replica-0-bad", "0") // ✗ Unhealthy
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation with USE_ALL_AVAILABLE_NODES strategy
        e2eShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Verify USE_ALL_AVAILABLE_NODES allocates ALL healthy nodes (ignores replica count)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("e2e-index"),
            eq("0"),
            argThat(allocation -> {
                // Verify IngestSUs: Must be exactly 1 (single writer constraint)
                assertThat(allocation.getIngestSUs())
                    .as("IngestSUs must be exactly 1 per shard (single writer constraint)")
                    .hasSize(1)
                    .containsExactly("primary-0-1");
                
                // Verify SearchSUs: Should allocate ALL 3 healthy replicas (ignoring config of 2)
                // This is the expected behavior of USE_ALL_AVAILABLE_NODES strategy
                assertThat(allocation.getSearchSUs())
                    .as("USE_ALL_AVAILABLE_NODES should allocate all 3 healthy nodes, ignoring replica count config of 2")
                    .hasSize(3)
                    .containsExactlyInAnyOrder("replica-0-1", "replica-0-2", "replica-0-3");
                
                return true;
            })
        );
    }

    @Test
    void testE2E_StandardAllocationEngine_RespectsReplicaCount() throws Exception {
        // Given: ShardAllocator with real StandardAllocationEngine
        ShardAllocator e2eShardAllocator = new ShardAllocator(metadataStore);
        
        // Given: Index config with replica count = 2
        Index index = createIndex("e2e-index-2", Arrays.asList(2));  // Config says 2 replicas
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        when(metadataStore.getPlannedAllocation(testClusterId, "e2e-index-2", "0")).thenReturn(null);
        
        // Given: 4 healthy replica nodes available (more than config)
        // RESPECT_REPLICA_COUNT should allocate ONLY 2 nodes, respecting the config
        List<SearchUnit> allNodes = Arrays.asList(
            createHealthyPrimaryNode("primary-1", "0"),
            createHealthyReplicaNode("replica-1", "0"),
            createHealthyReplicaNode("replica-2", "0"),
            createHealthyReplicaNode("replica-3", "0"),
            createHealthyReplicaNode("replica-4", "0")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation with RESPECT_REPLICA_COUNT strategy
        e2eShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.RESPECT_REPLICA_COUNT);
        
        // Then: Should allocate exactly 2 replicas (respecting config, not all 4 available)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("e2e-index-2"),
            eq("0"),
            argThat(allocation -> {
                // Verify IngestSUs: Must be exactly 1 (single writer constraint)
                assertThat(allocation.getIngestSUs())
                    .as("IngestSUs must be exactly 1 per shard (single writer constraint)")
                    .hasSize(1);
                
                // Verify SearchSUs: Should allocate exactly 2 replicas (respecting config, NOT all 4 available)
                // This is the expected behavior of RESPECT_REPLICA_COUNT strategy
                assertThat(allocation.getSearchSUs())
                    .as("RESPECT_REPLICA_COUNT should allocate exactly 2 nodes as configured, ignoring extra available nodes")
                    .hasSize(2);
                return true;
            })
        );
    }

    @Test
    void testE2E_StandardAllocationEngine_FiltersUnhealthyNodes() throws Exception {
        // Given: ShardAllocator with real StandardAllocationEngine
        ShardAllocator e2eShardAllocator = new ShardAllocator(metadataStore);
        
        // Given: Index config
        Index index = createIndex("e2e-index-3", Arrays.asList(1));
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        when(metadataStore.getPlannedAllocation(testClusterId, "e2e-index-3", "0")).thenReturn(null);
        
        // Given: Mix of healthy and unhealthy nodes
        List<SearchUnit> allNodes = Arrays.asList(
            createHealthyPrimaryNode("healthy-primary", "0"),
            createUnhealthyPrimaryNode("unhealthy-primary", "0"),
            createHealthyReplicaNode("healthy-replica-1", "0"),
            createHealthyReplicaNode("healthy-replica-2", "0"),
            createUnhealthyReplicaNode("unhealthy-replica-1", "0"),
            createUnhealthyReplicaNode("unhealthy-replica-2", "0")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation
        e2eShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Should only allocate to healthy nodes
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("e2e-index-3"),
            eq("0"),
            argThat(allocation -> {
                // Verify IngestSUs: Must be exactly 1 (single writer constraint) and only healthy
                assertThat(allocation.getIngestSUs())
                    .as("IngestSUs must be exactly 1 per shard (single writer constraint)")
                    .hasSize(1)
                    .containsExactly("healthy-primary");
                
                // Only healthy replicas
                assertThat(allocation.getSearchSUs())
                    .containsExactlyInAnyOrder("healthy-replica-1", "healthy-replica-2");
                
                return true;
            })
        );
    }


    // Helper methods for E2E tests
    private SearchUnit createHealthyPrimaryNode(String name, String shardId) {
        SearchUnit node = new SearchUnit();
        node.setId(name);
        node.setName(name);
        node.setRole("PRIMARY");
        node.setShardId(shardId);
        node.setStatePulled(HealthState.GREEN);
        node.setStateAdmin("NORMAL");
        node.setHost("localhost");
        return node;
    }

    private SearchUnit createUnhealthyPrimaryNode(String name, String shardId) {
        SearchUnit node = createHealthyPrimaryNode(name, shardId);
        node.setStatePulled(HealthState.RED);
        return node;
    }

    private SearchUnit createHealthyReplicaNode(String name, String shardId) {
        SearchUnit node = new SearchUnit();
        node.setId(name);
        node.setName(name);
        node.setRole("SEARCH_REPLICA");
        node.setShardId(shardId);
        node.setStatePulled(HealthState.GREEN);
        node.setStateAdmin("NORMAL");
        node.setHost("localhost");
        return node;
    }

    private SearchUnit createUnhealthyReplicaNode(String name, String shardId) {
        SearchUnit node = createHealthyReplicaNode(name, shardId);
        node.setStatePulled(HealthState.RED);
        return node;
    }

    // ========== E2E Bin-Packing Tests with Real GroupAwareBinPackingEngine ==========
    //
    // Key bin-packing behavior with USE_ALL_AVAILABLE_NODES strategy:
    // 1. GROUP is the allocation unit (not individual replicas)
    // 2. shardGroupsAllocateCount controls HOW MANY groups to select
    // 3. shardReplicaCount is IGNORED by USE_ALL_AVAILABLE_NODES strategy
    // 4. ALL healthy nodes from selected groups are used (entire group capacity)
    // 5. Once groups are selected, allocation is STABLE (no unnecessary movement)
    //
    // Example: replica count = 1, groups = 2, nodes per group = 3
    //   → Allocates to 2 groups × 3 nodes = 6 total nodes (not 1!)

    @Test
    void testE2E_BinPacking_StableAllocation() throws Exception {
        // Given: ShardAllocator with REAL GroupAwareBinPackingEngine
        ShardAllocator binPackingShardAllocator = new ShardAllocator(metadataStore);
        binPackingShardAllocator.setAllocationDecisionEngine(new GroupAwareBinPackingEngine());
        
        // Given: Index configured for 2 groups per shard
        // Replica count = 1 (will be IGNORED by USE_ALL_AVAILABLE_NODES strategy)
        Index index = createIndex("bp-stable", Arrays.asList(1));  // Only 1 replica configured!
        index.getSettings().setShardGroupsAllocateCount(Arrays.asList(2)); // 2 groups for shard 0
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        
        // Given: Existing planned allocation with nodes from 2 groups (group-a and group-b)
        ShardAllocation existingPlanned = new ShardAllocation("0", "bp-stable");
        existingPlanned.setIngestSUs(Arrays.asList("primary-0"));
        existingPlanned.setSearchSUs(Arrays.asList("replica-a-1", "replica-a-2", "replica-b-1", "replica-b-2"));
        existingPlanned.setAllocationTimestamp(System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());
        when(metadataStore.getPlannedAllocation(testClusterId, "bp-stable", "0"))
            .thenReturn(existingPlanned);
        
        // Given: 3 replica groups available (group-a, group-b, group-c)
        // Current allocation uses group-a and group-b → should remain stable
        List<SearchUnit> allNodes = Arrays.asList(
            createHealthyPrimaryNode("primary-0", "0"),
            createHealthyReplicaNode("replica-a-1", "group-a"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-a-2", "group-a"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-a-3", "group-a"),
            createHealthyReplicaNode("replica-b-1", "group-b"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-b-2", "group-b"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-b-3", "group-b"),
            createHealthyReplicaNode("replica-c-1", "group-c"),  // ✗ Not allocated (group-c available but not needed)
            createHealthyReplicaNode("replica-c-2", "group-c"),
            createHealthyReplicaNode("replica-c-3", "group-c")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation
        binPackingShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Should keep existing allocation stable (same 2 groups: group-a and group-b)
        // AND allocate to ALL healthy nodes in those groups (6 nodes, despite replica count = 1!)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("bp-stable"),
            eq("0"),
            argThat(allocation -> {
                // Should allocate to ALL 6 nodes from group-a and group-b (3 per group)
                // Config says replica count = 1, BUT:
                // - USE_ALL_AVAILABLE_NODES strategy ignores replica count
                // - Group-based allocation uses ALL healthy nodes from selected groups
                assertThat(allocation.getSearchSUs())
                    .as("Should use ALL 6 nodes from 2 groups (despite replica count = 1) due to USE_ALL_AVAILABLE_NODES")
                    .hasSize(6)  // 3 from group-a + 3 from group-b
                    .containsExactlyInAnyOrder(
                        "replica-a-1", "replica-a-2", "replica-a-3",
                        "replica-b-1", "replica-b-2", "replica-b-3"
                    );
                
                // Should NOT allocate to group-c
                assertThat(allocation.getSearchSUs())
                    .as("Should not select group-c (stable allocation)")
                    .noneMatch(node -> node.startsWith("replica-c-"));
                
                return true;
            })
        );
    }

    @Test
    void testE2E_BinPacking_ScaleUp() throws Exception {
        // Given: ShardAllocator with REAL GroupAwareBinPackingEngine
        ShardAllocator binPackingShardAllocator = new ShardAllocator(metadataStore);
        binPackingShardAllocator.setAllocationDecisionEngine(new GroupAwareBinPackingEngine());
        
        // Given: Index configured for 2 groups per shard
        Index index = createIndex("bp-scaleup", Arrays.asList(3));
        index.getSettings().setShardGroupsAllocateCount(Arrays.asList(2)); // Need 2 groups
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        
        // Given: Existing planned allocation with nodes from only 1 group (group-a)
        // Need to scale up to 2 groups
        ShardAllocation existingPlanned = new ShardAllocation("0", "bp-scaleup");
        existingPlanned.setIngestSUs(Arrays.asList("primary-0"));
        existingPlanned.setSearchSUs(Arrays.asList("replica-a-1", "replica-a-2"));
        existingPlanned.setAllocationTimestamp(System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());
        when(metadataStore.getPlannedAllocation(testClusterId, "bp-scaleup", "0"))
            .thenReturn(existingPlanned);
        
        // Given: 3 replica groups available
        List<SearchUnit> allNodes = Arrays.asList(
            createHealthyPrimaryNode("primary-0", "0"),
            createHealthyReplicaNode("replica-a-1", "group-a"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-a-2", "group-a"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-a-3", "group-a"),
            createHealthyReplicaNode("replica-b-1", "group-b"),  // Available for scale-up
            createHealthyReplicaNode("replica-b-2", "group-b"),
            createHealthyReplicaNode("replica-b-3", "group-b"),
            createHealthyReplicaNode("replica-c-1", "group-c"),  // Available for scale-up
            createHealthyReplicaNode("replica-c-2", "group-c"),
            createHealthyReplicaNode("replica-c-3", "group-c")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation
        binPackingShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Should add 1 more group (group-a + one of group-b/group-c)
        // AND allocate to ALL healthy nodes in those 2 groups (6 total nodes)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("bp-scaleup"),
            eq("0"),
            argThat(allocation -> {
                List<String> allocatedNodes = allocation.getSearchSUs();
                
                // Should allocate to ALL nodes from 2 groups (3 nodes per group = 6 total)
                // Replica count config (3) is IGNORED
                assertThat(allocatedNodes)
                    .as("Scale-up: should use ALL healthy nodes from 2 groups")
                    .hasSize(6);
                
                // Should include ALL nodes from group-a (existing group)
                assertThat(allocatedNodes)
                    .as("Should keep ALL nodes from existing group-a")
                    .contains("replica-a-1", "replica-a-2", "replica-a-3");
                
                // Should add one more group (group-b or group-c) with ALL its nodes
                boolean hasGroupB = allocatedNodes.containsAll(Arrays.asList("replica-b-1", "replica-b-2", "replica-b-3"));
                boolean hasGroupC = allocatedNodes.containsAll(Arrays.asList("replica-c-1", "replica-c-2", "replica-c-3"));
                assertThat(hasGroupB || hasGroupC)
                    .as("Should add ALL nodes from one additional group (group-b or group-c)")
                    .isTrue();
                
                // Should allocate to exactly 2 distinct groups
                long distinctGroups = allocatedNodes.stream()
                    .map(node -> node.split("-")[1]) // Extract group name
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Should allocate to exactly 2 groups")
                    .isEqualTo(2);
                
                return true;
            })
        );
    }

    @Test
    void testE2E_BinPacking_InitialAllocation() throws Exception {
        // Given: ShardAllocator with REAL GroupAwareBinPackingEngine
        ShardAllocator binPackingShardAllocator = new ShardAllocator(metadataStore);
        binPackingShardAllocator.setAllocationDecisionEngine(new GroupAwareBinPackingEngine());
        
        // Given: Index configured for 2 groups per shard
        Index index = createIndex("bp-initial", Arrays.asList(3));
        index.getSettings().setShardGroupsAllocateCount(Arrays.asList(2)); // Need 2 groups
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        
        // Given: No existing planned allocation (initial allocation)
        when(metadataStore.getPlannedAllocation(testClusterId, "bp-initial", "0"))
            .thenReturn(null);
        
        // Given: 3 replica groups available
        List<SearchUnit> allNodes = Arrays.asList(
            createHealthyPrimaryNode("primary-0", "0"),
            createHealthyReplicaNode("replica-a-1", "group-a"),
            createHealthyReplicaNode("replica-a-2", "group-a"),
            createHealthyReplicaNode("replica-a-3", "group-a"),
            createHealthyReplicaNode("replica-b-1", "group-b"),
            createHealthyReplicaNode("replica-b-2", "group-b"),
            createHealthyReplicaNode("replica-b-3", "group-b"),
            createHealthyReplicaNode("replica-c-1", "group-c"),
            createHealthyReplicaNode("replica-c-2", "group-c"),
            createHealthyReplicaNode("replica-c-3", "group-c")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation
        binPackingShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Should randomly select 2 groups from available 3
        // AND allocate to ALL healthy nodes in those 2 groups (6 total nodes)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("bp-initial"),
            eq("0"),
            argThat(allocation -> {
                // Should allocate IngestSU
                assertThat(allocation.getIngestSUs())
                    .as("IngestSUs must be exactly 1 per shard")
                    .hasSize(1);
                
                // Should allocate ALL nodes from 2 randomly selected groups (3 per group = 6 total)
                // Replica count config (3) is IGNORED
                List<String> allocatedNodes = allocation.getSearchSUs();
                assertThat(allocatedNodes)
                    .as("Initial allocation: should use ALL healthy nodes from 2 groups")
                    .hasSize(6);
                
                // Count distinct groups - should be exactly 2
                long distinctGroups = allocatedNodes.stream()
                    .map(node -> node.split("-")[1]) // Extract group name (a, b, or c)
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Should allocate to exactly 2 groups (random selection from 3 available)")
                    .isEqualTo(2);
                
                // Verify each selected group has ALL its nodes (3 nodes per group)
                allocatedNodes.stream()
                    .map(node -> node.split("-")[1]) // Get group name
                    .distinct()
                    .forEach(groupName -> {
                        long nodesInGroup = allocatedNodes.stream()
                            .filter(node -> node.contains("-" + groupName + "-"))
                            .count();
                        assertThat(nodesInGroup)
                            .as("Group " + groupName + " should have ALL 3 nodes allocated")
                            .isEqualTo(3);
                    });
                
                return true;
            })
        );
    }
}
