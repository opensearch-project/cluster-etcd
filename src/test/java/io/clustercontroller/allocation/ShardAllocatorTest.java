package io.clustercontroller.allocation;

import io.clustercontroller.allocation.AllocationDecisionEngine;
import io.clustercontroller.enums.HealthState;
import io.clustercontroller.enums.NodeRole;
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
        
        // Mock existing planned allocation with multiple IngestSUs (violates single-writer constraint)
        ShardAllocation currentAllocation = new ShardAllocation("0", "index1");
        currentAllocation.setIngestSUs(Arrays.asList("node1", "node2")); // Multiple IngestSUs!
        currentAllocation.setSearchSUs(Arrays.asList("node3"));
        when(metadataStore.getPlannedAllocation(anyString(), anyString(), anyString())).thenReturn(currentAllocation);

        // Mock PRIMARY nodes (for reallocation) and REPLICA nodes
        List<SearchUnit> eligiblePrimaryNodes = Arrays.asList(createSearchUnit("node4", "PRIMARY"));
        List<SearchUnit> eligibleSearchNodes = Arrays.asList(createSearchUnit("node4", "REPLICA"));
        when(allocationDecisionEngine.getAvailableNodesForAllocation(anyInt(), anyString(), any(), anyList(), eq(NodeRole.PRIMARY), any()))
            .thenReturn(eligiblePrimaryNodes);
        when(allocationDecisionEngine.getAvailableNodesForAllocation(anyInt(), anyString(), any(), anyList(), eq(NodeRole.REPLICA), any()))
            .thenReturn(eligibleSearchNodes);

        // When
        shardAllocator.planShardAllocation(testClusterId, AllocationStrategy.RESPECT_REPLICA_COUNT);

        // Then - Should reallocate to single ingester (fixing the violation)
        verify(metadataStore).getAllIndexConfigs(testClusterId);
        verify(metadataStore, atLeastOnce()).getPlannedAllocation(anyString(), anyString(), anyString());
        verify(metadataStore).setPlannedAllocation(anyString(), anyString(), anyString(), argThat(allocation -> {
            ShardAllocation shardAllocation = (ShardAllocation) allocation;
            // Should reallocate to a single ingester (fixing the multiple IngestSUs violation)
            return shardAllocation.getIngestSUs() != null &&
                   shardAllocation.getIngestSUs().size() == 1 &&
                   shardAllocation.getIngestSUs().contains("node4") &&
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
        
        // Should call getPlannedAllocation for each shard (6 shards * 1 call per shard = 6 calls)
        verify(metadataStore, times(6)).getPlannedAllocation(anyString(), anyString(), anyString());
        
        // Should call setPlannedAllocation for each shard (6 calls)
        verify(metadataStore, times(6)).setPlannedAllocation(anyString(), anyString(), anyString(), any(ShardAllocation.class));
        
        // Verify specific index/shard combinations were processed
        verify(metadataStore, times(1)).getPlannedAllocation(testClusterId, "index1", "0");
        verify(metadataStore, times(1)).getPlannedAllocation(testClusterId, "index1", "1");
        verify(metadataStore, times(1)).getPlannedAllocation(testClusterId, "index2", "0");
        verify(metadataStore, times(1)).getPlannedAllocation(testClusterId, "index3", "0");
        verify(metadataStore, times(1)).getPlannedAllocation(testClusterId, "index3", "1");
        verify(metadataStore, times(1)).getPlannedAllocation(testClusterId, "index3", "2");
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

    private Index createIndexWithGroupsAndReplicas(String indexName, List<Integer> shardReplicaCount, List<Integer> shardGroupsAllocateCount) {
        Index index = new Index();
        index.setIndexName(indexName);
        index.setSettings(new io.clustercontroller.models.IndexSettings());
        index.getSettings().setShardReplicaCount(shardReplicaCount);
        index.getSettings().setNumGroupsPerShard(shardGroupsAllocateCount);
        index.getSettings().setNumberOfShards(shardReplicaCount.size());
        return index;
    }

    // ========== E2E Tests with Real StandardAllocationEngine ==========

    @Test
    void testE2E_UseAllAvailableNodes_IgnoresReplicaCount() throws Exception {
        // Given: Create ShardAllocator with REAL StandardAllocationEngine (not mocked!)
        ShardAllocator e2eShardAllocator = new ShardAllocator(metadataStore);
        e2eShardAllocator.setAllocationDecisionEngine(new StandardAllocationEngine()); // Explicitly use Standard
        
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
        e2eShardAllocator.setAllocationDecisionEngine(new StandardAllocationEngine()); // Explicitly use Standard
        
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
        e2eShardAllocator.setAllocationDecisionEngine(new StandardAllocationEngine()); // Explicitly use Standard
        
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
        
        // Given: Index with 3 shards, different group counts per shard
        // Shard 0: 2 groups, Shard 1: 3 groups, Shard 2: 2 groups
        Index index = createIndex("bp-stable", Arrays.asList(1, 1, 1));  // Replica count IGNORED
        index.getSettings().setNumGroupsPerShard(Arrays.asList(2, 3, 2)); // Group counts per shard
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        
        // Given: Existing planned allocations for 3 shards
        // Test THREE scenarios with ONLY 2 PRIMARY nodes (demonstrates bin-packing reuse!):
        // 1. Stable allocation: Shard 0 has existing healthy PRIMARY → should keep it
        // 2. New allocation: Shard 1 has NO PRIMARY → should allocate from shared PRIMARY group
        // 3. PRIMARY reuse: Shard 2 has NO PRIMARY → reuses one of the 2 PRIMARY nodes!
        
        // Shard 0: HAS existing PRIMARY (stable allocation test)
        ShardAllocation shard0Planned = new ShardAllocation("0", "bp-stable");
        shard0Planned.setIngestSUs(Arrays.asList("primary-pool-0-a"));  // ← Existing PRIMARY
        shard0Planned.setSearchSUs(Arrays.asList("replica-a-1", "replica-a-2", "replica-b-1", "replica-b-2"));
        shard0Planned.setAllocationTimestamp(System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());
        
        // Shard 1: NO PRIMARY yet (new allocation test)
        ShardAllocation shard1Planned = new ShardAllocation("1", "bp-stable");
        shard1Planned.setIngestSUs(null);  // ← NO PRIMARY! Bin-packing should allocate one
        shard1Planned.setSearchSUs(Arrays.asList("replica-d-1", "replica-d-2", "replica-e-1", "replica-e-2", "replica-f-1", "replica-f-2"));
        shard1Planned.setAllocationTimestamp(System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());
        
        // Shard 2: NO PRIMARY yet (PRIMARY reuse test - with only 2 PRIMARY nodes, this must reuse one!)
        ShardAllocation shard2Planned = new ShardAllocation("2", "bp-stable");
        shard2Planned.setIngestSUs(null);  // ← NO PRIMARY! Must reuse from 2 available
        shard2Planned.setSearchSUs(Arrays.asList("replica-h-1", "replica-h-2", "replica-i-1", "replica-i-2"));
        shard2Planned.setAllocationTimestamp(System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());
        
        when(metadataStore.getPlannedAllocation(testClusterId, "bp-stable", "0"))
            .thenReturn(shard0Planned);
        when(metadataStore.getPlannedAllocation(testClusterId, "bp-stable", "1"))
            .thenReturn(shard1Planned);
        when(metadataStore.getPlannedAllocation(testClusterId, "bp-stable", "2"))
            .thenReturn(shard2Planned);
        
        // Given: Single PRIMARY group with 2 nodes (shared across shards - bin-packing!)
        // These same PRIMARY nodes can be reused for future shards (no shard affinity!)
        // Multiple REPLICA groups for different shards
        List<SearchUnit> allNodes = Arrays.asList(
            // PRIMARY Group 0 - SHARED across ALL shards (no shard affinity!)
            createHealthyPrimaryNode("primary-pool-0-a", "0"),  // ✓ Currently used by Shard 0, reusable for future shards
            createHealthyPrimaryNode("primary-pool-0-b", "0"),  // ✓ Currently used by Shard 1, reusable for future shards
            
            // REPLICA Groups for Shard 0
            createHealthyReplicaNode("replica-a-1", "group-a"),  // ✓ Shard 0 allocated
            createHealthyReplicaNode("replica-a-2", "group-a"),  // ✓ Shard 0 allocated
            createHealthyReplicaNode("replica-a-3", "group-a"),
            createHealthyReplicaNode("replica-b-1", "group-b"),  // ✓ Shard 0 allocated
            createHealthyReplicaNode("replica-b-2", "group-b"),  // ✓ Shard 0 allocated
            createHealthyReplicaNode("replica-b-3", "group-b"),
            createHealthyReplicaNode("replica-c-1", "group-c"),  // ✗ Available but not needed
            createHealthyReplicaNode("replica-c-2", "group-c"),
            createHealthyReplicaNode("replica-c-3", "group-c"),
            
            // REPLICA Groups for Shard 1
            createHealthyReplicaNode("replica-d-1", "group-d"),  // ✓ Shard 1 allocated
            createHealthyReplicaNode("replica-d-2", "group-d"),  // ✓ Shard 1 allocated
            createHealthyReplicaNode("replica-d-3", "group-d"),
            createHealthyReplicaNode("replica-e-1", "group-e"),  // ✓ Shard 1 allocated
            createHealthyReplicaNode("replica-e-2", "group-e"),  // ✓ Shard 1 allocated
            createHealthyReplicaNode("replica-e-3", "group-e"),
            createHealthyReplicaNode("replica-f-1", "group-f"),  // ✓ Shard 1 allocated
            createHealthyReplicaNode("replica-f-2", "group-f"),  // ✓ Shard 1 allocated
            createHealthyReplicaNode("replica-f-3", "group-f"),
            createHealthyReplicaNode("replica-g-1", "group-g"),  // ✗ Available but not needed
            createHealthyReplicaNode("replica-g-2", "group-g"),
            createHealthyReplicaNode("replica-g-3", "group-g"),
            
            // REPLICA Groups for Shard 2
            createHealthyReplicaNode("replica-h-1", "group-h"),  // ✓ Shard 2 allocated
            createHealthyReplicaNode("replica-h-2", "group-h"),  // ✓ Shard 2 allocated
            createHealthyReplicaNode("replica-h-3", "group-h"),
            createHealthyReplicaNode("replica-i-1", "group-i"),  // ✓ Shard 2 allocated
            createHealthyReplicaNode("replica-i-2", "group-i"),  // ✓ Shard 2 allocated
            createHealthyReplicaNode("replica-i-3", "group-i"),
            createHealthyReplicaNode("replica-j-1", "group-j"),  // ✗ Available but not needed
            createHealthyReplicaNode("replica-j-2", "group-j"),
            createHealthyReplicaNode("replica-j-3", "group-j")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation
        binPackingShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Verify Shard 0 - should keep existing allocation stable (2 replica groups + PRIMARY)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("bp-stable"),
            eq("0"),
            argThat(allocation -> {
                // ========== PRIMARY VALIDATION (Stable Allocation) ==========
                assertThat(allocation.getIngestSUs())
                    .as("Shard 0: Should keep existing PRIMARY node (stable allocation)")
                    .hasSize(1)
                    .containsExactly("primary-pool-0-a");  // ✅ Keeps same PRIMARY
                
                // ========== REPLICA VALIDATION ==========
                // Should allocate to ALL 6 nodes from group-a and group-b (3 per group)
                assertThat(allocation.getSearchSUs())
                    .as("Shard 0: Should use ALL nodes from 2 groups (stable allocation)")
                    .hasSize(6)
                    .containsExactlyInAnyOrder(
                        "replica-a-1", "replica-a-2", "replica-a-3",
                        "replica-b-1", "replica-b-2", "replica-b-3"
                    );
                
                // Should NOT allocate to group-c
                assertThat(allocation.getSearchSUs())
                    .noneMatch(node -> node.startsWith("replica-c-"));
                
                return true;
            })
        );
        
        // Then: Verify Shard 1 - should allocate NEW PRIMARY from shared group (3 replica groups + PRIMARY)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("bp-stable"),
            eq("1"),
            argThat(allocation -> {
                // ========== PRIMARY VALIDATION (NEW Allocation from SHARED GROUP) ==========
                assertThat(allocation.getIngestSUs())
                    .as("Shard 1: Should allocate NEW PRIMARY from SHARED PRIMARY group (bin-packing)")
                    .hasSize(1)
                    .satisfies(ingestSUs -> assertThat(ingestSUs.get(0))
                        .as("Should pick from shared PRIMARY group (could be primary-pool-0-a or primary-pool-0-b)")
                        .isIn("primary-pool-0-a", "primary-pool-0-b"));  // ✅ Random from shared group
                
                // ========== REPLICA VALIDATION ==========
                // Should allocate to ALL 9 nodes from group-d, group-e, and group-f (3 per group)
                assertThat(allocation.getSearchSUs())
                    .as("Shard 1: Should use ALL nodes from 3 groups (stable allocation)")
                    .hasSize(9)
                    .containsExactlyInAnyOrder(
                        "replica-d-1", "replica-d-2", "replica-d-3",
                        "replica-e-1", "replica-e-2", "replica-e-3",
                        "replica-f-1", "replica-f-2", "replica-f-3"
                    );
                
                // Should NOT allocate to group-g
                assertThat(allocation.getSearchSUs())
                    .noneMatch(node -> node.startsWith("replica-g-"));
                
                return true;
            })
        );
        
        // Then: Verify Shard 2 - demonstrates PRIMARY REUSE (only 2 PRIMARY nodes for 3 shards!)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("bp-stable"),
            eq("2"),
            argThat(allocation -> {
                // ========== PRIMARY VALIDATION (PRIMARY REUSE!) ==========
                // With only 2 PRIMARY nodes and 3 shards, Shard 2 MUST reuse one of them!
                // This demonstrates the power of bin-packing: no shard affinity needed
                assertThat(allocation.getIngestSUs())
                    .as("Shard 2: Should REUSE PRIMARY from shared group (only 2 PRIMARY nodes for 3 shards!)")
                    .hasSize(1)
                    .satisfies(ingestSUs -> assertThat(ingestSUs.get(0))
                        .as("Must pick from the 2 available PRIMARY nodes (demonstrates PRIMARY reuse)")
                        .isIn("primary-pool-0-a", "primary-pool-0-b"));  // ✅ Reuses one of the 2 PRIMARYs
                
                // ========== REPLICA VALIDATION ==========
                // Should allocate to ALL 6 nodes from group-h and group-i (3 per group)
                assertThat(allocation.getSearchSUs())
                    .as("Shard 2: Should use ALL nodes from 2 groups")
                    .hasSize(6)
                    .containsExactlyInAnyOrder(
                        "replica-h-1", "replica-h-2", "replica-h-3",
                        "replica-i-1", "replica-i-2", "replica-i-3"
                    );
                
                // Should NOT allocate to group-j
                assertThat(allocation.getSearchSUs())
                    .noneMatch(node -> node.startsWith("replica-j-"));
                
                return true;
            })
        );
    }

    @Test
    void testE2E_BinPacking_ScaleUp() throws Exception {
        // Given: ShardAllocator with REAL GroupAwareBinPackingEngine
        ShardAllocator binPackingShardAllocator = new ShardAllocator(metadataStore);
        binPackingShardAllocator.setAllocationDecisionEngine(new GroupAwareBinPackingEngine());
        
        // Given: Index with 2 shards, admin increased group counts
        // Shard 0: 1 group → 2 groups (scale up)
        // Shard 1: 2 groups → 4 groups (scale up)
        Index index = createIndex("bp-scaleup", Arrays.asList(3, 3));
        index.getSettings().setNumGroupsPerShard(Arrays.asList(2, 4)); // New desired group counts
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        
        // Given: Existing planned allocations
        // Shard 0: currently has 1 group (group-a), needs to scale to 2
        ShardAllocation shard0Planned = new ShardAllocation("0", "bp-scaleup");
        shard0Planned.setIngestSUs(Arrays.asList("primary-0"));
        shard0Planned.setSearchSUs(Arrays.asList("replica-a-1", "replica-a-2"));
        shard0Planned.setAllocationTimestamp(System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());
        
        // Shard 1: currently has 2 groups (group-d, group-e), needs to scale to 4
        ShardAllocation shard1Planned = new ShardAllocation("1", "bp-scaleup");
        shard1Planned.setIngestSUs(Arrays.asList("primary-1"));
        shard1Planned.setSearchSUs(Arrays.asList("replica-d-1", "replica-d-2", "replica-e-1", "replica-e-2"));
        shard1Planned.setAllocationTimestamp(System.currentTimeMillis() - Duration.ofMinutes(10).toMillis());
        
        when(metadataStore.getPlannedAllocation(testClusterId, "bp-scaleup", "0"))
            .thenReturn(shard0Planned);
        when(metadataStore.getPlannedAllocation(testClusterId, "bp-scaleup", "1"))
            .thenReturn(shard1Planned);
        
        // Given: Multiple replica groups available for scale-up
        // Shard 0: group-a (allocated), group-b, group-c (available for scale-up)
        // Shard 1: group-d, group-e (allocated), group-f, group-g, group-h, group-i (available for scale-up)
        List<SearchUnit> allNodes = Arrays.asList(
            // Shard 0 nodes (shard pool ID must match shard ID "0")
            createHealthyPrimaryNode("primary-0", "0"),
            createHealthyReplicaNode("replica-a-1", "group-a"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-a-2", "group-a"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-a-3", "group-a"),
            createHealthyReplicaNode("replica-b-1", "group-b"),  // Available for scale-up
            createHealthyReplicaNode("replica-b-2", "group-b"),
            createHealthyReplicaNode("replica-b-3", "group-b"),
            createHealthyReplicaNode("replica-c-1", "group-c"),  // Available for scale-up
            createHealthyReplicaNode("replica-c-2", "group-c"),
            createHealthyReplicaNode("replica-c-3", "group-c"),
            
            // Shard 1 nodes (shard pool ID must match shard ID "1")
            createHealthyPrimaryNode("primary-1", "1"),
            createHealthyReplicaNode("replica-d-1", "group-d"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-d-2", "group-d"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-d-3", "group-d"),
            createHealthyReplicaNode("replica-e-1", "group-e"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-e-2", "group-e"),  // ✓ Currently allocated
            createHealthyReplicaNode("replica-e-3", "group-e"),
            createHealthyReplicaNode("replica-f-1", "group-f"),  // Available for scale-up
            createHealthyReplicaNode("replica-f-2", "group-f"),
            createHealthyReplicaNode("replica-f-3", "group-f"),
            createHealthyReplicaNode("replica-g-1", "group-g"),  // Available for scale-up
            createHealthyReplicaNode("replica-g-2", "group-g"),
            createHealthyReplicaNode("replica-g-3", "group-g"),
            createHealthyReplicaNode("replica-h-1", "group-h"),  // Available for scale-up
            createHealthyReplicaNode("replica-h-2", "group-h"),
            createHealthyReplicaNode("replica-h-3", "group-h"),
            createHealthyReplicaNode("replica-i-1", "group-i"),  // Available for scale-up
            createHealthyReplicaNode("replica-i-2", "group-i"),
            createHealthyReplicaNode("replica-i-3", "group-i")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation
        binPackingShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Verify Shard 0 - should scale up from 1 group to 2 groups
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("bp-scaleup"),
            eq("0"),
            argThat(allocation -> {
                // Single writer constraint
                assertThat(allocation.getIngestSUs())
                    .as("Shard 0: Single writer constraint")
                    .hasSize(1)
                    .containsExactly("primary-0");
                
                List<String> allocatedNodes = allocation.getSearchSUs();
                
                // Should allocate to ALL nodes from 2 groups (3 nodes per group = 6 total)
                assertThat(allocatedNodes)
                    .as("Shard 0: Scale-up should use ALL healthy nodes from 2 groups")
                    .hasSize(6);
                
                // Should keep ALL nodes from existing group-a
                assertThat(allocatedNodes)
                    .as("Shard 0: Should keep ALL nodes from existing group-a")
                    .contains("replica-a-1", "replica-a-2", "replica-a-3");
                
                // Verify exactly 2 distinct groups (doesn't matter which specific group was added)
                long distinctGroups = allocatedNodes.stream()
                    .map(node -> node.split("-")[1]) // Extract group name
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Shard 0: Should allocate to exactly 2 groups")
                    .isEqualTo(2);
                
                return true;
            })
        );
        
        // Then: Verify Shard 1 - should scale up from 2 groups to 4 groups
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("bp-scaleup"),
            eq("1"),
            argThat(allocation -> {
                // Single writer constraint
                assertThat(allocation.getIngestSUs())
                    .as("Shard 1: Single writer constraint")
                    .hasSize(1)
                    .containsExactly("primary-1");
                
                List<String> allocatedNodes = allocation.getSearchSUs();
                
                // Should allocate to ALL nodes from 4 groups (3 nodes per group = 12 total)
                assertThat(allocatedNodes)
                    .as("Shard 1: Scale-up should use ALL healthy nodes from 4 groups")
                    .hasSize(12);
                
                // Should keep ALL nodes from existing group-d and group-e
                assertThat(allocatedNodes)
                    .as("Shard 1: Should keep ALL nodes from existing groups (group-d, group-e)")
                    .contains("replica-d-1", "replica-d-2", "replica-d-3",
                              "replica-e-1", "replica-e-2", "replica-e-3");
                
                // Should have exactly 4 distinct groups
                long distinctGroups = allocatedNodes.stream()
                    .map(node -> node.split("-")[1]) // Extract group name
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Shard 1: Should allocate to exactly 4 groups")
                    .isEqualTo(4);
                
                return true;
            })
        );
    }

    @Test
    void testE2E_BinPacking_InitialAllocation() throws Exception {
        // Given: ShardAllocator with REAL GroupAwareBinPackingEngine
        ShardAllocator binPackingShardAllocator = new ShardAllocator(metadataStore);
        binPackingShardAllocator.setAllocationDecisionEngine(new GroupAwareBinPackingEngine());
        
        // Given: Index with 2 shards, different group requirements
        // Shard 0: needs 2 groups, Shard 1: needs 3 groups
        Index index = createIndex("bp-initial", Arrays.asList(3, 3));
        index.getSettings().setNumGroupsPerShard(Arrays.asList(2, 3));
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        
        // Given: No existing planned allocations (brand new index)
        when(metadataStore.getPlannedAllocation(testClusterId, "bp-initial", "0"))
            .thenReturn(null);
        when(metadataStore.getPlannedAllocation(testClusterId, "bp-initial", "1"))
            .thenReturn(null);
        
        // Given: Multiple replica groups available for initial allocation
        // Shard 0: 3 groups available (will select 2)
        // Shard 1: 4 groups available (will select 3)
        List<SearchUnit> allNodes = Arrays.asList(
            // Shard 0 nodes
            createHealthyPrimaryNode("primary-0", "0"),
            createHealthyReplicaNode("replica-a-1", "group-a"),
            createHealthyReplicaNode("replica-a-2", "group-a"),
            createHealthyReplicaNode("replica-a-3", "group-a"),
            createHealthyReplicaNode("replica-b-1", "group-b"),
            createHealthyReplicaNode("replica-b-2", "group-b"),
            createHealthyReplicaNode("replica-b-3", "group-b"),
            createHealthyReplicaNode("replica-c-1", "group-c"),
            createHealthyReplicaNode("replica-c-2", "group-c"),
            createHealthyReplicaNode("replica-c-3", "group-c"),
            
            // Shard 1 nodes
            createHealthyPrimaryNode("primary-1", "1"),
            createHealthyReplicaNode("replica-d-1", "group-d"),
            createHealthyReplicaNode("replica-d-2", "group-d"),
            createHealthyReplicaNode("replica-d-3", "group-d"),
            createHealthyReplicaNode("replica-e-1", "group-e"),
            createHealthyReplicaNode("replica-e-2", "group-e"),
            createHealthyReplicaNode("replica-e-3", "group-e"),
            createHealthyReplicaNode("replica-f-1", "group-f"),
            createHealthyReplicaNode("replica-f-2", "group-f"),
            createHealthyReplicaNode("replica-f-3", "group-f"),
            createHealthyReplicaNode("replica-g-1", "group-g"),
            createHealthyReplicaNode("replica-g-2", "group-g"),
            createHealthyReplicaNode("replica-g-3", "group-g")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation
        binPackingShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Verify Shard 0 - should randomly select 2 groups from 3 available
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("bp-initial"),
            eq("0"),
            argThat(allocation -> {
                // Single writer constraint
                assertThat(allocation.getIngestSUs())
                    .as("Shard 0: IngestSUs must be exactly 1 per shard")
                    .hasSize(1);
                
                // Should allocate ALL nodes from 2 randomly selected groups (3 per group = 6 total)
                List<String> allocatedNodes = allocation.getSearchSUs();
                assertThat(allocatedNodes)
                    .as("Shard 0: Initial allocation should use ALL healthy nodes from 2 groups")
                    .hasSize(6);
                
                // Count distinct groups - should be exactly 2
                long distinctGroups = allocatedNodes.stream()
                    .map(node -> node.split("-")[1]) // Extract group name (a, b, or c)
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Shard 0: Should allocate to exactly 2 groups (random selection from 3 available)")
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
                            .as("Shard 0: Group " + groupName + " should have ALL 3 nodes allocated")
                            .isEqualTo(3);
                    });
                
                return true;
            })
        );
        
        // Then: Verify Shard 1 - should randomly select 3 groups from 4 available
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("bp-initial"),
            eq("1"),
            argThat(allocation -> {
                // Single writer constraint
                assertThat(allocation.getIngestSUs())
                    .as("Shard 1: IngestSUs must be exactly 1 per shard")
                    .hasSize(1);
                
                // Should allocate ALL nodes from 3 randomly selected groups (3 per group = 9 total)
                List<String> allocatedNodes = allocation.getSearchSUs();
                assertThat(allocatedNodes)
                    .as("Shard 1: Initial allocation should use ALL healthy nodes from 3 groups")
                    .hasSize(9);
                
                // Count distinct groups - should be exactly 3
                long distinctGroups = allocatedNodes.stream()
                    .map(node -> node.split("-")[1]) // Extract group name (d, e, f, or g)
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Shard 1: Should allocate to exactly 3 groups (random selection from 4 available)")
                    .isEqualTo(3);
                
                // Verify each selected group has ALL its nodes (3 nodes per group)
                allocatedNodes.stream()
                    .map(node -> node.split("-")[1]) // Get group name
                    .distinct()
                    .forEach(groupName -> {
                        long nodesInGroup = allocatedNodes.stream()
                            .filter(node -> node.contains("-" + groupName + "-"))
                            .count();
                        assertThat(nodesInGroup)
                            .as("Shard 1: Group " + groupName + " should have ALL 3 nodes allocated")
                            .isEqualTo(3);
                    });
                
                return true;
            })
        );
    }

    @Test
    void testE2E_BinPacking_MultipleIndexes_InitialAllocation() throws Exception {
        // Given: ShardAllocator with REAL GroupAwareBinPackingEngine using seeded random for deterministic tests
        ShardAllocator binPackingShardAllocator = new ShardAllocator(metadataStore);
        GroupAwareBinPackingEngine engine = new GroupAwareBinPackingEngine();
        // Use seeded random (seed=100) for deterministic node selection in tests
        IngesterNodeSelector seededSelector = new IngesterNodeSelector(new RandomIngesterNodeSelectionStrategy(100L));
        engine.setIngesterNodeSelector(seededSelector);
        binPackingShardAllocator.setAllocationDecisionEngine(engine);
        
        // Given: 2 indexes with multiple shards, different group configurations
        // Index1: 2 shards → Shard 0: 2 groups, Shard 1: 3 groups
        Index index1 = createIndex("multi-index1", Arrays.asList(1, 1));
        index1.getSettings().setNumGroupsPerShard(Arrays.asList(2, 3));
        
        // Index2: 2 shards → Shard 0: 1 group, Shard 1: 2 groups
        Index index2 = createIndex("multi-index2", Arrays.asList(1, 1));
        index2.getSettings().setNumGroupsPerShard(Arrays.asList(1, 2));
        
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index1, index2));
        
        // Given: No existing planned allocations (all brand new)
        when(metadataStore.getPlannedAllocation(eq(testClusterId), anyString(), anyString()))
            .thenReturn(null);
        
        // Given: 5 replica groups available, shared across both indexes
        // Note: In real deployments, shard pools are shared across indexes!
        // - Both Index1 Shard 0 and Index2 Shard 0 use the same PRIMARY node (pool "0")
        // - Both Index1 Shard 1 and Index2 Shard 1 use the same PRIMARY node (pool "1")
        List<SearchUnit> allNodes = Arrays.asList(
            // Shard pool "0" - serves both Index1/Shard0 and Index2/Shard0
            createHealthyPrimaryNode("primary-pool-0", "0"),
            // Shard pool "1" - serves both Index1/Shard1 and Index2/Shard1
            createHealthyPrimaryNode("primary-pool-1", "1"),
            
            // Group A
            createHealthyReplicaNode("replica-a-1", "group-a"),
            createHealthyReplicaNode("replica-a-2", "group-a"),
            createHealthyReplicaNode("replica-a-3", "group-a"),
            // Group B
            createHealthyReplicaNode("replica-b-1", "group-b"),
            createHealthyReplicaNode("replica-b-2", "group-b"),
            createHealthyReplicaNode("replica-b-3", "group-b"),
            // Group C
            createHealthyReplicaNode("replica-c-1", "group-c"),
            createHealthyReplicaNode("replica-c-2", "group-c"),
            createHealthyReplicaNode("replica-c-3", "group-c"),
            // Group D
            createHealthyReplicaNode("replica-d-1", "group-d"),
            createHealthyReplicaNode("replica-d-2", "group-d"),
            createHealthyReplicaNode("replica-d-3", "group-d"),
            // Group E
            createHealthyReplicaNode("replica-e-1", "group-e"),
            createHealthyReplicaNode("replica-e-2", "group-e"),
            createHealthyReplicaNode("replica-e-3", "group-e")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation
        binPackingShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Verify Index1 Shard 0 - should select 2 groups, random primary selection with seed=100
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("multi-index1"),
            eq("0"),
            argThat(allocation -> {
                assertThat(allocation.getIngestSUs())
                    .as("Index1 Shard 0: Single writer constraint, random selection with seed=100")
                    .hasSize(1)
                    .satisfies(ingestSUs -> assertThat(ingestSUs.get(0))
                        .isIn("primary-pool-0", "primary-pool-1"));  // Random selection from available primaries
                
                assertThat(allocation.getSearchSUs())
                    .as("Index1 Shard 0: Should allocate to 2 groups × 3 nodes = 6 nodes")
                    .hasSize(6);
                
                long distinctGroups = allocation.getSearchSUs().stream()
                    .map(node -> node.split("-")[1])
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Index1 Shard 0: Should have exactly 2 groups")
                    .isEqualTo(2);
                
                return true;
            })
        );
        
        // Then: Verify Index1 Shard 1 - should select 3 groups, random primary selection with seed=100
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("multi-index1"),
            eq("1"),
            argThat(allocation -> {
                assertThat(allocation.getIngestSUs())
                    .as("Index1 Shard 1: Single writer constraint, random selection with seed=100")
                    .hasSize(1)
                    .satisfies(ingestSUs -> assertThat(ingestSUs.get(0))
                        .isIn("primary-pool-0", "primary-pool-1"));  // Random selection from available primaries
                
                assertThat(allocation.getSearchSUs())
                    .as("Index1 Shard 1: Should allocate to 3 groups × 3 nodes = 9 nodes")
                    .hasSize(9);
                
                long distinctGroups = allocation.getSearchSUs().stream()
                    .map(node -> node.split("-")[1])
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Index1 Shard 1: Should have exactly 3 groups")
                    .isEqualTo(3);
                
                return true;
            })
        );
        
        // Then: Verify Index2 Shard 0 - should select 1 group, independent random primary selection
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("multi-index2"),
            eq("0"),
            argThat(allocation -> {
                assertThat(allocation.getIngestSUs())
                    .as("Index2 Shard 0: Single writer constraint, random selection")
                    .hasSize(1)
                    .satisfies(ingestSUs -> assertThat(ingestSUs.get(0))
                        .isIn("primary-pool-0", "primary-pool-1"));  // Random selection from available primaries
                
                assertThat(allocation.getSearchSUs())
                    .as("Index2 Shard 0: Should allocate to 1 group × 3 nodes = 3 nodes")
                    .hasSize(3);
                
                long distinctGroups = allocation.getSearchSUs().stream()
                    .map(node -> node.split("-")[1])
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Index2 Shard 0: Should have exactly 1 group")
                    .isEqualTo(1);
                
                return true;
            })
        );
        
        // Then: Verify Index2 Shard 1 - should select 2 groups, independent random primary selection
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("multi-index2"),
            eq("1"),
            argThat(allocation -> {
                assertThat(allocation.getIngestSUs())
                    .as("Index2 Shard 1: Single writer constraint, random selection")
                    .hasSize(1)
                    .satisfies(ingestSUs -> assertThat(ingestSUs.get(0))
                        .isIn("primary-pool-0", "primary-pool-1"));  // Random selection from available primaries
                
                assertThat(allocation.getSearchSUs())
                    .as("Index2 Shard 1: Should allocate to 2 groups × 3 nodes = 6 nodes")
                    .hasSize(6);
                
                long distinctGroups = allocation.getSearchSUs().stream()
                    .map(node -> node.split("-")[1])
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Index2 Shard 1: Should have exactly 2 groups")
                    .isEqualTo(2);
                
                return true;
            })
        );
    }

    @Test
    void testE2E_BinPacking_RespectsGroupCount_IgnoresReplicaCount() throws Exception {
        // Given: ShardAllocator with REAL GroupAwareBinPackingEngine (random ingester selection)
        ShardAllocator binPackingShardAllocator = new ShardAllocator(metadataStore);
        binPackingShardAllocator.setAllocationDecisionEngine(new GroupAwareBinPackingEngine());
        
        // Given: Index with 3 shards, using num_replicas_per_shard and num_groups_per_shard
        // Shard 0: replicaCount=2, groups=1  → should use 1 group, ignore replica count (use ALL nodes from group)
        // Shard 1: replicaCount=4, groups=2  → should use 2 groups, ignore replica count (use ALL nodes from both groups)
        // Shard 2: replicaCount=3, groups=1  → should use 1 group, ignore replica count (use ALL nodes from group)
        Index index = createIndexWithGroupsAndReplicas(
            "test-groups-replicas", 
            Arrays.asList(2, 4, 3),  // shardReplicaCount from num_replicas_per_shard
            Arrays.asList(1, 2, 1)   // shardGroupsAllocateCount from num_groups_per_shard
        );
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        
        // Given: No existing planned allocations
        when(metadataStore.getPlannedAllocation(eq(testClusterId), anyString(), anyString()))
            .thenReturn(null);
        
        // Given: Multiple replica groups available (5 nodes per group)
        // Group A: 5 nodes (for Shard 0 - needs 1 group)
        // Group B: 5 nodes (for Shard 1 - needs 2 groups)
        // Group C: 5 nodes (for Shard 1 - needs 2 groups)
        // Group D: 5 nodes (for Shard 2 - needs 1 group)
        List<SearchUnit> allNodes = Arrays.asList(
            // Shard 0 nodes
            createHealthyPrimaryNode("primary-0", "0"),
            createHealthyReplicaNode("replica-a-1", "group-a"),
            createHealthyReplicaNode("replica-a-2", "group-a"),
            createHealthyReplicaNode("replica-a-3", "group-a"),
            createHealthyReplicaNode("replica-a-4", "group-a"),
            createHealthyReplicaNode("replica-a-5", "group-a"),
            
            // Shard 1 nodes
            createHealthyPrimaryNode("primary-1", "1"),
            createHealthyReplicaNode("replica-b-1", "group-b"),
            createHealthyReplicaNode("replica-b-2", "group-b"),
            createHealthyReplicaNode("replica-b-3", "group-b"),
            createHealthyReplicaNode("replica-b-4", "group-b"),
            createHealthyReplicaNode("replica-b-5", "group-b"),
            createHealthyReplicaNode("replica-c-1", "group-c"),
            createHealthyReplicaNode("replica-c-2", "group-c"),
            createHealthyReplicaNode("replica-c-3", "group-c"),
            createHealthyReplicaNode("replica-c-4", "group-c"),
            createHealthyReplicaNode("replica-c-5", "group-c"),
            
            // Shard 2 nodes
            createHealthyPrimaryNode("primary-2", "2"),
            createHealthyReplicaNode("replica-d-1", "group-d"),
            createHealthyReplicaNode("replica-d-2", "group-d"),
            createHealthyReplicaNode("replica-d-3", "group-d"),
            createHealthyReplicaNode("replica-d-4", "group-d"),
            createHealthyReplicaNode("replica-d-5", "group-d")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation with USE_ALL_AVAILABLE_NODES strategy
        binPackingShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Verify Shard 0 - should use 1 group (as specified), ALL nodes from that group (ignoring replica count of 2)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("test-groups-replicas"),
            eq("0"),
            argThat(allocation -> {
                // Single writer constraint - random selection from available PRIMARY nodes
                assertThat(allocation.getIngestSUs())
                    .as("Shard 0: Single writer constraint, random ingester selection")
                    .hasSize(1)
                    .satisfies(ingestSUs -> assertThat(ingestSUs.get(0))
                        .as("Should select one PRIMARY node")
                        .isIn("primary-0", "primary-1", "primary-2"));  // Accept any PRIMARY (random selection)
                
                // Should use ALL nodes from 1 group (5 nodes), ignoring replica count of 2
                // Verify group count: all nodes should be from the same group
                long distinctGroups = allocation.getSearchSUs().stream()
                    .map(node -> node.split("-")[1])
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Shard 0: Should respect group count (1 group)")
                    .isEqualTo(1);
                
                // Verify node count: should use all nodes from the selected group, ignoring replica count of 2
                assertThat(allocation.getSearchSUs())
                    .as("Shard 0: Should ignore replica count (use all 5 nodes from 1 group, not 2)")
                    .hasSize(5);
                
                return true;
            })
        );
        
        // Then: Verify Shard 1 - should use 2 groups (as specified), ALL nodes from both groups (ignoring replica count of 4)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("test-groups-replicas"),
            eq("1"),
            argThat(allocation -> {
                // Single writer constraint - random selection from available PRIMARY nodes
                assertThat(allocation.getIngestSUs())
                    .as("Shard 1: Single writer constraint, random ingester selection")
                    .hasSize(1)
                    .satisfies(ingestSUs -> assertThat(ingestSUs.get(0))
                        .as("Should select one PRIMARY node")
                        .isIn("primary-0", "primary-1", "primary-2"));  // Accept any PRIMARY (random selection)
                
                // Should use ALL nodes from 2 groups (10 nodes total), ignoring replica count of 4
                // Verify group count: all nodes should be from exactly 2 groups
                long distinctGroups = allocation.getSearchSUs().stream()
                    .map(node -> node.split("-")[1])
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Shard 1: Should respect group count (2 groups)")
                    .isEqualTo(2);
                
                // Verify node count: should use all nodes from the selected groups, ignoring replica count of 4
                assertThat(allocation.getSearchSUs())
                    .as("Shard 1: Should ignore replica count (use all 10 nodes from 2 groups, not 4)")
                    .hasSize(10);
                
                return true;
            })
        );
        
        // Then: Verify Shard 2 - should use 1 group (as specified), ALL nodes from that group (ignoring replica count of 3)
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("test-groups-replicas"),
            eq("2"),
            argThat(allocation -> {
                // Single writer constraint - random selection from available PRIMARY nodes
                assertThat(allocation.getIngestSUs())
                    .as("Shard 2: Single writer constraint, random ingester selection")
                    .hasSize(1)
                    .satisfies(ingestSUs -> assertThat(ingestSUs.get(0))
                        .as("Should select one PRIMARY node")
                        .isIn("primary-0", "primary-1", "primary-2"));  // Accept any PRIMARY (random selection)
                
                // Should use ALL nodes from 1 group (5 nodes), ignoring replica count of 3
                // Verify group count: all nodes should be from the same group
                long distinctGroups = allocation.getSearchSUs().stream()
                    .map(node -> node.split("-")[1])
                    .distinct()
                    .count();
                assertThat(distinctGroups)
                    .as("Shard 2: Should respect group count (1 group)")
                    .isEqualTo(1);
                
                // Verify node count: should use all nodes from the selected group, ignoring replica count of 3
                assertThat(allocation.getSearchSUs())
                    .as("Shard 2: Should ignore replica count (use all 5 nodes from 1 group, not 3)")
                    .hasSize(5);
                
                return true;
            })
        );
    }

    @Test
    void testE2E_BinPacking_MultiWriter_WithZoneAntiAffinity() throws Exception {
        // Given: ShardAllocator with REAL GroupAwareBinPackingEngine
        ShardAllocator binPackingShardAllocator = new ShardAllocator(metadataStore);
        binPackingShardAllocator.setAllocationDecisionEngine(new GroupAwareBinPackingEngine());
        
        // Given: Index with multi-writer configuration (2 ingester groups per shard)
        Index index = createIndex("multi-writer-index", Arrays.asList(3));
        index.getSettings().setNumIngestGroupsPerShard(Arrays.asList(2));  // ← Multi-writer: 2 ingesters!
        index.getSettings().setNumGroupsPerShard(Arrays.asList(2));    // 2 replica groups
        when(metadataStore.getAllIndexConfigs(testClusterId)).thenReturn(Arrays.asList(index));
        
        // Given: No existing planned allocations
        when(metadataStore.getPlannedAllocation(testClusterId, "multi-writer-index", "0"))
            .thenReturn(null);
        
        // Given: Multiple PRIMARY groups with MIXED zones (for realistic zone anti-affinity testing)
        // Each PRIMARY group has nodes in DIFFERENT zones (not all in same zone)
        // This validates that zone anti-affinity actually selects nodes from different zones
        // PRIMARY Group 0: nodes in us-east-1, us-west-2, eu-west-1
        // PRIMARY Group 1: nodes in us-east-1, us-west-2, eu-west-1
        // PRIMARY Group 2: nodes in us-east-1, us-west-2, eu-west-1
        // REPLICA Groups: A, B (for searchers)
        List<SearchUnit> allNodes = Arrays.asList(
            // PRIMARY Group 0 (shard pool ID "0") - MIXED zones
            createHealthyPrimaryNodeWithZone("primary-pool-0-a", "0", "us-east-1"),
            createHealthyPrimaryNodeWithZone("primary-pool-0-b", "0", "us-west-2"),
            createHealthyPrimaryNodeWithZone("primary-pool-0-c", "0", "eu-west-1"),
            
            // PRIMARY Group 1 (shard pool ID "1") - MIXED zones
            createHealthyPrimaryNodeWithZone("primary-pool-1-a", "1", "us-east-1"),
            createHealthyPrimaryNodeWithZone("primary-pool-1-b", "1", "us-west-2"),
            createHealthyPrimaryNodeWithZone("primary-pool-1-c", "1", "eu-west-1"),
            
            // PRIMARY Group 2 (shard pool ID "2") - MIXED zones
            createHealthyPrimaryNodeWithZone("primary-pool-2-a", "2", "us-east-1"),
            createHealthyPrimaryNodeWithZone("primary-pool-2-b", "2", "us-west-2"),
            createHealthyPrimaryNodeWithZone("primary-pool-2-c", "2", "eu-west-1"),
            
            // REPLICA Groups (for searchers)
            createHealthyReplicaNode("replica-a-1", "group-a"),
            createHealthyReplicaNode("replica-a-2", "group-a"),
            createHealthyReplicaNode("replica-a-3", "group-a"),
            createHealthyReplicaNode("replica-b-1", "group-b"),
            createHealthyReplicaNode("replica-b-2", "group-b"),
            createHealthyReplicaNode("replica-b-3", "group-b")
        );
        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allNodes);
        
        // When: Plan allocation
        binPackingShardAllocator.planShardAllocation(testClusterId, AllocationStrategy.USE_ALL_AVAILABLE_NODES);
        
        // Then: Verify multi-writer allocation with zone anti-affinity
        verify(metadataStore).setPlannedAllocation(
            eq(testClusterId),
            eq("multi-writer-index"),
            eq("0"),
            argThat(allocation -> {
                // ========== INGESTER VALIDATION (Multi-Writer) ==========
                
                // Should have exactly 2 ingesters (multi-writer constraint)
                assertThat(allocation.getIngestSUs())
                    .as("Multi-writer: Should allocate exactly 2 ingesters")
                    .hasSize(2);
                
                // Extract selected ingesters
                List<String> selectedIngesters = allocation.getIngestSUs();
                
                // Verify both ingesters are from different PRIMARY groups
                String ingester1 = selectedIngesters.get(0);
                String ingester2 = selectedIngesters.get(1);
                
                // Extract group IDs (format: "primary-pool-{groupId}-{nodeId}")
                String group1 = ingester1.split("-")[2];  // "0", "1", or "2"
                String group2 = ingester2.split("-")[2];
                
                assertThat(group1)
                    .as("Multi-writer: Ingesters should be from DIFFERENT PRIMARY groups")
                    .isNotEqualTo(group2);
                
                // Verify ingesters are valid PRIMARY nodes from available groups
                assertThat(ingester1)
                    .as("Ingester 1 should be from a PRIMARY group")
                    .matches("primary-pool-[0-2]-[a-c]");
                
                assertThat(ingester2)
                    .as("Ingester 2 should be from a PRIMARY group")
                    .matches("primary-pool-[0-2]-[a-c]");
                
                // ========== ZONE ANTI-AFFINITY VALIDATION ==========
                
                // Map nodes to zones (now with MIXED zones per group)
                java.util.Map<String, String> nodeToZone = new java.util.HashMap<>();
                // Group 0 nodes spread across zones
                nodeToZone.put("primary-pool-0-a", "us-east-1");
                nodeToZone.put("primary-pool-0-b", "us-west-2");
                nodeToZone.put("primary-pool-0-c", "eu-west-1");
                // Group 1 nodes spread across zones
                nodeToZone.put("primary-pool-1-a", "us-east-1");
                nodeToZone.put("primary-pool-1-b", "us-west-2");
                nodeToZone.put("primary-pool-1-c", "eu-west-1");
                // Group 2 nodes spread across zones
                nodeToZone.put("primary-pool-2-a", "us-east-1");
                nodeToZone.put("primary-pool-2-b", "us-west-2");
                nodeToZone.put("primary-pool-2-c", "eu-west-1");
                
                String zone1 = nodeToZone.get(ingester1);
                String zone2 = nodeToZone.get(ingester2);
                
                assertThat(zone1)
                    .as("Zone anti-affinity: Ingesters should be in DIFFERENT zones (preferred)")
                    .isNotNull();
                
                assertThat(zone2)
                    .as("Zone anti-affinity: Ingesters should be in DIFFERENT zones (preferred)")
                    .isNotNull();
                
                // Zone anti-affinity: prefer different zones (best effort)
                // With mixed zones per group, the ZoneAntiAffinityDecider should actively choose
                // nodes from different zones when selecting from the 2 groups
                assertThat(zone1)
                    .as("Zone anti-affinity: ZoneAntiAffinityDecider should select nodes in different zones when possible")
                    .isNotEqualTo(zone2);
                
                // ========== REPLICA VALIDATION ==========
                
                // Should allocate to 2 replica groups
                assertThat(allocation.getSearchSUs())
                    .as("Should allocate to 2 replica groups × 3 nodes = 6 nodes")
                    .hasSize(6);
                
                long distinctReplicaGroups = allocation.getSearchSUs().stream()
                    .map(node -> node.split("-")[1])  // Extract group ID
                    .distinct()
                    .count();
                
                assertThat(distinctReplicaGroups)
                    .as("Should have exactly 2 replica groups")
                    .isEqualTo(2);
                
                return true;
            })
        );
    }

    // Helper method to create PRIMARY nodes with zones
    private SearchUnit createHealthyPrimaryNodeWithZone(String name, String shardId, String zone) {
        SearchUnit searchUnit = new SearchUnit();
        searchUnit.setId(name);
        searchUnit.setName(name);
        searchUnit.setShardId(shardId);
        searchUnit.setRole("PRIMARY");
        searchUnit.setZone(zone);  // ← Set zone for anti-affinity
        searchUnit.setStatePulled(HealthState.GREEN);
        searchUnit.setStateAdmin("NORMAL");
        return searchUnit;
    }
}
