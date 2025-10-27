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
}
