package io.clustercontroller.allocation;

import io.clustercontroller.enums.ShardState;
import io.clustercontroller.metrics.MetricsProvider;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.store.MetadataStore;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests for ActualAllocationUpdater cleanup logic (PHASE 1 and PHASE 2).
 */
class ActualAllocationUpdaterCleanupTest {
    
    @Mock
    private MetadataStore metadataStore;
    
    @Mock
    private MetricsProvider metricsProvider;
    
    @Mock
    private Counter mockCounter;
    
    private ActualAllocationUpdater updater;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        updater = new ActualAllocationUpdater(metadataStore, metricsProvider);
        // Use lenient() for metrics since not all tests will trigger metric emissions
        lenient().when(metricsProvider.counter(anyString(), anyMap())).thenReturn(mockCounter);
    }
    
    // ========== PHASE 1 TESTS: Existing Index - Selective Cleanup ==========
    
    @Test
    void testPhase1_ExistingIndex_ClearsStaleShards() throws Exception {
        // Given: An existing index with stored allocations but nodes not reporting them anymore
        String clusterId = "test-cluster";
        String indexName = "active-index";
        
        // Search unit exists but reports empty actual state (no allocations)
        SearchUnit unit = createSearchUnit("node1");
        SearchUnitActualState emptyState = new SearchUnitActualState();
        emptyState.setNodeRouting(new HashMap<>());
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(unit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(emptyState);
        
        // Index config exists
        Index indexConfig = createIndex(indexName, 2);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(List.of(indexConfig));
        
        // Mock that the index has actual-allocation entries in etcd
        when(metadataStore.getAllIndicesWithActualAllocations(clusterId))
            .thenReturn(Collections.singleton(indexName));
        
        // Stored actual allocations in etcd (stale)
        ShardAllocation storedShard0 = createShardAllocation("0", List.of("node1"), List.of());
        ShardAllocation storedShard1 = createShardAllocation("1", List.of("node1"), List.of());
        when(metadataStore.getAllActualAllocations(clusterId, indexName))
            .thenReturn(List.of(storedShard0, storedShard1));
        
        // When
        updater.updateActualAllocations(clusterId);
        
        // Then: Both shards should be cleared with empty lists (PHASE 1 behavior)
        verify(metadataStore, times(2)).setActualAllocation(eq(clusterId), eq(indexName), anyString(), argThat(allocation -> 
            allocation.getIngestSUs().isEmpty() && allocation.getSearchSUs().isEmpty()
        ));
        
        // But NO deletions should occur (PHASE 1 only clears, doesn't delete)
        verify(metadataStore, never()).deleteActualAllocation(anyString(), anyString(), anyString());
    }
    
    // ========== PHASE 2 TESTS: Deleted Index - Full Cleanup ==========
    
    @Test
    void testPhase2_DeletedIndex_DeletesAllAllocations() throws Exception {
        // Given: A deleted index (no config) but nodes still report having its shards
        String clusterId = "test-cluster";
        String deletedIndex = "deleted-index";
        
        // Node reports having shards from the deleted index
        SearchUnit unit = createSearchUnit("node1");
        SearchUnitActualState actualState = createActualStateWithShards(deletedIndex, List.of(0, 1, 2));
        
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(unit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState);
        
        // NO config exists for deleted-index (this makes it a deleted index)
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());
        
        // Mock that the deleted index has actual-allocation entries in etcd
        when(metadataStore.getAllIndicesWithActualAllocations(clusterId))
            .thenReturn(Collections.singleton(deletedIndex));
        
        // The deleted index has actual allocations stored in etcd (orphaned entries)
        ShardAllocation shard0 = createShardAllocation("0", List.of("node1"), List.of());
        ShardAllocation shard1 = createShardAllocation("1", List.of("node1"), List.of());
        ShardAllocation shard2 = createShardAllocation("2", List.of("node1"), List.of());
        
        when(metadataStore.getAllActualAllocations(clusterId, deletedIndex))
            .thenReturn(List.of(shard0, shard1, shard2));
        
        // When
        updater.updateActualAllocations(clusterId);
        
        // Then: Verify getAllIndexConfigs was called (should return empty for deleted index)
        verify(metadataStore, atLeastOnce()).getAllIndexConfigs(clusterId);
        
        // Main update will set allocations for what nodes report
        verify(metadataStore, times(3)).setActualAllocation(eq(clusterId), eq(deletedIndex), anyString(), any());
        
        // Then PHASE 2: All actual allocations for deleted index should be DELETED
        verify(metadataStore).deleteActualAllocation(eq(clusterId), eq(deletedIndex), eq("0"));
        verify(metadataStore).deleteActualAllocation(eq(clusterId), eq(deletedIndex), eq("1"));
        verify(metadataStore).deleteActualAllocation(eq(clusterId), eq(deletedIndex), eq("2"));
    }
    
    
    @Test
    void testMetricsCounter_IncrementedOnAllocationUpdateFailure() throws Exception {
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";
        
        SearchUnit unit = createSearchUnit("node1");
        SearchUnitActualState actualState = createActualStateWithShards(indexName, List.of(0));
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(unit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState);
        Index indexConfig = createIndex(indexName, 1);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(List.of(indexConfig));
        when(metadataStore.getAllIndicesWithActualAllocations(clusterId))
            .thenReturn(Collections.singleton(indexName));
        when(metadataStore.getActualAllocation(clusterId, indexName, shardId))
            .thenThrow(new RuntimeException("Database connection failed"));

        updater.updateActualAllocations(clusterId);
        
        verify(metricsProvider).counter(
            eq("update_actual_allocation_failures_count"),
            argThat(tags -> 
                tags.get("clusterId").equals(clusterId) &&
                tags.get("indexName").equals(indexName) &&
                tags.get("shardId").equals(shardId)
            )
        );
        verify(mockCounter).increment();
    }
    
    @Test
    void testMetricsCounter_NotIncrementedOnSuccess() throws Exception {
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        SearchUnit unit = createSearchUnit("node1");
        unit.setRole("PRIMARY");
        SearchUnitActualState actualState = createActualStateWithShards(indexName, List.of(0));
        
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(List.of(unit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState);
        when(metadataStore.getSearchUnit(clusterId, "node1"))
            .thenReturn(java.util.Optional.of(unit));
        Index indexConfig = createIndex(indexName, 1);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(List.of(indexConfig));
        when(metadataStore.getAllIndicesWithActualAllocations(clusterId))
            .thenReturn(Collections.singleton(indexName));
        when(metadataStore.getActualAllocation(clusterId, indexName, "0")).thenReturn(null);
        doNothing().when(metadataStore).setActualAllocation(eq(clusterId), eq(indexName), eq("0"), any());
        
        updater.updateActualAllocations(clusterId);
        verify(metricsProvider, never()).counter(eq("update_actual_allocation_failures_count"), anyMap());
        verify(mockCounter, never()).increment();
    }
    // Helper methods
    private Index createIndex(String indexName, int numberOfShards) {
        Index index = new Index();
        index.setIndexName(indexName);
        
        IndexSettings settings = new IndexSettings();
        settings.setNumberOfShards(numberOfShards);
        index.setSettings(settings);
        
        return index;
    }
    
    private SearchUnit createSearchUnit(String unitName) {
        SearchUnit unit = new SearchUnit();
        unit.setName(unitName);
        unit.setStateAdmin("NORMAL"); // Required for collectActualAllocations
        return unit;
    }
    
    private SearchUnitActualState createActualStateWithShards(String indexName, List<Integer> shardIds) {
        SearchUnitActualState actualState = new SearchUnitActualState();
        actualState.setTimestamp(System.currentTimeMillis()); // Recent timestamp for health check
        
        Map<String, List<SearchUnitActualState.ShardRoutingInfo>> nodeRouting = new HashMap<>();
        
        List<SearchUnitActualState.ShardRoutingInfo> shards = new java.util.ArrayList<>();
        for (Integer shardId : shardIds) {
            SearchUnitActualState.ShardRoutingInfo shard = new SearchUnitActualState.ShardRoutingInfo();
            shard.setShardId(shardId);
            shard.setState(ShardState.STARTED); // Must be STARTED to be included
            shard.setRole("primary");
            shards.add(shard);
        }
        
        nodeRouting.put(indexName, shards);
        actualState.setNodeRouting(nodeRouting);
        return actualState;
    }
    
    private ShardAllocation createShardAllocation(String shardId, java.util.List<String> ingestSUs, java.util.List<String> searchSUs) {
        ShardAllocation allocation = new ShardAllocation();
        allocation.setShardId(shardId);
        allocation.setIngestSUs(ingestSUs);
        allocation.setSearchSUs(searchSUs);
        return allocation;
    }
}

