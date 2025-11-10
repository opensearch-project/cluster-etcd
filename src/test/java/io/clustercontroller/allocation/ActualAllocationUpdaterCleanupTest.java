package io.clustercontroller.allocation;

import io.clustercontroller.enums.ShardState;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
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
    
    private ActualAllocationUpdater updater;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        updater = new ActualAllocationUpdater(metadataStore);
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
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(Arrays.asList(unit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(emptyState);
        
        // Index config exists
        Index indexConfig = createIndex(indexName, 2);
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        
        // Stored actual allocations in etcd (stale)
        ShardAllocation storedShard0 = createShardAllocation("0", Arrays.asList("node1"), Arrays.asList());
        ShardAllocation storedShard1 = createShardAllocation("1", Arrays.asList("node1"), Arrays.asList());
        when(metadataStore.getAllActualAllocations(clusterId, indexName))
            .thenReturn(Arrays.asList(storedShard0, storedShard1));
        
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
        SearchUnitActualState actualState = createActualStateWithShards(deletedIndex, Arrays.asList(0, 1, 2));
        
        when(metadataStore.getAllSearchUnits(clusterId)).thenReturn(Arrays.asList(unit));
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState);
        
        // NO config exists for deleted-index (this makes it a deleted index)
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());
        
        // The deleted index has actual allocations stored in etcd (orphaned entries)
        ShardAllocation shard0 = createShardAllocation("0", Arrays.asList("node1"), Arrays.asList());
        ShardAllocation shard1 = createShardAllocation("1", Arrays.asList("node1"), Arrays.asList());
        ShardAllocation shard2 = createShardAllocation("2", Arrays.asList("node1"), Arrays.asList());
        
        when(metadataStore.getAllActualAllocations(clusterId, deletedIndex))
            .thenReturn(Arrays.asList(shard0, shard1, shard2));
        
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

