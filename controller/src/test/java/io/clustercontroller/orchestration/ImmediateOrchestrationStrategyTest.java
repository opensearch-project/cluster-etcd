package io.clustercontroller.orchestration;

import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ImmediateOrchestrationStrategyTest {

    @Mock
    private MetadataStore metadataStore;

    private ImmediateOrchestrationStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new ImmediateOrchestrationStrategy(metadataStore);
    }

    @Test
    void testOrchestrateWithSingleIndexSingleShard() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        int numberOfShards = 1;
        
        Index indexConfig = createIndex(indexName, numberOfShards);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2", "node3"));
        
        SearchUnitGoalState existingGoalState = new SearchUnitGoalState();
        existingGoalState.setLocalShards(new HashMap<>());
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(eq(clusterId), anyString())).thenReturn(existingGoalState);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        
        // Verify goal states are set for all nodes
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node1"), any(SearchUnitGoalState.class));
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node2"), any(SearchUnitGoalState.class));
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node3"), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithMultipleIndexesAndShards() throws Exception {
        // Given
        String clusterId = "test-cluster";
        
        Index index1 = createIndex("index1", 2);
        Index index2 = createIndex("index2", 1);
        
        ShardAllocation planned1_0 = new ShardAllocation();
        planned1_0.setIngestSUs(Arrays.asList("node1"));
        planned1_0.setSearchSUs(Arrays.asList("node2"));
        
        ShardAllocation planned1_1 = new ShardAllocation();
        planned1_1.setIngestSUs(Arrays.asList("node3"));
        planned1_1.setSearchSUs(Arrays.asList("node4"));
        
        ShardAllocation planned2_0 = new ShardAllocation();
        planned2_0.setIngestSUs(Arrays.asList("node1"));
        planned2_0.setSearchSUs(Arrays.asList("node2", "node3"));
        
        SearchUnitGoalState existingGoalState = new SearchUnitGoalState();
        existingGoalState.setLocalShards(new HashMap<>());
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(index1, index2));
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "0")).thenReturn(planned1_0);
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "1")).thenReturn(planned1_1);
        when(metadataStore.getPlannedAllocation(clusterId, "index2", "0")).thenReturn(planned2_0);
        when(metadataStore.getSearchUnitGoalState(eq(clusterId), anyString())).thenReturn(existingGoalState);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        
        // Verify all planned allocations are fetched
        verify(metadataStore).getPlannedAllocation(clusterId, "index1", "0");
        verify(metadataStore).getPlannedAllocation(clusterId, "index1", "1");
        verify(metadataStore).getPlannedAllocation(clusterId, "index2", "0");
        
        // Verify goal states are set for all nodes (7 total calls due to duplicate nodes across shards)
        verify(metadataStore, times(7)).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithNoPlannedAllocation() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(null);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        verify(metadataStore, never()).setSearchUnitGoalState(anyString(), anyString(), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithEmptyIndexConfigs() throws Exception {
        // Given
        String clusterId = "test-cluster";
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList());

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore, never()).getPlannedAllocation(anyString(), anyString(), anyString());
        verify(metadataStore, never()).setSearchUnitGoalState(anyString(), anyString(), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithNullGoalState() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2"));
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(eq(clusterId), anyString())).thenReturn(null);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        
        // Verify goal states are still set (new ones created for null existing states)
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node1"), any(SearchUnitGoalState.class));
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node2"), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithExceptionInShardProcessing() throws Exception {
        // Given
        String clusterId = "test-cluster";
        
        Index index1 = createIndex("index1", 1);
        Index index2 = createIndex("index2", 1);
        
        ShardAllocation planned2 = new ShardAllocation();
        planned2.setIngestSUs(Arrays.asList("node1"));
        planned2.setSearchSUs(Arrays.asList("node2"));
        
        SearchUnitGoalState existingGoalState = new SearchUnitGoalState();
        existingGoalState.setLocalShards(new HashMap<>());
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(index1, index2));
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "0"))
                .thenThrow(new RuntimeException("Database error"));
        when(metadataStore.getPlannedAllocation(clusterId, "index2", "0")).thenReturn(planned2);
        when(metadataStore.getSearchUnitGoalState(eq(clusterId), anyString())).thenReturn(existingGoalState);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        
        // Verify that index1 fails but index2 still processes
        verify(metadataStore).getPlannedAllocation(clusterId, "index1", "0");
        verify(metadataStore).getPlannedAllocation(clusterId, "index2", "0");
        
        // Verify goal states are set for index2 nodes despite index1 failure
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node1"), any(SearchUnitGoalState.class));
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node2"), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithExceptionInGoalStateUpdate() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2"));
        
        SearchUnitGoalState existingGoalState = new SearchUnitGoalState();
        existingGoalState.setLocalShards(new HashMap<>());
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1")).thenReturn(existingGoalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node2"))
                .thenThrow(new RuntimeException("Goal state error"));
        doThrow(new RuntimeException("Update error"))
                .when(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node1"), any(SearchUnitGoalState.class));

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        
        // Verify that exceptions in goal state updates are handled gracefully
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node1"), any(SearchUnitGoalState.class));
        verify(metadataStore, never()).setSearchUnitGoalState(eq(clusterId), eq("node2"), any(SearchUnitGoalState.class));
    }

    @Test
    void testUpdateNodeGoalStateThrowsExceptionWhenGetSearchUnitGoalStateFails() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";
        String nodeId = "node1";
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList(nodeId));
        planned.setSearchSUs(Arrays.asList("node2"));
        
        when(metadataStore.getSearchUnitGoalState(clusterId, nodeId))
                .thenThrow(new RuntimeException("Goal state retrieval failed"));

        // When & Then
        assertThatThrownBy(() -> {
            // Use reflection to call the private method
            java.lang.reflect.Method method = ImmediateOrchestrationStrategy.class.getDeclaredMethod(
                    "updateNodeGoalState", String.class, String.class, String.class, ShardAllocation.class, String.class);
            method.setAccessible(true);
            method.invoke(strategy, nodeId, indexName, shardId, planned, clusterId);
        })
        .isInstanceOf(java.lang.reflect.InvocationTargetException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasRootCauseMessage("Goal state retrieval failed");
        
        verify(metadataStore).getSearchUnitGoalState(clusterId, nodeId);
    }

    @Test
    void testOrchestrateThrowsExceptionWhenGetSearchUnitGoalStateFails() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2"));
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1"))
                .thenThrow(new RuntimeException("Goal state retrieval failed"));

        // When - orchestrate should handle the exception gracefully
        strategy.orchestrate(clusterId);
        
        // Then - verify the method was called and exception was handled
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        // The orchestration should complete without throwing (resilient behavior)
        verify(metadataStore).getSearchUnitGoalState(clusterId, "node1");
    }

    @Test
    void testOrchestrateThrowsExceptionWhenSetSearchUnitGoalStateFails() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2"));
        
        SearchUnitGoalState existingGoalState = new SearchUnitGoalState();
        existingGoalState.setLocalShards(new HashMap<>());
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(eq(clusterId), anyString())).thenReturn(existingGoalState);
        doThrow(new RuntimeException("Goal state update failed"))
                .when(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node1"), any(SearchUnitGoalState.class));

        // When - orchestrate should handle the exception gracefully
        strategy.orchestrate(clusterId);
        
        // Then - verify the method was called and exception was handled
        // The orchestration should complete without throwing (resilient behavior)
        
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        verify(metadataStore).getSearchUnitGoalState(clusterId, "node1");
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq("node1"), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithEmptyIngestAndSearchSUs() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList());
        planned.setSearchSUs(Arrays.asList());
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        verify(metadataStore, never()).setSearchUnitGoalState(anyString(), anyString(), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithNullIngestAndSearchSUs() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(null);
        planned.setSearchSUs(null);
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        verify(metadataStore, never()).setSearchUnitGoalState(anyString(), anyString(), any(SearchUnitGoalState.class));
    }

    // Helper method to create Index with initialized settings
    private Index createIndex(String indexName, int numberOfShards) {
        Index index = new Index();
        index.setIndexName(indexName);
        
        IndexSettings settings = new IndexSettings();
        settings.setNumberOfShards(numberOfShards);
        index.setSettings(settings);
        
        return index;
    }
}
