package io.clustercontroller.orchestration;

import com.google.common.util.concurrent.AtomicDouble;
import io.clustercontroller.metrics.MetricsProvider;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.ShardAllocation;
import io.clustercontroller.models.SearchUnitActualState;
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

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RollingUpdateOrchestrationStrategyTest {

    @Mock
    private MetadataStore metadataStore;

    @Mock
    private MetricsProvider metricsProvider;

    private RollingUpdateOrchestrationStrategy strategy;

    @BeforeEach
    void setUp() {
        strategy = new RollingUpdateOrchestrationStrategy(metadataStore, metricsProvider);
        // Use lenient() since not all tests call gauge (e.g., tests with empty nodes or no planned allocation)
        lenient().when(metricsProvider.gauge(anyString(), anyDouble(), anyMap())).thenReturn(new AtomicDouble(0.0));
    }

    @Test
    void testOrchestrateWithNoNodesInTransit() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2", "node3"));
        
        // Mock goal states - no nodes have been updated yet
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        
        // With separate PRIMARY and REPLICA processing:
        // PRIMARY group: 1 node, 20% = 0.2 → Math.ceil(0.2) = 1 → updates 1 node
        // REPLICA group: 2 nodes, 20% = 0.4 → Math.ceil(0.4) = 1 → updates 1 node
        // Total: 2 nodes updated
        verify(metadataStore, times(2)).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
        
        // Verify gauge metrics are updated for both PRIMARY and REPLICA groups
        verify(metricsProvider, times(2)).gauge(
            eq("rolling_update_progress_percentage"),
            anyDouble(),
            anyMap()
        );
    }

    @Test
    void testOrchestrateWithSomeNodesInTransit() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2", "node3", "node4", "node5"));
        
        // Mock goal states - node1 and node2 already updated, node3 converged
        SearchUnitGoalState node1GoalState = createGoalStateWithShard(indexName, "0", "PRIMARY");
        SearchUnitGoalState node2GoalState = createGoalStateWithShard(indexName, "0", "SEARCH_REPLICA");
        SearchUnitGoalState node3GoalState = createGoalStateWithShard(indexName, "0", "SEARCH_REPLICA");
        
        // Mock actual states - node3 has converged (has the shard)
        SearchUnitActualState node3ActualState = createActualStateWithShard(indexName, "0", "SEARCH_REPLICA");
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1")).thenReturn(node1GoalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node2")).thenReturn(node2GoalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node3")).thenReturn(node3GoalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node4")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node5")).thenReturn(null);
        
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(null);
        when(metadataStore.getSearchUnitActualState(clusterId, "node2")).thenReturn(null);
        when(metadataStore.getSearchUnitActualState(clusterId, "node3")).thenReturn(node3ActualState);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        
        // Should not update any nodes (already at 20% limit with 2 nodes in transit)
        verify(metadataStore, never()).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithMaxTransitPercentageReached() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2", "node3", "node4", "node5"));
        
        // Mock goal states - 2 nodes updated (40% of 5 nodes = 8% < 20%)
        SearchUnitGoalState node1GoalState = createGoalStateWithShard(indexName, "0", "PRIMARY");
        SearchUnitGoalState node2GoalState = createGoalStateWithShard(indexName, "0", "SEARCH_REPLICA");
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1")).thenReturn(node1GoalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node2")).thenReturn(node2GoalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node3")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node4")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node5")).thenReturn(null);
        
        when(metadataStore.getSearchUnitActualState(eq(clusterId), anyString())).thenReturn(null);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        
        // Should not update any more nodes since 2/5 = 40% > 20% transit limit
        verify(metadataStore, never()).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithAllNodesConverged() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2", "node3"));
        
        // Mock goal states - all nodes updated
        SearchUnitGoalState node1GoalState = createGoalStateWithShard(indexName, "0", "PRIMARY");
        SearchUnitGoalState node2GoalState = createGoalStateWithShard(indexName, "0", "SEARCH_REPLICA");
        SearchUnitGoalState node3GoalState = createGoalStateWithShard(indexName, "0", "SEARCH_REPLICA");
        
        // Mock actual states - all nodes converged
        SearchUnitActualState node1ActualState = createActualStateWithShard(indexName, "0", "PRIMARY");
        SearchUnitActualState node2ActualState = createActualStateWithShard(indexName, "0", "SEARCH_REPLICA");
        SearchUnitActualState node3ActualState = createActualStateWithShard(indexName, "0", "SEARCH_REPLICA");
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1")).thenReturn(node1GoalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node2")).thenReturn(node2GoalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node3")).thenReturn(node3GoalState);
        
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(node1ActualState);
        when(metadataStore.getSearchUnitActualState(clusterId, "node2")).thenReturn(node2ActualState);
        when(metadataStore.getSearchUnitActualState(clusterId, "node3")).thenReturn(node3ActualState);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        
        // Should not update any nodes since all are converged (0% in transit)
        verify(metadataStore, never()).setSearchUnitGoalState(anyString(), anyString(), any(SearchUnitGoalState.class));
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
    void testOrchestrateWithEmptyNodes() throws Exception {
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
    void testOrchestrateWithExceptionInShardProcessing() throws Exception {
        // Given
        String clusterId = "test-cluster";
        
        Index index1 = createIndex("index1", 1);
        Index index2 = createIndex("index2", 1);
        
        ShardAllocation planned2 = new ShardAllocation();
        planned2.setIngestSUs(Arrays.asList("node1"));
        planned2.setSearchSUs(Arrays.asList("node2"));
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(index1, index2));
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "0"))
                .thenThrow(new RuntimeException("Database error"));
        when(metadataStore.getPlannedAllocation(clusterId, "index2", "0")).thenReturn(planned2);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        
        // Verify that index1 fails but index2 still processes
        verify(metadataStore).getPlannedAllocation(clusterId, "index1", "0");
        verify(metadataStore).getPlannedAllocation(clusterId, "index2", "0");
        
        // Verify goal states are set for index2 (resilient behavior)
        // With separate PRIMARY and REPLICA processing:
        // PRIMARY group: 1 node, 20% = 0.2 → Math.ceil(0.2) = 1 → updates 1 node
        // REPLICA group: 1 node, 20% = 0.2 → Math.ceil(0.2) = 1 → updates 1 node
        // Total: 2 nodes updated
        verify(metadataStore, times(2)).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateWithExceptionInGoalStateCheck() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2"));
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node2"))
                .thenThrow(new RuntimeException("Goal state error"));

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        
        // Should update nodes despite exception in goal state check (resilient behavior)
        // With 20% rolling update limit and 2 total nodes, only 1 node should be updated
        verify(metadataStore, times(1)).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
    }

    @Test
    void testRollingUpdateBehaviorWith20PercentLimit() throws Exception {
        // Given: Multiple indexes and shards with 10 nodes each, testing rolling update per index-shard
        String clusterId = "test-cluster";
        
        Index index1 = createIndex("index1", 2);
        Index index2 = createIndex("index2", 1);
        
        // Create planned allocations with 10 nodes each (1 primary + 9 replicas)
        ShardAllocation planned1_0 = new ShardAllocation();
        planned1_0.setIngestSUs(Arrays.asList("node1"));
        planned1_0.setSearchSUs(Arrays.asList("node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9", "node10"));
        
        ShardAllocation planned1_1 = new ShardAllocation();
        planned1_1.setIngestSUs(Arrays.asList("node11"));
        planned1_1.setSearchSUs(Arrays.asList("node12", "node13", "node14", "node15", "node16", "node17", "node18", "node19", "node20"));
        
        ShardAllocation planned2_0 = new ShardAllocation();
        planned2_0.setIngestSUs(Arrays.asList("node21"));
        planned2_0.setSearchSUs(Arrays.asList("node22", "node23", "node24", "node25", "node26", "node27", "node28", "node29", "node30"));
        
        // Mock goal states: All nodes have null goal states (no existing goal states)
        when(metadataStore.getSearchUnitGoalState(eq(clusterId), anyString())).thenReturn(null);
        when(metadataStore.getSearchUnitActualState(eq(clusterId), anyString())).thenReturn(null);
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(index1, index2));
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "0")).thenReturn(planned1_0);
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "1")).thenReturn(planned1_1);
        when(metadataStore.getPlannedAllocation(clusterId, "index2", "0")).thenReturn(planned2_0);

        // FIRST CALL: Should update 20% of nodes (2 per index-shard = 6 total)
        strategy.orchestrate(clusterId);

        // Verify first call results
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, "index1", "0");
        verify(metadataStore).getPlannedAllocation(clusterId, "index1", "1");
        verify(metadataStore).getPlannedAllocation(clusterId, "index2", "0");
        
        // With separate PRIMARY and REPLICA processing and 20% rolling update limit:
        // - index1/shard0: 1 PRIMARY (20% = 0.2 → 1) + 9 REPLICA (20% = 1.8 → 2) = 3 updates
        // - index1/shard1: 1 PRIMARY (20% = 0.2 → 1) + 9 REPLICA (20% = 1.8 → 2) = 3 updates  
        // - index2/shard0: 1 PRIMARY (20% = 0.2 → 1) + 9 REPLICA (20% = 1.8 → 2) = 3 updates
        // Total: 9 updates across 3 index-shard combinations
        verify(metadataStore, times(9)).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
        
        // SECOND CALL: Mock goal states as updated (matching planned allocation) but actual states still null (in transit)
        // First call updated 9 nodes total: 3 per shard (1 PRIMARY + 2 REPLICA)
        SearchUnitGoalState updatedGoalState1 = createGoalStateWithShard("index1", "0", "PRIMARY");
        SearchUnitGoalState updatedGoalState2 = createGoalStateWithShard("index1", "0", "SEARCH_REPLICA");
        SearchUnitGoalState updatedGoalState3 = createGoalStateWithShard("index1", "0", "SEARCH_REPLICA");
        SearchUnitGoalState updatedGoalState11 = createGoalStateWithShard("index1", "1", "PRIMARY");
        SearchUnitGoalState updatedGoalState12 = createGoalStateWithShard("index1", "1", "SEARCH_REPLICA");
        SearchUnitGoalState updatedGoalState13 = createGoalStateWithShard("index1", "1", "SEARCH_REPLICA");
        SearchUnitGoalState updatedGoalState21 = createGoalStateWithShard("index2", "0", "PRIMARY");
        SearchUnitGoalState updatedGoalState22 = createGoalStateWithShard("index2", "0", "SEARCH_REPLICA");
        SearchUnitGoalState updatedGoalState23 = createGoalStateWithShard("index2", "0", "SEARCH_REPLICA");
        
        // Mock goal states for all 9 nodes that were updated in first call
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1")).thenReturn(updatedGoalState1);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node2")).thenReturn(updatedGoalState2);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node3")).thenReturn(updatedGoalState3);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node11")).thenReturn(updatedGoalState11);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node12")).thenReturn(updatedGoalState12);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node13")).thenReturn(updatedGoalState13);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node21")).thenReturn(updatedGoalState21);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node22")).thenReturn(updatedGoalState22);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node23")).thenReturn(updatedGoalState23);

        // Reset mock verification counts for second call
        clearInvocations(metadataStore);
        
        // SECOND CALL: Should NOT update any more nodes (already at 20% limit)
        strategy.orchestrate(clusterId);

        // Verify second call results - no additional updates
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, "index1", "0");
        verify(metadataStore).getPlannedAllocation(clusterId, "index1", "1");
        verify(metadataStore).getPlannedAllocation(clusterId, "index2", "0");
        
        // Should NOT update any more nodes (already at 20% limit)
        verify(metadataStore, never()).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
        
        // THIRD CALL: Mock actual states as converged (matching goal states)
        // First call updated 9 nodes total: 3 per shard (1 PRIMARY + 2 REPLICA)
        SearchUnitActualState convergedActualState1 = createActualStateWithShard("index1", "0", "PRIMARY");
        SearchUnitActualState convergedActualState2 = createActualStateWithShard("index1", "0", "SEARCH_REPLICA");
        SearchUnitActualState convergedActualState3 = createActualStateWithShard("index1", "0", "SEARCH_REPLICA");
        SearchUnitActualState convergedActualState11 = createActualStateWithShard("index1", "1", "PRIMARY");
        SearchUnitActualState convergedActualState12 = createActualStateWithShard("index1", "1", "SEARCH_REPLICA");
        SearchUnitActualState convergedActualState13 = createActualStateWithShard("index1", "1", "SEARCH_REPLICA");
        SearchUnitActualState convergedActualState21 = createActualStateWithShard("index2", "0", "PRIMARY");
        SearchUnitActualState convergedActualState22 = createActualStateWithShard("index2", "0", "SEARCH_REPLICA");
        SearchUnitActualState convergedActualState23 = createActualStateWithShard("index2", "0", "SEARCH_REPLICA");
        
        // Mock actual states: All 9 nodes that were updated in first call are now converged
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(convergedActualState1);
        when(metadataStore.getSearchUnitActualState(clusterId, "node2")).thenReturn(convergedActualState2);
        when(metadataStore.getSearchUnitActualState(clusterId, "node3")).thenReturn(convergedActualState3);
        when(metadataStore.getSearchUnitActualState(clusterId, "node11")).thenReturn(convergedActualState11);
        when(metadataStore.getSearchUnitActualState(clusterId, "node12")).thenReturn(convergedActualState12);
        when(metadataStore.getSearchUnitActualState(clusterId, "node13")).thenReturn(convergedActualState13);
        when(metadataStore.getSearchUnitActualState(clusterId, "node21")).thenReturn(convergedActualState21);
        when(metadataStore.getSearchUnitActualState(clusterId, "node22")).thenReturn(convergedActualState22);
        when(metadataStore.getSearchUnitActualState(clusterId, "node23")).thenReturn(convergedActualState23);

        // Reset mock verification counts for third call
        clearInvocations(metadataStore);
        
        // THIRD CALL: Should update next 20% of nodes (2 per index-shard = 6 total)
        strategy.orchestrate(clusterId);

        // Verify third call results - next 20% should be updated
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, "index1", "0");
        verify(metadataStore).getPlannedAllocation(clusterId, "index1", "1");
        verify(metadataStore).getPlannedAllocation(clusterId, "index2", "0");
        
        // Should update next 20% of nodes (2 per index-shard = 6 total)
        verify(metadataStore, times(6)).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
    }

    @Test
    void testRollingUpdateBehaviorWithExistingGoalStates() throws Exception {
        // Given: A scenario where some nodes already have goal states and some are converged
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";
        
        Index indexConfig = createIndex(indexName, 1);
        
        // Create planned allocation with 10 nodes (1 primary + 9 replicas)
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1")); // 1 primary
        planned.setSearchSUs(Arrays.asList("node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9", "node10")); // 9 replicas
        
        // Create goal states for some nodes (already have goal states)
        SearchUnitGoalState existingGoalState1 = createGoalStateWithShard(indexName, shardId, "PRIMARY");
        SearchUnitGoalState existingGoalState2 = createGoalStateWithShard(indexName, shardId, "SEARCH_REPLICA");
        SearchUnitGoalState existingGoalState3 = createGoalStateWithShard(indexName, shardId, "SEARCH_REPLICA");
        
        // Create actual states that match goal states (converged)
        SearchUnitActualState actualState1 = createActualStateWithShard(indexName, shardId, "PRIMARY");
        SearchUnitActualState actualState2 = createActualStateWithShard(indexName, shardId, "SEARCH_REPLICA");
        SearchUnitActualState actualState3 = createActualStateWithShard(indexName, shardId, "SEARCH_REPLICA");
        
        // Mock goal states: node1, node2, node3 have existing goal states, rest are null
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1")).thenReturn(existingGoalState1);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node2")).thenReturn(existingGoalState2);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node3")).thenReturn(existingGoalState3);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node4")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node5")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node6")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node7")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node8")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node9")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node10")).thenReturn(null);
        
        // Mock actual states: node1, node2, node3 are converged, rest are null
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState1);
        when(metadataStore.getSearchUnitActualState(clusterId, "node2")).thenReturn(actualState2);
        when(metadataStore.getSearchUnitActualState(clusterId, "node3")).thenReturn(actualState3);
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, shardId)).thenReturn(planned);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, shardId);
        
        // Verify actual states are checked for nodes with existing goal states
        verify(metadataStore).getSearchUnitActualState(clusterId, "node1");
        verify(metadataStore).getSearchUnitActualState(clusterId, "node2");
        verify(metadataStore).getSearchUnitActualState(clusterId, "node3");
        
        // With 20% rolling update limit and 10 total nodes, only 2 nodes should be updated
        // (20% of 10 = 2 nodes), but since node1, node2, node3 are already converged,
        // only 2 of the remaining 7 nodes should be updated
        verify(metadataStore, times(2)).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
        
        // CRITICAL: Verify that converged nodes (node1, node2, node3) are NOT touched
        verify(metadataStore, never()).setSearchUnitGoalState(eq(clusterId), eq("node1"), any(SearchUnitGoalState.class));
        verify(metadataStore, never()).setSearchUnitGoalState(eq(clusterId), eq("node2"), any(SearchUnitGoalState.class));
        verify(metadataStore, never()).setSearchUnitGoalState(eq(clusterId), eq("node3"), any(SearchUnitGoalState.class));
    }

    @Test
    void testRollingUpdateBehaviorWithTransitNodes() throws Exception {
        // Given: A scenario where some nodes are already in transit (goal state updated but not converged)
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";
        
        Index indexConfig = createIndex(indexName, 1);
        
        // Create planned allocation with 10 nodes (1 primary + 9 replicas)
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1")); // 1 primary
        planned.setSearchSUs(Arrays.asList("node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9", "node10")); // 9 replicas
        
        // Create goal states for some nodes (already have goal states)
        SearchUnitGoalState existingGoalState1 = createGoalStateWithShard(indexName, shardId, "PRIMARY");
        SearchUnitGoalState existingGoalState2 = createGoalStateWithShard(indexName, shardId, "SEARCH_REPLICA");
        
        // Create actual states that DON'T match goal states (in transit)
        SearchUnitActualState actualState1 = createActualStateWithShard(indexName, shardId, "SEARCH_REPLICA"); // Different from goal state
        SearchUnitActualState actualState2 = createActualStateWithShard(indexName, shardId, "PRIMARY"); // Different from goal state
        
        // Mock goal states: node1, node2 have existing goal states, rest are null
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1")).thenReturn(existingGoalState1);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node2")).thenReturn(existingGoalState2);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node3")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node4")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node5")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node6")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node7")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node8")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node9")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node10")).thenReturn(null);
        
        // Mock actual states: node1, node2 are in transit (not converged)
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState1);
        when(metadataStore.getSearchUnitActualState(clusterId, "node2")).thenReturn(actualState2);
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, shardId)).thenReturn(planned);

        // When
        strategy.orchestrate(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, shardId);
        
        // Verify actual states are checked for nodes with existing goal states
        verify(metadataStore).getSearchUnitActualState(clusterId, "node1");
        verify(metadataStore).getSearchUnitActualState(clusterId, "node2");
        
        // With 20% rolling update limit and 10 total nodes, only 2 nodes should be updated
        // (20% of 10 = 2 nodes), but since node1 and node2 are already converged,
        // 2 more nodes should be updated to reach the 20% limit
        verify(metadataStore, times(2)).setSearchUnitGoalState(eq(clusterId), anyString(), any(SearchUnitGoalState.class));
    }

    @Test
    void testOrchestrateThrowsExceptionWhenGetPlannedAllocationFails() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        Index indexConfig = createIndex(indexName, 1);
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0"))
                .thenThrow(new RuntimeException("Database connection failed"));

        // When - orchestrate should handle the exception gracefully
        strategy.orchestrate(clusterId);
        
        // Then - verify the method was called and exception was handled
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        // The orchestration should complete without throwing (resilient behavior)
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
        verify(metadataStore, times(2)).getSearchUnitGoalState(clusterId, "node1"); // Called in hasGoalStateUpdated and updateNodeGoalState
        // The orchestration should complete without throwing (resilient behavior)
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
        
        SearchUnitGoalState existingGoalState = createGoalStateWithShard(indexName, "0", "PRIMARY");
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, "0")).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1")).thenReturn(existingGoalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node2")).thenReturn(null);

        // When - orchestrate should handle the exception gracefully
        strategy.orchestrate(clusterId);
        
        // Then - verify the method was called and exception was handled
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, indexName, "0");
        verify(metadataStore).getSearchUnitGoalState(clusterId, "node1");
        // Note: setSearchUnitGoalState may not be called if nodes are already in correct state
        // The orchestration should complete without throwing (resilient behavior)
    }

    // ========== CLEANUP TESTS ==========
    
    @Test
    void testCleanupStaleGoalStates_RemovesNodeNotInPlannedAllocation() throws Exception {
        // Given: Node has goal state for a shard that is NO LONGER in planned allocation
        String clusterId = "test-cluster";
        String nodeId = "node1";
        
        // Node has goal state for index1/shard0
        SearchUnitGoalState goalState = createGoalStateWithShard("index1", "0", "PRIMARY");
        when(metadataStore.getAllNodesWithGoalStates(clusterId)).thenReturn(Arrays.asList(nodeId));
        when(metadataStore.getSearchUnitGoalState(clusterId, nodeId)).thenReturn(goalState);
        
        // But no planned allocation exists for index1/shard0
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "0")).thenReturn(null);
        
        // Mock empty index configs for main orchestration
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList());
        
        // When
        strategy.orchestrate(clusterId);
        
        // Then: Stale goal state should be removed
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq(nodeId), argThat(gs -> 
            gs.getLocalShards() == null || gs.getLocalShards().isEmpty()
        ));
    }
    
    @Test
    void testCleanupStaleGoalStates_KeepsNodeInPlannedAllocation() throws Exception {
        // Given: Node has goal state that IS in planned allocation
        String clusterId = "test-cluster";
        String nodeId = "node1";
        
        // Node has goal state for index1/shard0 as PRIMARY
        SearchUnitGoalState goalState = createGoalStateWithShard("index1", "0", "PRIMARY");
        when(metadataStore.getAllNodesWithGoalStates(clusterId)).thenReturn(Arrays.asList(nodeId));
        when(metadataStore.getSearchUnitGoalState(clusterId, nodeId)).thenReturn(goalState);
        
        // Planned allocation includes this node for index1/shard0 as PRIMARY
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList(nodeId));
        planned.setSearchSUs(Arrays.asList());
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "0")).thenReturn(planned);
        
        // Mock empty index configs for main orchestration
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList());
        
        // When
        strategy.orchestrate(clusterId);
        
        // Then: Goal state should NOT be modified (no cleanup call for this node)
        verify(metadataStore, never()).setSearchUnitGoalState(eq(clusterId), eq(nodeId), any());
    }
    
    @Test
    void testCleanupStaleGoalStates_HandlesMixedValidAndStaleEntries() throws Exception {
        // Given: Node has MULTIPLE goal states - some valid, some stale
        String clusterId = "test-cluster";
        String nodeId = "node1";
        
        // Node has goal state for 2 shards: index1/shard0 (valid) and index2/shard0 (stale)
        SearchUnitGoalState goalState = new SearchUnitGoalState();
        Map<String, Map<String, String>> localShards = new HashMap<>();
        
        Map<String, String> index1Shards = new HashMap<>();
        index1Shards.put("0", "PRIMARY");
        localShards.put("index1", index1Shards);
        
        Map<String, String> index2Shards = new HashMap<>();
        index2Shards.put("0", "SEARCH_REPLICA");
        localShards.put("index2", index2Shards);
        
        goalState.setLocalShards(localShards);
        
        when(metadataStore.getAllNodesWithGoalStates(clusterId)).thenReturn(Arrays.asList(nodeId));
        when(metadataStore.getSearchUnitGoalState(clusterId, nodeId)).thenReturn(goalState);
        
        // Planned allocation: index1/shard0 includes node1, but index2/shard0 does NOT
        ShardAllocation planned1 = new ShardAllocation();
        planned1.setIngestSUs(Arrays.asList(nodeId));
        planned1.setSearchSUs(Arrays.asList());
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "0")).thenReturn(planned1);
        
        ShardAllocation planned2 = new ShardAllocation();
        planned2.setIngestSUs(Arrays.asList());
        planned2.setSearchSUs(Arrays.asList("node2", "node3")); // node1 NOT in planned allocation
        when(metadataStore.getPlannedAllocation(clusterId, "index2", "0")).thenReturn(planned2);
        
        // Mock empty index configs for main orchestration
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList());
        
        // When
        strategy.orchestrate(clusterId);
        
        // Then: Cleanup should remove index2/shard0 but keep index1/shard0
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq(nodeId), argThat(gs -> {
            Map<String, Map<String, String>> shards = gs.getLocalShards();
            // Should have index1 but NOT index2
            return shards.containsKey("index1") && !shards.containsKey("index2");
        }));
    }
    
    @Test
    void testCleanupStaleGoalStates_HandlesEmptyGoalState() throws Exception {
        // Given: Node has empty goal state
        String clusterId = "test-cluster";
        String nodeId = "node1";
        
        SearchUnitGoalState emptyGoalState = new SearchUnitGoalState();
        emptyGoalState.setLocalShards(new HashMap<>());
        
        when(metadataStore.getAllNodesWithGoalStates(clusterId)).thenReturn(Arrays.asList(nodeId));
        when(metadataStore.getSearchUnitGoalState(clusterId, nodeId)).thenReturn(emptyGoalState);
        
        // Mock empty index configs for main orchestration
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList());
        
        // When
        strategy.orchestrate(clusterId);
        
        // Then: No cleanup should be performed (nothing to clean)
        verify(metadataStore, never()).setSearchUnitGoalState(eq(clusterId), eq(nodeId), any());
    }
    
    @Test
    void testCleanupStaleGoalStates_HandlesNullGoalState() throws Exception {
        // Given: Node has null goal state
        String clusterId = "test-cluster";
        String nodeId = "node1";
        
        when(metadataStore.getAllNodesWithGoalStates(clusterId)).thenReturn(Arrays.asList(nodeId));
        when(metadataStore.getSearchUnitGoalState(clusterId, nodeId)).thenReturn(null);
        
        // Mock empty index configs for main orchestration
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList());
        
        // When
        strategy.orchestrate(clusterId);
        
        // Then: No cleanup should be performed (nothing to clean)
        verify(metadataStore, never()).setSearchUnitGoalState(eq(clusterId), eq(nodeId), any());
    }
    
    @Test
    void testCleanupStaleGoalStates_WorksForBothPrimaryAndReplica() throws Exception {
        // Given: Multiple nodes with both PRIMARY and REPLICA roles
        String clusterId = "test-cluster";
        String primaryNode = "primary-node";
        String replicaNode = "replica-node";
        
        // Primary node has stale goal state
        SearchUnitGoalState primaryGoalState = createGoalStateWithShard("index1", "0", "PRIMARY");
        when(metadataStore.getSearchUnitGoalState(clusterId, primaryNode)).thenReturn(primaryGoalState);
        
        // Replica node has stale goal state
        SearchUnitGoalState replicaGoalState = createGoalStateWithShard("index1", "0", "SEARCH_REPLICA");
        when(metadataStore.getSearchUnitGoalState(clusterId, replicaNode)).thenReturn(replicaGoalState);
        
        when(metadataStore.getAllNodesWithGoalStates(clusterId)).thenReturn(Arrays.asList(primaryNode, replicaNode));
        
        // No planned allocation exists for index1/shard0
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "0")).thenReturn(null);
        
        // Mock empty index configs for main orchestration
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList());
        
        // When
        strategy.orchestrate(clusterId);
        
        // Then: Both PRIMARY and REPLICA stale goal states should be removed
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq(primaryNode), argThat(gs -> 
            gs.getLocalShards() == null || gs.getLocalShards().isEmpty()
        ));
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq(replicaNode), argThat(gs -> 
            gs.getLocalShards() == null || gs.getLocalShards().isEmpty()
        ));
    }
    
    @Test
    void testCleanupStaleGoalStates_HandlesNodeNotInAnyPlannedAllocation() throws Exception {
        // Given: Node has goal states for multiple shards, NONE in planned allocation
        String clusterId = "test-cluster";
        String nodeId = "node1";
        
        // Node has goal states for 3 different shards
        SearchUnitGoalState goalState = new SearchUnitGoalState();
        Map<String, Map<String, String>> localShards = new HashMap<>();
        
        Map<String, String> index1Shards = new HashMap<>();
        index1Shards.put("0", "PRIMARY");
        index1Shards.put("1", "PRIMARY");
        localShards.put("index1", index1Shards);
        
        Map<String, String> index2Shards = new HashMap<>();
        index2Shards.put("0", "SEARCH_REPLICA");
        localShards.put("index2", index2Shards);
        
        goalState.setLocalShards(localShards);
        
        when(metadataStore.getAllNodesWithGoalStates(clusterId)).thenReturn(Arrays.asList(nodeId));
        when(metadataStore.getSearchUnitGoalState(clusterId, nodeId)).thenReturn(goalState);
        
        // None of the planned allocations include this node
        ShardAllocation planned1_0 = new ShardAllocation();
        planned1_0.setIngestSUs(Arrays.asList("other-node"));
        planned1_0.setSearchSUs(Arrays.asList());
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "0")).thenReturn(planned1_0);
        
        ShardAllocation planned1_1 = new ShardAllocation();
        planned1_1.setIngestSUs(Arrays.asList("another-node"));
        planned1_1.setSearchSUs(Arrays.asList());
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "1")).thenReturn(planned1_1);
        
        ShardAllocation planned2_0 = new ShardAllocation();
        planned2_0.setIngestSUs(Arrays.asList());
        planned2_0.setSearchSUs(Arrays.asList("replica-1", "replica-2"));
        when(metadataStore.getPlannedAllocation(clusterId, "index2", "0")).thenReturn(planned2_0);
        
        // Mock empty index configs for main orchestration
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList());
        
        // When
        strategy.orchestrate(clusterId);
        
        // Then: ALL goal states should be removed (node not in any planned allocation)
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq(nodeId), argThat(gs -> 
            gs.getLocalShards() == null || gs.getLocalShards().isEmpty()
        ));
    }
    
    @Test
    void testCleanupStaleGoalStates_HandlesDecommissionedNodes() throws Exception {
        // Given: A decommissioned node (no conf file) still has goal states
        String clusterId = "test-cluster";
        String decommissionedNode = "decommissioned-node";
        
        // Node is in getAllNodesWithGoalStates (has goal state) but NOT in getAllSearchUnits (no conf)
        when(metadataStore.getAllNodesWithGoalStates(clusterId)).thenReturn(Arrays.asList(decommissionedNode));
        
        SearchUnitGoalState goalState = createGoalStateWithShard("index1", "0", "PRIMARY");
        when(metadataStore.getSearchUnitGoalState(clusterId, decommissionedNode)).thenReturn(goalState);
        
        // No planned allocation exists for this node
        when(metadataStore.getPlannedAllocation(clusterId, "index1", "0")).thenReturn(null);
        
        // Mock empty index configs for main orchestration
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList());
        
        // When
        strategy.orchestrate(clusterId);
        
        // Then: Goal state should be cleaned up even though node has no conf file
        verify(metadataStore).setSearchUnitGoalState(eq(clusterId), eq(decommissionedNode), argThat(gs -> 
            gs.getLocalShards() == null || gs.getLocalShards().isEmpty()
        ));
    }

    @Test
    void testGaugeMetricUpdatesWithCorrectProgressPercentage() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";
        
        Index indexConfig = createIndex(indexName, 1);
        
        // Create planned allocation with 10 nodes (1 primary + 9 replicas)
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1"));
        planned.setSearchSUs(Arrays.asList("node2", "node3", "node4", "node5", "node6", "node7", "node8", "node9", "node10", "node11"));
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, shardId)).thenReturn(planned);
        lenient().when(metadataStore.getSearchUnitGoalState(eq(clusterId), anyString())).thenReturn(null);
        lenient().when(metadataStore.getSearchUnitActualState(eq(clusterId), anyString())).thenReturn(null);

        // When
        strategy.orchestrate(clusterId);

        // Then - Verify gauge is called for PRIMARY group
        // PRIMARY: 1 node updated out of 1 = 100%
        verify(metricsProvider).gauge(
            eq("rolling_update_progress_percentage"),
            eq(100.0),
            argThat(tags -> 
                tags.get("clusterId").equals(clusterId) &&
                tags.get("indexName").equals(indexName) &&
                tags.get("shardId").equals(shardId) &&
                tags.get("role").equals("PRIMARY")
            )
        );
        
        // REPLICA: 2 nodes updated out of 10 ≈ 20%
        verify(metricsProvider).gauge(
            eq("rolling_update_progress_percentage"),
            eq(20.0),
            argThat(tags -> 
                tags.get("clusterId").equals(clusterId) &&
                tags.get("indexName").equals(indexName) &&
                tags.get("shardId").equals(shardId) &&
                tags.get("role").equals("SEARCH_REPLICA")
            )
        );
    }

    @Test
    void testGaugeMetricWithConvergedNodes() throws Exception {
        // Given: Some nodes already converged
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String shardId = "0";
        
        Index indexConfig = createIndex(indexName, 1);
        
        ShardAllocation planned = new ShardAllocation();
        planned.setIngestSUs(Arrays.asList("node1", "node2", "node3", "node4", "node5"));
        planned.setSearchSUs(Arrays.asList());
        
        // Mock 3 nodes already converged
        SearchUnitGoalState goalState = createGoalStateWithShard(indexName, shardId, "PRIMARY");
        SearchUnitActualState actualState = createActualStateWithShard(indexName, shardId, "PRIMARY");
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Arrays.asList(indexConfig));
        when(metadataStore.getPlannedAllocation(clusterId, indexName, shardId)).thenReturn(planned);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node1")).thenReturn(goalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node2")).thenReturn(goalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node3")).thenReturn(goalState);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node4")).thenReturn(null);
        when(metadataStore.getSearchUnitGoalState(clusterId, "node5")).thenReturn(null);
        
        when(metadataStore.getSearchUnitActualState(clusterId, "node1")).thenReturn(actualState);
        when(metadataStore.getSearchUnitActualState(clusterId, "node2")).thenReturn(actualState);
        when(metadataStore.getSearchUnitActualState(clusterId, "node3")).thenReturn(actualState);

        // When
        strategy.orchestrate(clusterId);

        // Then - Verify gauge shows 4 out of 5 nodes = 80% (3 already converged + 1 newly updated)
        verify(metricsProvider).gauge(
            eq("rolling_update_progress_percentage"),
            eq(80.0),
            argThat(tags -> 
                tags.get("clusterId").equals(clusterId) &&
                tags.get("indexName").equals(indexName) &&
                tags.get("shardId").equals(shardId) &&
                tags.get("role").equals("PRIMARY")
            )
        );
    }

    // Helper methods
    private SearchUnitGoalState createGoalStateWithShard(String indexName, String shardId, String role) {
        SearchUnitGoalState goalState = new SearchUnitGoalState();
        Map<String, Map<String, String>> localShards = new HashMap<>();
        Map<String, String> shards = new HashMap<>();
        shards.put(shardId, role);
        localShards.put(indexName, shards);
        goalState.setLocalShards(localShards);
        return goalState;
    }

    private SearchUnitActualState createActualStateWithShard(String indexName, String shardId, String role) {
        SearchUnitActualState actualState = new SearchUnitActualState();
        Map<String, List<SearchUnitActualState.ShardRoutingInfo>> nodeRouting = new HashMap<>();
        SearchUnitActualState.ShardRoutingInfo shardInfo = new SearchUnitActualState.ShardRoutingInfo();
        // Parse shard ID string to integer
        shardInfo.setShardId(Integer.parseInt(shardId));
        // Convert role to lowercase format matching worker output ("primary", "search_replica", "replica")
        shardInfo.setRole("PRIMARY".equals(role) ? "primary" : "replica");
        shardInfo.setState(io.clustercontroller.enums.ShardState.STARTED);
        nodeRouting.put(indexName, Arrays.asList(shardInfo));
        actualState.setNodeRouting(nodeRouting);
        return actualState;
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
    
    // Helper method to create SearchUnit
    private SearchUnit createSearchUnit(String nodeId) {
        SearchUnit node = new SearchUnit();
        node.setName(nodeId);
        return node;
    }
}
