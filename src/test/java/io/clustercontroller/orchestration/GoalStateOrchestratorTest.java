package io.clustercontroller.orchestration;

import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GoalStateOrchestratorTest {

    @Mock
    private MetadataStore metadataStore;

    private GoalStateOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        orchestrator = new GoalStateOrchestrator(metadataStore);
    }

    @Test
    void testOrchestrateGoalStatesSuccess() throws Exception {
        // Given
        String clusterId = "test-cluster";
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());

        // When
        orchestrator.orchestrateGoalStates(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
    }

    @Test
    void testOrchestrateGoalStatesWithMetadataStoreException() throws Exception {
        // Given
        String clusterId = "test-cluster";
        Exception metadataException = new RuntimeException("Metadata store error");
        
        doThrow(metadataException).when(metadataStore).getAllIndexConfigs(clusterId);

        // When
        orchestrator.orchestrateGoalStates(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        // Exception should be handled gracefully (logged but not re-thrown)
    }

    @Test
    void testOrchestrateGoalStatesWithNullClusterId() throws Exception {
        // Given
        String clusterId = null;
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());

        // When
        orchestrator.orchestrateGoalStates(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
    }

    @Test
    void testOrchestrateGoalStatesWithEmptyClusterId() throws Exception {
        // Given
        String clusterId = "";
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(Collections.emptyList());

        // When
        orchestrator.orchestrateGoalStates(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
    }

    @Test
    void testOrchestrateGoalStatesMultipleCalls() throws Exception {
        // Given
        String clusterId1 = "cluster1";
        String clusterId2 = "cluster2";
        when(metadataStore.getAllIndexConfigs(clusterId1)).thenReturn(Collections.emptyList());
        when(metadataStore.getAllIndexConfigs(clusterId2)).thenReturn(Collections.emptyList());

        // When
        orchestrator.orchestrateGoalStates(clusterId1);
        orchestrator.orchestrateGoalStates(clusterId2);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId1);
        verify(metadataStore).getAllIndexConfigs(clusterId2);
        verify(metadataStore, times(2)).getAllIndexConfigs(anyString());
    }

    @Test
    void testOrchestrateGoalStatesWithIndexConfigs() throws Exception {
        // Given
        String clusterId = "test-cluster";
        Index index = new Index();
        index.setIndexName("test-index");
        
        // Initialize settings
        IndexSettings settings = new IndexSettings();
        settings.setNumberOfShards(2);
        index.setSettings(settings);
        
        List<Index> indexConfigs = List.of(index);
        
        when(metadataStore.getAllIndexConfigs(clusterId)).thenReturn(indexConfigs);
        when(metadataStore.getPlannedAllocation(clusterId, "test-index", "00")).thenReturn(null);
        when(metadataStore.getPlannedAllocation(clusterId, "test-index", "01")).thenReturn(null);

        // When
        orchestrator.orchestrateGoalStates(clusterId);

        // Then
        verify(metadataStore).getAllIndexConfigs(clusterId);
        verify(metadataStore).getPlannedAllocation(clusterId, "test-index", "00");
        verify(metadataStore).getPlannedAllocation(clusterId, "test-index", "01");
    }
}
