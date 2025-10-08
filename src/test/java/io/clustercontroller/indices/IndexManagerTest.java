package io.clustercontroller.indices;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.clustercontroller.models.Index;
import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IndexManagerTest {

    @Mock
    private MetadataStore metadataStore;

    private IndexManager indexManager;

    @BeforeEach
    void setUp() {
        indexManager = new IndexManager(metadataStore);
    }

    @Test
    void testCreateIndex_Success() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        List<SearchUnit> availableSearchUnits = createMockSearchUnits();
        String createIndexRequestJson = """
            {
                "mappings": {"properties": {"field1": {"type": "text"}}},
                "settings": {"number_of_shards": 1}
            }
            """;

        // Mock dependencies
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.empty());
        when(metadataStore.createIndexConfig(eq(clusterId), eq(indexName), any(String.class))).thenReturn("doc-id-123");

        // When
        indexManager.createIndex(clusterId, indexName, createIndexRequestJson);

        // Then
        ArgumentCaptor<String> indexConfigCaptor = ArgumentCaptor.forClass(String.class);
        verify(metadataStore).createIndexConfig(eq(clusterId), eq(indexName), indexConfigCaptor.capture());

        String capturedIndexConfig = indexConfigCaptor.getValue();
        assertThat(capturedIndexConfig).isNotNull();
        assertThat(capturedIndexConfig).contains(indexName);

        // getAllSearchUnits is no longer called since the check was removed
        
        // Verify that setIndexMappings and setIndexSettings are called with correct values
        verify(metadataStore).setIndexMappings(clusterId, indexName, "{\"properties\":{\"field1\":{\"type\":\"text\"}}}");
        verify(metadataStore).setIndexSettings(clusterId, indexName, "{\"number_of_shards\":1}");
    }


    @Test
    void testCreateIndex_NoAvailableSearchUnits() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String createIndexRequestJson = """
            {
            }
            """;

        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.empty());
        when(metadataStore.createIndexConfig(eq(clusterId), eq(indexName), any(String.class))).thenReturn("doc-id-123");

        // When
        indexManager.createIndex(clusterId, indexName, createIndexRequestJson);

        // Then - should succeed even without search units (search units check was removed)
        verify(metadataStore).createIndexConfig(eq(clusterId), eq(indexName), any(String.class));
    }

    @Test
    void testCreateIndex_InvalidJson() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String invalidJson = "invalid json";

        // When & Then
        assertThatThrownBy(() -> indexManager.createIndex(clusterId, indexName, invalidJson))
                .isInstanceOf(Exception.class);
    }

    @Test
    void testCreateIndex_IndexAlreadyExists() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "existing-index";
        String createIndexRequestJson = """
            {
            }
            """;

        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.of("existing-config"));

        // When
        indexManager.createIndex(clusterId, indexName, createIndexRequestJson);

        // Then
        verify(metadataStore, never()).createIndexConfig(any(), any(), any());
        verify(metadataStore, never()).getAllSearchUnits(any());
    }

    @Test
    void testDeleteIndex_Success() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.of("existing-config"));

        // When
        indexManager.deleteIndex(clusterId, indexName);

        // Then
        verify(metadataStore).getIndexConfig(clusterId, indexName);
        verify(metadataStore).deletePrefix(clusterId, "/" + clusterId + "/indices/" + indexName);
    }

    @Test
    void testDeleteIndex_IndexNotFound() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "non-existent-index";
        
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.empty());

        // When
        indexManager.deleteIndex(clusterId, indexName);

        // Then
        verify(metadataStore).getIndexConfig(clusterId, indexName);
        verify(metadataStore, never()).deletePrefix(any(), any());
    }

    @Test
    void testDeleteIndex_EmptyIndexName() {
        // Given
        String clusterId = "test-cluster";
        String indexName = "";

        // When & Then
        assertThatThrownBy(() -> indexManager.deleteIndex(clusterId, indexName))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Index name cannot be null or empty");
    }

    @Test
    void testDeleteIndex_NullClusterId() {
        // Given
        String clusterId = null;
        String indexName = "test-index";

        // When & Then
        assertThatThrownBy(() -> indexManager.deleteIndex(clusterId, indexName))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Cluster ID cannot be null or empty");
    }

    @Test
    void testDeleteIndex_DeletePrefixThrowsException() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.of("existing-config"));
        doThrow(new Exception("Delete prefix failed")).when(metadataStore).deletePrefix(any(), any());

        // When & Then
        assertThatThrownBy(() -> indexManager.deleteIndex(clusterId, indexName))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("Failed to delete index 'test-index' from cluster 'test-cluster'");
    }

    @Test
    void testCreateIndex_DefaultValues() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String createIndexRequestJson = "{}"; // Empty JSON should use defaults

        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.empty());
        when(metadataStore.createIndexConfig(eq(clusterId), eq(indexName), any(String.class))).thenReturn("doc-id-789");

        // When
        indexManager.createIndex(clusterId, indexName, createIndexRequestJson);

        // Then
        ArgumentCaptor<String> indexConfigCaptor = ArgumentCaptor.forClass(String.class);
        verify(metadataStore).createIndexConfig(eq(clusterId), eq(indexName), indexConfigCaptor.capture());

        String capturedIndexConfig = indexConfigCaptor.getValue();
        assertThat(capturedIndexConfig).isNotNull();
        assertThat(capturedIndexConfig).contains(indexName);
    }

    @Test
    void testCreateIndex_WithoutMappingsAndSettings() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String createIndexRequestJson = """
            {
            }
            """;

        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.empty());
        when(metadataStore.createIndexConfig(eq(clusterId), eq(indexName), any(String.class))).thenReturn("doc-id-999");

        // When
        indexManager.createIndex(clusterId, indexName, createIndexRequestJson);

        // Then
        verify(metadataStore).createIndexConfig(eq(clusterId), eq(indexName), any(String.class));
        verify(metadataStore, never()).setIndexMappings(any(), any(), any());
        verify(metadataStore, never()).setIndexSettings(any(), any(), any());
    }

    @Test
    void testCreateIndex_WithCustomNumberOfShards() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String createIndexRequestJson = """
            {
                "settings": {"number_of_shards": 3, "number_of_replicas": 1}
            }
            """;
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.empty());
        when(metadataStore.createIndexConfig(eq(clusterId), eq(indexName), any(String.class))).thenReturn("doc-id-456");

        // When
        indexManager.createIndex(clusterId, indexName, createIndexRequestJson);

        // Then
        verify(metadataStore).createIndexConfig(eq(clusterId), eq(indexName), any(String.class));
        verify(metadataStore).setIndexSettings(clusterId, indexName, "{\"number_of_shards\":3,\"number_of_replicas\":1}");
    }

    @Test
    void testCreateIndex_WithInvalidSettingsJson() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String createIndexRequestJson = """
            {
                "settings": {"invalid_field": "invalid_value"}
            }
            """;
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.empty());
        when(metadataStore.createIndexConfig(eq(clusterId), eq(indexName), any(String.class))).thenReturn("doc-id-789");

        // When
        indexManager.createIndex(clusterId, indexName, createIndexRequestJson);

        // Then - should still work with default shard count (1) even with invalid settings
        verify(metadataStore).createIndexConfig(eq(clusterId), eq(indexName), any(String.class));
        verify(metadataStore).setIndexSettings(clusterId, indexName, "{\"invalid_field\":\"invalid_value\"}");
    }

    // ========== getSettings Tests ==========

    @Test
    void testGetSettings_Success() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        
        IndexSettings expectedSettings = new IndexSettings();
        expectedSettings.setNumberOfShards(3);
        expectedSettings.setShardReplicaCount(List.of(2, 2, 2));
        expectedSettings.setPausePullIngestion(true);

        // Mock dependencies
        when(metadataStore.getIndexSettings(clusterId, indexName)).thenReturn(expectedSettings);

        // When
        String result = indexManager.getSettings(clusterId, indexName);

        // Then
        assertThat(result).contains("\"number_of_shards\":3");
        assertThat(result).contains("\"pause_pull_ingestion\":true");
        verify(metadataStore).getIndexSettings(clusterId, indexName);
    }

    @Test
    void testGetSettings_EmptyClusterId() {
        // Given
        String clusterId = "";
        String indexName = "test-index";

        // When & Then
        assertThatThrownBy(() -> indexManager.getSettings(clusterId, indexName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cluster ID cannot be null or empty");
    }

    @Test
    void testGetSettings_EmptyIndexName() {
        // Given
        String clusterId = "test-cluster";
        String indexName = "";

        // When & Then
        assertThatThrownBy(() -> indexManager.getSettings(clusterId, indexName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Index name cannot be null or empty");
    }

    @Test
    void testGetSettings_IndexDoesNotExist() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "non-existent-index";

        // Mock dependencies - settings not found
        when(metadataStore.getIndexSettings(clusterId, indexName)).thenReturn(null);

        // When & Then
        assertThatThrownBy(() -> indexManager.getSettings(clusterId, indexName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Index 'non-existent-index' does not exist in cluster 'test-cluster'");
    }

    // ========== updateSettings Tests ==========

    @Test
    void testUpdateSettings_Success() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String settingsJson = """
            {
                "pause_pull_ingestion": true
            }
            """;
        
        IndexSettings existingSettings = new IndexSettings();
        existingSettings.setNumberOfShards(3);
        existingSettings.setShardReplicaCount(List.of(2, 2, 2));

        // Mock dependencies
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.of("existing-config"));
        when(metadataStore.getIndexSettings(clusterId, indexName)).thenReturn(existingSettings);
        doNothing().when(metadataStore).setIndexSettings(eq(clusterId), eq(indexName), any(String.class));

        // When
        indexManager.updateSettings(clusterId, indexName, settingsJson);

        // Then
        verify(metadataStore).getIndexConfig(clusterId, indexName);
        verify(metadataStore).getIndexSettings(clusterId, indexName);
        
        ArgumentCaptor<String> settingsCaptor = ArgumentCaptor.forClass(String.class);
        verify(metadataStore).setIndexSettings(eq(clusterId), eq(indexName), settingsCaptor.capture());
        
        String mergedSettings = settingsCaptor.getValue();
        assertThat(mergedSettings).contains("\"pause_pull_ingestion\":true");
        assertThat(mergedSettings).contains("\"number_of_shards\":3");
    }

    @Test
    void testUpdateSettings_NoExistingSettings() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "new-index";
        String settingsJson = """
            {
                "number_of_shards": 5
            }
            """;

        // Mock dependencies
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.of("existing-config"));
        when(metadataStore.getIndexSettings(clusterId, indexName)).thenReturn(null);
        doNothing().when(metadataStore).setIndexSettings(eq(clusterId), eq(indexName), any(String.class));

        // When
        indexManager.updateSettings(clusterId, indexName, settingsJson);

        // Then
        ArgumentCaptor<String> settingsCaptor = ArgumentCaptor.forClass(String.class);
        verify(metadataStore).setIndexSettings(eq(clusterId), eq(indexName), settingsCaptor.capture());
        
        String mergedSettings = settingsCaptor.getValue();
        assertThat(mergedSettings).contains("\"number_of_shards\":5");
    }

    @Test
    void testUpdateSettings_EmptyClusterId() {
        // Given
        String clusterId = "";
        String indexName = "test-index";
        String settingsJson = "{\"number_of_shards\": 1}";

        // When & Then
        assertThatThrownBy(() -> indexManager.updateSettings(clusterId, indexName, settingsJson))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cluster ID cannot be null or empty");
    }

    @Test
    void testUpdateSettings_EmptyIndexName() {
        // Given
        String clusterId = "test-cluster";
        String indexName = "";
        String settingsJson = "{\"number_of_shards\": 1}";

        // When & Then
        assertThatThrownBy(() -> indexManager.updateSettings(clusterId, indexName, settingsJson))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Index name cannot be null or empty");
    }

    @Test
    void testUpdateSettings_EmptySettingsJson() {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String settingsJson = "";

        // When & Then
        assertThatThrownBy(() -> indexManager.updateSettings(clusterId, indexName, settingsJson))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Settings JSON cannot be null or empty");
    }

    @Test
    void testUpdateSettings_IndexDoesNotExist() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "non-existent-index";
        String settingsJson = "{\"number_of_shards\": 1}";

        // Mock dependencies
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.empty());

        // When & Then
        assertThatThrownBy(() -> indexManager.updateSettings(clusterId, indexName, settingsJson))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Index 'non-existent-index' does not exist in cluster 'test-cluster'");
    }

    @Test
    void testUpdateSettings_InvalidJsonFormat() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String settingsJson = "invalid json format";

        // Mock dependencies
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.of("existing-config"));

        // When & Then
        assertThatThrownBy(() -> indexManager.updateSettings(clusterId, indexName, settingsJson))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid JSON format for settings");
    }

    @Test
    void testUpdateSettings_EmptyJsonObject() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String settingsJson = "{}";
        
        IndexSettings existingSettings = new IndexSettings();
        existingSettings.setNumberOfShards(3);

        // Mock dependencies
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.of("existing-config"));
        when(metadataStore.getIndexSettings(clusterId, indexName)).thenReturn(existingSettings);
        doNothing().when(metadataStore).setIndexSettings(eq(clusterId), eq(indexName), any(String.class));

        // When - empty JSON object {} will have all fields as null, so nothing gets merged
        indexManager.updateSettings(clusterId, indexName, settingsJson);

        // Then - should succeed and preserve existing settings since no fields were provided
        ArgumentCaptor<String> settingsCaptor = ArgumentCaptor.forClass(String.class);
        verify(metadataStore).setIndexSettings(eq(clusterId), eq(indexName), settingsCaptor.capture());
        
        String mergedSettings = settingsCaptor.getValue();
        assertThat(mergedSettings).contains("\"number_of_shards\":3"); // Preserved from existing
    }

    @Test
    void testUpdateSettings_MalformedJson() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String settingsJson = "{\"number_of_shards\": 1,}"; // Trailing comma

        // Mock dependencies
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.of("existing-config"));

        // When & Then
        assertThatThrownBy(() -> indexManager.updateSettings(clusterId, indexName, settingsJson))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid JSON format for settings");
    }

    @Test
    void testUpdateSettings_MetadataStoreException() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String settingsJson = "{\"number_of_shards\": 1}";

        // Mock dependencies
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.of("existing-config"));
        when(metadataStore.getIndexSettings(clusterId, indexName)).thenReturn(null);
        doThrow(new RuntimeException("Database connection failed"))
                .when(metadataStore).setIndexSettings(eq(clusterId), eq(indexName), any(String.class));

        // When & Then
        assertThatThrownBy(() -> indexManager.updateSettings(clusterId, indexName, settingsJson))
                .isInstanceOf(Exception.class)
                .hasMessage("Failed to update settings for index 'test-index': Database connection failed");
    }

    @Test
    void testUpdateSettings_MergeWithExistingSettings() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String newSettings = """
            {
                "pause_pull_ingestion": true,
                "number_of_shards": 5
            }
            """;
        
        IndexSettings existingSettings = new IndexSettings();
        existingSettings.setNumberOfShards(3);
        existingSettings.setShardReplicaCount(List.of(1, 1, 1));
        existingSettings.setPausePullIngestion(false);

        // Mock dependencies
        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.of("existing-config"));
        when(metadataStore.getIndexSettings(clusterId, indexName)).thenReturn(existingSettings);
        doNothing().when(metadataStore).setIndexSettings(eq(clusterId), eq(indexName), any(String.class));

        // When
        indexManager.updateSettings(clusterId, indexName, newSettings);

        // Then
        ArgumentCaptor<String> settingsCaptor = ArgumentCaptor.forClass(String.class);
        verify(metadataStore).setIndexSettings(eq(clusterId), eq(indexName), settingsCaptor.capture());
        
        String mergedSettings = settingsCaptor.getValue();
        // Should have updated values from newSettings
        assertThat(mergedSettings).contains("\"pause_pull_ingestion\":true");
        assertThat(mergedSettings).contains("\"number_of_shards\":5");
        // Should preserve shard_replica_count from existing settings
        assertThat(mergedSettings).contains("\"shard_replica_count\"");
    }

    private List<SearchUnit> createMockSearchUnits() {
        List<SearchUnit> searchUnits = new ArrayList<>();
        SearchUnit unit = new SearchUnit();
        unit.setName("test-unit-1");
        searchUnits.add(unit);
        return searchUnits;
    }
}