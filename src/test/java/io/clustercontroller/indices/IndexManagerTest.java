package io.clustercontroller.indices;

import io.clustercontroller.models.IndexSettings;
import io.clustercontroller.models.TypeMapping;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.templates.TemplateManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Mock
    private TemplateManager templateManager;

    private IndexManager indexManager;

    @BeforeEach
    void setUp() throws Exception {
        // Mock template manager to return empty list by default (no matching templates)
        // Use lenient() to avoid UnnecessaryStubbingException in tests that don't create indices
        lenient().when(templateManager.findMatchingTemplates(any(), any())).thenReturn(new ArrayList<>());
        indexManager = new IndexManager(metadataStore, templateManager);
    }

    @Test
    void testCreateIndex_Success() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
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

    @Test
    void testCreateIndex_FiltersControllerSpecificSettings() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";
        String createIndexRequestJson = """
            {
                "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 2,
                    "num_groups_per_shard": [2, 2, 1],
                    "shard_replica_count": [2, 2, 2],
                    "num_ingest_groups_per_shard": [1, 1, 1],
                    "refresh_interval": "30s",
                    "max_result_window": 10000,
                    "remote_store.enabled": true
                }
            }
            """;

        when(metadataStore.getIndexConfig(clusterId, indexName)).thenReturn(Optional.empty());
        when(metadataStore.createIndexConfig(eq(clusterId), eq(indexName), any())).thenReturn("doc-id");

        // When
        indexManager.createIndex(clusterId, indexName, createIndexRequestJson);

        // Then - verify controller-specific settings are stored in /conf
        ArgumentCaptor<String> confCaptor = ArgumentCaptor.forClass(String.class);
        verify(metadataStore).createIndexConfig(eq(clusterId), eq(indexName), confCaptor.capture());
        
        String confJson = confCaptor.getValue();
        assertThat(confJson).contains("\"num_groups_per_shard\"");
        assertThat(confJson).contains("\"shard_replica_count\"");

        // Then - verify controller-specific settings are filtered from /settings
        ArgumentCaptor<String> settingsCaptor = ArgumentCaptor.forClass(String.class);
        verify(metadataStore).setIndexSettings(eq(clusterId), eq(indexName), settingsCaptor.capture());
        
        String settingsJson = settingsCaptor.getValue();
        // Should NOT contain controller-specific settings
        assertThat(settingsJson).doesNotContain("num_groups_per_shard");
        assertThat(settingsJson).doesNotContain("shard_replica_count");
        assertThat(settingsJson).doesNotContain("num_ingest_groups_per_shard");
        
        // Should contain OpenSearch-native settings
        assertThat(settingsJson).contains("\"number_of_shards\":3");
        assertThat(settingsJson).contains("\"number_of_replicas\":2");
        assertThat(settingsJson).contains("\"refresh_interval\":\"30s\"");
        assertThat(settingsJson).contains("\"max_result_window\":10000");
        assertThat(settingsJson).contains("\"remote_store.enabled\":true");
    }

    // ========== getIndex Tests ==========

    @Test
    void testGetIndex_Success_WithSettingsAndMappings() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";

        IndexSettings mockSettings = new IndexSettings();
        mockSettings.setNumberOfShards(3);
        mockSettings.setShardReplicaCount(List.of(2, 2, 2));
        mockSettings.setPausePullIngestion(true);

        TypeMapping mockMappings = new TypeMapping();
        Map<String, Object> properties = new HashMap<>();
        properties.put("field1", Map.of("type", "text"));
        mockMappings.setProperties(properties);

        // Mock dependencies
        when(metadataStore.getIndexSettings(clusterId, indexName)).thenReturn(mockSettings);
        when(metadataStore.getIndexMappings(clusterId, indexName)).thenReturn(mockMappings);

        // When
        String result = indexManager.getIndex(clusterId, indexName);

        // Then
        assertThat(result).isNotNull();
        assertThat(result).contains("\"" + indexName + "\"");
        assertThat(result).contains("\"number_of_shards\":3");
        assertThat(result).contains("\"pause_pull_ingestion\":true");
        assertThat(result).contains("\"properties\"");
        assertThat(result).contains("\"field1\"");

        verify(metadataStore).getIndexSettings(clusterId, indexName);
        verify(metadataStore).getIndexMappings(clusterId, indexName);
    }

    @Test
    void testGetIndex_Success_WithoutSettingsAndMappings() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";

        // Mock dependencies - neither settings nor mappings exist
        when(metadataStore.getIndexSettings(clusterId, indexName)).thenReturn(null);
        when(metadataStore.getIndexMappings(clusterId, indexName)).thenReturn(null);

        // When
        String result = indexManager.getIndex(clusterId, indexName);

        // Then
        assertThat(result).isNotNull();
        // Check response structure: { "test-index": { "settings": {}, "mappings": {}, "aliases": {} } }
        assertThat(result).contains("\"" + indexName + "\"");
        assertThat(result).contains("\"settings\"");
        assertThat(result).contains("\"mappings\"");
        assertThat(result).contains("\"aliases\"");
        
        verify(metadataStore).getIndexSettings(clusterId, indexName);
        verify(metadataStore).getIndexMappings(clusterId, indexName);
    }

    @Test
    void testGetIndex_EmptyClusterId() throws Exception {
        // Given
        String clusterId = "";
        String indexName = "test-index";

        // When & Then
        assertThatThrownBy(() -> indexManager.getIndex(clusterId, indexName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cluster ID cannot be null or empty");

        verify(metadataStore, never()).getIndexSettings(any(), any());
        verify(metadataStore, never()).getIndexMappings(any(), any());
    }

    @Test
    void testGetIndex_EmptyIndexName() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "";

        // When & Then
        assertThatThrownBy(() -> indexManager.getIndex(clusterId, indexName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Index name cannot be null or empty");

        verify(metadataStore, never()).getIndexSettings(any(), any());
        verify(metadataStore, never()).getIndexMappings(any(), any());
    }

    @Test
    void testGetIndex_MetadataStoreException_OnGetSettings() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";

        // Mock dependencies - throw exception on getSettings
        when(metadataStore.getIndexSettings(clusterId, indexName))
                .thenThrow(new RuntimeException("Database connection failed"));
        when(metadataStore.getIndexMappings(clusterId, indexName)).thenReturn(null);

        // When - should continue gracefully even if settings fail
        String result = indexManager.getIndex(clusterId, indexName);

        // Then - should still return basic index structure without settings
        assertThat(result).isNotNull();
        assertThat(result).contains("\"" + indexName + "\"");
        
        verify(metadataStore).getIndexSettings(clusterId, indexName);
        verify(metadataStore).getIndexMappings(clusterId, indexName);
    }

    @Test
    void testGetIndex_MetadataStoreException_OnGetMappings() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";

        IndexSettings mockSettings = new IndexSettings();
        mockSettings.setNumberOfShards(1);

        // Mock dependencies - throw exception on getMappings
        when(metadataStore.getIndexSettings(clusterId, indexName)).thenReturn(mockSettings);
        when(metadataStore.getIndexMappings(clusterId, indexName))
                .thenThrow(new RuntimeException("Mapping retrieval failed"));

        // When - should continue gracefully even if mappings fail
        String result = indexManager.getIndex(clusterId, indexName);

        // Then - should return index with settings but without mappings
        assertThat(result).isNotNull();
        assertThat(result).contains("\"" + indexName + "\"");
        assertThat(result).contains("\"number_of_shards\":1");
        
        verify(metadataStore).getIndexSettings(clusterId, indexName);
        verify(metadataStore).getIndexMappings(clusterId, indexName);
    }
}