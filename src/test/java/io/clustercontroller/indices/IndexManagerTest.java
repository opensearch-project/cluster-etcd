package io.clustercontroller.indices;

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
    void testDeleteIndex() throws Exception {
        // Given
        String clusterId = "test-cluster";
        String indexName = "test-index";

        // When
        indexManager.deleteIndex(clusterId, indexName);

        // Then
        // Verify the method was called (implementation is TODO)
        verify(metadataStore, never()).deleteIndexConfig(any(), any());
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

    private List<SearchUnit> createMockSearchUnits() {
        List<SearchUnit> searchUnits = new ArrayList<>();
        SearchUnit unit = new SearchUnit();
        unit.setName("test-unit-1");
        searchUnits.add(unit);
        return searchUnits;
    }
}