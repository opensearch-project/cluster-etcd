package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.requests.IndexRequest;
import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.api.models.responses.IndexResponse;
import io.clustercontroller.indices.IndexManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class IndexHandlerTest {

    @Mock
    private IndexManager indexManager;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private IndexHandler indexHandler;

    private final String testClusterId = "test-cluster";
    private final String testIndexName = "test-index";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testCreateIndex_Success() throws Exception {
        // Given
        IndexRequest request = IndexRequest.builder().build();
        String indexInfoJson = "{\"test-index\":{\"settings\":{},\"mappings\":{},\"aliases\":{}}}";
        Map<String, Object> expectedResponse = new HashMap<>();
        expectedResponse.put("test-index", new HashMap<>());

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(indexManager.createIndex(anyString(), anyString(), anyString())).thenReturn(indexInfoJson);
        when(objectMapper.readValue(eq(indexInfoJson), eq(Object.class))).thenReturn(expectedResponse);

        // When
        ResponseEntity<Object> response = indexHandler.createIndex(testClusterId, testIndexName, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).isInstanceOf(Map.class);

        verify(indexManager).createIndex(testClusterId, testIndexName, "{}");
        verify(objectMapper).readValue(eq(indexInfoJson), eq(Object.class));
    }

    @Test
    void testCreateIndex_UnsupportedOperation() throws Exception {
        // Given
        IndexRequest request = IndexRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(indexManager.createIndex(anyString(), anyString(), anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = indexHandler.createIndex(testClusterId, testIndexName, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testCreateIndex_InternalError() throws Exception {
        // Given
        IndexRequest request = IndexRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(indexManager.createIndex(anyString(), anyString(), anyString()))
            .thenThrow(new RuntimeException("Database error"));

        // When
        ResponseEntity<Object> response = indexHandler.createIndex(testClusterId, testIndexName, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Database error");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testDeleteIndex_Success() throws Exception {
        // Given
        doNothing().when(indexManager).deleteIndex(anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.deleteIndex(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(IndexResponse.class);

        IndexResponse indexResponse = (IndexResponse) response.getBody();
        assertThat(indexResponse.isAcknowledged()).isTrue();
        assertThat(indexResponse.isShardsAcknowledged()).isTrue();
        assertThat(indexResponse.getIndex()).isEqualTo(testIndexName);

        verify(indexManager).deleteIndex(testClusterId, testIndexName);
    }

    @Test
    void testDeleteIndex_UnsupportedOperation() throws Exception {
        // Given
        doThrow(new UnsupportedOperationException("Not implemented"))
            .when(indexManager).deleteIndex(anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.deleteIndex(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testGetIndex_Success() throws Exception {
        // Given
        String expectedIndexJson = "{\"test-index\":{\"settings\":{},\"mappings\":{}}}";
        when(indexManager.getIndex(anyString(), anyString())).thenReturn(expectedIndexJson);

        // When
        ResponseEntity<Object> response = indexHandler.getIndex(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(expectedIndexJson);

        verify(indexManager).getIndex(testClusterId, testIndexName);
    }

    @Test
    void testGetIndex_WithMetadata() throws Exception {
        // Given
        String expectedIndexJson = "{\"test-index\":{\"settings\":{},\"mappings\":{\"_meta\":{\"id_field\":\"uuid\"}},\"aliases\":{}}}";
        when(indexManager.getIndex(anyString(), anyString())).thenReturn(expectedIndexJson);

        // When
        ResponseEntity<Object> response = indexHandler.getIndex(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(expectedIndexJson);
        assertThat(response.getBody().toString()).contains("_meta");
        assertThat(response.getBody().toString()).contains("id_field");

        verify(indexManager).getIndex(testClusterId, testIndexName);
    }

    @Test
    void testGetIndex_IndexNotFound() throws Exception {
        // Given
        when(indexManager.getIndex(anyString(), anyString()))
            .thenThrow(new IllegalArgumentException("Index 'test-index' does not exist"));

        // When
        ResponseEntity<Object> response = indexHandler.getIndex(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("bad_request");
        assertThat(errorResponse.getReason()).contains("does not exist");
        assertThat(errorResponse.getStatus()).isEqualTo(400);
    }

    @Test
    void testGetIndex_InternalError() throws Exception {
        // Given
        when(indexManager.getIndex(anyString(), anyString()))
            .thenThrow(new RuntimeException("Etcd connection failed"));

        // When
        ResponseEntity<Object> response = indexHandler.getIndex(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Etcd connection failed");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testGetIndexSettings_Success() throws Exception {
        // Given
        String expectedSettings = "{\"number_of_shards\": 3, \"refresh_interval\": \"30s\"}";
        when(indexManager.getSettings(anyString(), anyString())).thenReturn(expectedSettings);

        // When
        ResponseEntity<Object> response = indexHandler.getIndexSettings(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(expectedSettings);

        verify(indexManager).getSettings(testClusterId, testIndexName);
    }

    @Test
    void testGetIndexSettings_Exception() throws Exception {
        // Given
        when(indexManager.getSettings(anyString(), anyString()))
            .thenThrow(new RuntimeException("Index not found"));

        // When
        ResponseEntity<Object> response = indexHandler.getIndexSettings(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Index not found");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testUpdateIndexSettings_Success() throws Exception {
        // Given
        String settingsJson = "{\"refresh_interval\":\"30s\"}";
        doNothing().when(indexManager).updateSettings(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.updateIndexSettings(testClusterId, testIndexName, settingsJson);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(IndexResponse.class);

        IndexResponse indexResponse = (IndexResponse) response.getBody();
        assertThat(indexResponse.isAcknowledged()).isTrue();
        assertThat(indexResponse.isShardsAcknowledged()).isTrue();
        assertThat(indexResponse.getIndex()).isEqualTo(testIndexName);

        verify(indexManager).updateSettings(testClusterId, testIndexName, settingsJson);
    }

    @Test
    void testUpdateIndexSettings_Exception() throws Exception {
        // Given
        String settingsJson = "{\"refresh_interval\":\"30s\"}";
        doThrow(new RuntimeException("Failed to update settings"))
            .when(indexManager).updateSettings(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.updateIndexSettings(testClusterId, testIndexName, settingsJson);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Failed to update settings");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testGetIndexMapping_Success() throws Exception {
        // Given
        String expectedMappings = "{\"properties\":{\"title\":{\"type\":\"text\"}}}";
        when(indexManager.getMapping(anyString(), anyString())).thenReturn(expectedMappings);

        // When
        ResponseEntity<Object> response = indexHandler.getIndexMapping(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(expectedMappings);

        verify(indexManager).getMapping(testClusterId, testIndexName);
    }

    @Test
    void testGetIndexMapping_IndexNotFound() throws Exception {
        // Given
        when(indexManager.getMapping(anyString(), anyString()))
            .thenThrow(new IllegalArgumentException("Index 'test-index' does not exist"));

        // When
        ResponseEntity<Object> response = indexHandler.getIndexMapping(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("does not exist");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testUpdateIndexMapping_Success() {
        // Given
        String mappingJson = "{\"properties\":{\"title\":{\"type\":\"text\"}}}";
        doNothing().when(indexManager).updateMapping(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.updateIndexMapping(testClusterId, testIndexName, mappingJson);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(IndexResponse.class);

        IndexResponse indexResponse = (IndexResponse) response.getBody();
        assertThat(indexResponse.isAcknowledged()).isTrue();
        assertThat(indexResponse.getIndex()).isEqualTo(testIndexName);

        verify(indexManager).updateMapping(testClusterId, testIndexName, mappingJson);
    }

    @Test
    void testUpdateIndexMetadata_Success() throws Exception {
        // Given
        String metadataJson = "{\"id_field\":\"uuid\",\"version_field\":\"timestamp\"}";
        doNothing().when(indexManager).updateMetadata(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.updateIndexMetadata(testClusterId, testIndexName, metadataJson);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(IndexResponse.class);

        IndexResponse indexResponse = (IndexResponse) response.getBody();
        assertThat(indexResponse.isAcknowledged()).isTrue();
        assertThat(indexResponse.isShardsAcknowledged()).isTrue();
        assertThat(indexResponse.getIndex()).isEqualTo(testIndexName);

        verify(indexManager).updateMetadata(testClusterId, testIndexName, metadataJson);
    }

    @Test
    void testUpdateIndexMetadata_IndexNotFound() throws Exception {
        // Given
        String metadataJson = "{\"id_field\":\"uuid\"}";
        doThrow(new IllegalArgumentException("Index 'test-index' not found"))
            .when(indexManager).updateMetadata(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.updateIndexMetadata(testClusterId, testIndexName, metadataJson);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("bad_request");
        assertThat(errorResponse.getReason()).contains("not found");
        assertThat(errorResponse.getStatus()).isEqualTo(400);
    }

    @Test
    void testUpdateIndexMetadata_InvalidJson() throws Exception {
        // Given
        String invalidMetadataJson = "{invalid json}";
        doThrow(new IllegalArgumentException("Invalid metadata JSON format"))
            .when(indexManager).updateMetadata(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.updateIndexMetadata(testClusterId, testIndexName, invalidMetadataJson);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("bad_request");
        assertThat(errorResponse.getReason()).contains("Invalid metadata JSON");
        assertThat(errorResponse.getStatus()).isEqualTo(400);
    }

    @Test
    void testUpdateIndexMetadata_InternalError() throws Exception {
        // Given
        String metadataJson = "{\"id_field\":\"uuid\"}";
        doThrow(new RuntimeException("Etcd write failed"))
            .when(indexManager).updateMetadata(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.updateIndexMetadata(testClusterId, testIndexName, metadataJson);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Etcd write failed");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testUpdateIndexMetadata_ComplexMetadata() throws Exception {
        // Given - Complex metadata with aliases and ingestion sources
        String complexMetadataJson = "{"
            + "\"is_index_template_type\":true,"
            + "\"aliases\":[{\"name\":\"active_alias\",\"match_strategy\":\"LATEST\"}],"
            + "\"id_field\":\"uuid\","
            + "\"version_field\":\"timestamp\","
            + "\"batch_ingestion_source\":{\"hive_table\":\"db.table\"},"
            + "\"live_ingestion_source\":{\"kafka_topic\":\"topic1\",\"cluster\":\"kafka-cluster\"}"
            + "}";
        doNothing().when(indexManager).updateMetadata(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.updateIndexMetadata(testClusterId, testIndexName, complexMetadataJson);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(IndexResponse.class);

        verify(indexManager).updateMetadata(testClusterId, testIndexName, complexMetadataJson);
    }

    @Test
    void testMultiClusterSupport() throws Exception {
        // Test that different cluster IDs are handled properly
        String cluster1 = "cluster1";
        String cluster2 = "cluster2";
        IndexRequest request = IndexRequest.builder().build();
        String indexInfoJson = "{\"test-index\":{\"settings\":{},\"mappings\":{},\"aliases\":{}}}";
        Map<String, Object> expectedResponse = new HashMap<>();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        when(indexManager.createIndex(anyString(), anyString(), anyString())).thenReturn(indexInfoJson);
        when(objectMapper.readValue(eq(indexInfoJson), eq(Object.class))).thenReturn(expectedResponse);

        // When
        ResponseEntity<Object> response1 = indexHandler.createIndex(cluster1, testIndexName, request);
        ResponseEntity<Object> response2 = indexHandler.createIndex(cluster2, testIndexName, request);

        // Then
        assertThat(response1.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response2.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        verify(indexManager).createIndex(cluster1, testIndexName, "{}");
        verify(indexManager).createIndex(cluster2, testIndexName, "{}");
    }

    @Test
    void testMultiClusterSupport_Metadata() throws Exception {
        // Test that metadata updates work across different clusters
        String cluster1 = "cluster1";
        String cluster2 = "cluster2";
        String metadataJson = "{\"id_field\":\"uuid\"}";

        doNothing().when(indexManager).updateMetadata(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response1 = indexHandler.updateIndexMetadata(cluster1, testIndexName, metadataJson);
        ResponseEntity<Object> response2 = indexHandler.updateIndexMetadata(cluster2, testIndexName, metadataJson);

        // Then
        assertThat(response1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response2.getStatusCode()).isEqualTo(HttpStatus.OK);

        verify(indexManager).updateMetadata(cluster1, testIndexName, metadataJson);
        verify(indexManager).updateMetadata(cluster2, testIndexName, metadataJson);
    }
}