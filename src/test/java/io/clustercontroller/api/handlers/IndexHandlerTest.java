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

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doNothing().when(indexManager).createIndex(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = indexHandler.createIndex(testClusterId, testIndexName, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isInstanceOf(IndexResponse.class);

        IndexResponse indexResponse = (IndexResponse) response.getBody();
        assertThat(indexResponse.isAcknowledged()).isTrue();
        assertThat(indexResponse.isShardsAcknowledged()).isTrue();
        assertThat(indexResponse.getIndex()).isEqualTo(testIndexName);

        verify(indexManager).createIndex(testClusterId, testIndexName, "{}");
    }

    @Test
    void testCreateIndex_UnsupportedOperation() throws Exception {
        // Given
        IndexRequest request = IndexRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doThrow(new UnsupportedOperationException("Not implemented"))
            .when(indexManager).createIndex(anyString(), anyString(), anyString());

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
        doThrow(new RuntimeException("Database error"))
            .when(indexManager).createIndex(anyString(), anyString(), anyString());

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
    void testGetIndex_NotImplemented() {
        // Given
        when(indexManager.getIndex(anyString(), anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = indexHandler.getIndex(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
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
    void testGetIndexMapping_NotImplemented() {
        // Given
        when(indexManager.getMapping(anyString(), anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = indexHandler.getIndexMapping(testClusterId, testIndexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
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
    void testMultiClusterSupport() throws Exception {
        // Test that different cluster IDs are handled properly
        String cluster1 = "cluster1";
        String cluster2 = "cluster2";
        IndexRequest request = IndexRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doNothing().when(indexManager).createIndex(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response1 = indexHandler.createIndex(cluster1, testIndexName, request);
        ResponseEntity<Object> response2 = indexHandler.createIndex(cluster2, testIndexName, request);

        // Then
        assertThat(response1.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response2.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        verify(indexManager).createIndex(cluster1, testIndexName, "{}");
        verify(indexManager).createIndex(cluster2, testIndexName, "{}");
    }
}