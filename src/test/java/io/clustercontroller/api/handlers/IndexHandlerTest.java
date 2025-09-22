package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.requests.IndexRequest;
import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.api.models.responses.IndexResponse;
import io.clustercontroller.indices.IndexManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IndexHandlerTest {

    @Mock
    private IndexManager indexManager;
    
    @Mock
    private ObjectMapper objectMapper;
    
    private IndexHandler indexHandler;
    
    @BeforeEach
    void setUp() {
        indexHandler = new IndexHandler(indexManager, objectMapper);
    }
    
    @Test
    void testCreateIndex_Success() throws Exception {
        // Given
        String indexName = "test-index";
        IndexRequest request = IndexRequest.builder()
            .settings(Map.of("replicas", 1))
            .build();
        
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"settings\":{\"replicas\":1}}");
        doNothing().when(indexManager).createIndex(anyString());
        
        // When
        ResponseEntity<Object> response = indexHandler.createIndex(indexName, request);
        
        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isInstanceOf(IndexResponse.class);
        
        IndexResponse indexResponse = (IndexResponse) response.getBody();
        assertThat(indexResponse.getAcknowledged()).isTrue();
        assertThat(indexResponse.getShardsAcknowledged()).isTrue();
        assertThat(indexResponse.getIndex()).isEqualTo(indexName);
        
        verify(indexManager).createIndex("{\"settings\":{\"replicas\":1}}");
    }
    
    @Test
    void testCreateIndex_WithNullRequest() throws Exception {
        // Given
        String indexName = "test-index";
        
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doNothing().when(indexManager).createIndex(anyString());
        
        // When
        ResponseEntity<Object> response = indexHandler.createIndex(indexName, null);
        
        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        verify(indexManager).createIndex("{}");
    }
    
    @Test
    void testCreateIndex_UnsupportedOperation() throws Exception {
        // Given
        String indexName = "test-index";
        IndexRequest request = IndexRequest.builder().build();
        
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doThrow(new UnsupportedOperationException("Not implemented"))
            .when(indexManager).createIndex(anyString());
        
        // When
        ResponseEntity<Object> response = indexHandler.createIndex(indexName, request);
        
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
        String indexName = "test-index";
        IndexRequest request = IndexRequest.builder().build();
        
        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doThrow(new RuntimeException("Database error"))
            .when(indexManager).createIndex(anyString());
        
        // When
        ResponseEntity<Object> response = indexHandler.createIndex(indexName, request);
        
        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);
        
        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }
    
    @Test
    void testGetIndex_NotImplemented() {
        // Given
        String indexName = "test-index";
        
        when(indexManager.getIndex(indexName))
            .thenThrow(new UnsupportedOperationException("Not implemented"));
        
        // When
        ResponseEntity<Object> response = indexHandler.getIndex(indexName);
        
        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);
    }
    
    @Test
    void testDeleteIndex_Success() {
        // Given
        String indexName = "test-index";
        
        doNothing().when(indexManager).deleteIndex(indexName);
        
        // When
        ResponseEntity<Object> response = indexHandler.deleteIndex(indexName);
        
        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(IndexResponse.class);
        
        IndexResponse indexResponse = (IndexResponse) response.getBody();
        assertThat(indexResponse.getAcknowledged()).isTrue();
        assertThat(indexResponse.getIndex()).isEqualTo(indexName);
        
        verify(indexManager).deleteIndex(indexName);
    }
}
