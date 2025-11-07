package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.health.ClusterHealthManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class HealthHandlerTest {

    @Mock
    private ClusterHealthManager healthManager;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private HealthHandler healthHandler;
    
    private final String testClusterId = "test-cluster";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetClusterHealth_Success() throws Exception {
        // Given
        String healthJson = "{\"cluster_name\":\"test-cluster\",\"status\":\"green\"}";
        when(healthManager.getClusterHealth(testClusterId, "cluster"))
            .thenReturn(healthJson);

        // When
        ResponseEntity<Object> response = healthHandler.getClusterHealth(testClusterId, "cluster");

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(healthJson);
    }

    @Test
    void testGetClusterHealth_WithCustomLevel() throws Exception {
        // Given
        String healthJson = "{\"cluster_name\":\"test-cluster\",\"status\":\"green\",\"indices\":{}}";
        when(healthManager.getClusterHealth(testClusterId, "indices"))
            .thenReturn(healthJson);

        // When
        ResponseEntity<Object> response = healthHandler.getClusterHealth(testClusterId, "indices");

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(healthJson);
    }

    @Test
    void testGetIndexHealth_Success() throws Exception {
        // Given
        String indexName = "test-index";
        // IndexHealthInfo JSON (without cluster_name and index wrapper fields)
        String healthJson = "{\"status\":\"green\",\"number_of_shards\":5,\"number_of_replicas\":1,\"active_shards\":5,\"relocating_shards\":0,\"initializing_shards\":0,\"unassigned_shards\":0}";
        when(healthManager.getIndexHealth(testClusterId, indexName, "indices"))
            .thenReturn(healthJson);

        // When
        ResponseEntity<Object> response = healthHandler.getIndexHealth(testClusterId, indexName, "indices");

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(healthJson);
    }

    @Test
    void testGetClusterStats_NotImplemented() {
        // Given
        when(healthManager.getClusterStats(anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = healthHandler.getClusterStats(testClusterId);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getReason()).contains("not yet implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testGetClusterHealth_InternalError() throws Exception {
        // Given
        when(healthManager.getClusterHealth(anyString(), anyString()))
            .thenThrow(new RuntimeException("Database connection failed"));

        // When
        ResponseEntity<Object> response = healthHandler.getClusterHealth(testClusterId, "cluster");

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Database connection failed");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testGetIndexHealth_InternalError() throws Exception {
        // Given
        String indexName = "test-index";
        when(healthManager.getIndexHealth(anyString(), anyString(), anyString()))
            .thenThrow(new RuntimeException("Failed to retrieve index health"));

        // When
        ResponseEntity<Object> response = healthHandler.getIndexHealth(testClusterId, indexName, "indices");

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Failed to retrieve index health");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testGetClusterInformation_ClusterLocked_Success() throws Exception {
        // Given
        String clusterInfoJson = "{\"cluster_name\":\"test-cluster\",\"name\":\"node-1\"}";
        when(healthManager.getClusterInformation(testClusterId))
            .thenReturn(clusterInfoJson);

        // When
        ResponseEntity<Object> response = healthHandler.getClusterInformation(testClusterId);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(clusterInfoJson);
    }

    @Test
    void testGetClusterInformation_ClusterNotLocked_ThrowsException() throws Exception {
        // Given
        when(healthManager.getClusterInformation(testClusterId))
            .thenThrow(new Exception("Cluster is not associated with a controller"));

        // When
        ResponseEntity<Object> response = healthHandler.getClusterInformation(testClusterId);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Cluster is not associated with a controller");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testGetClusterInformation_InternalError() throws Exception {
        // Given
        when(healthManager.getClusterInformation(testClusterId))
            .thenThrow(new RuntimeException("Failed to retrieve cluster information"));

        // When
        ResponseEntity<Object> response = healthHandler.getClusterInformation(testClusterId);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Failed to retrieve cluster information");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }
}