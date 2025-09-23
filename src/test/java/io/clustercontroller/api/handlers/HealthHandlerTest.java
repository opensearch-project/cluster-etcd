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
    void testGetClusterHealth_NotImplemented() {
        // Given
        when(healthManager.getClusterHealth(anyString(), anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = healthHandler.getClusterHealth(testClusterId, "cluster");

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getReason()).contains("not yet implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testGetClusterHealth_WithCustomLevel() {
        // Given
        when(healthManager.getClusterHealth(anyString(), anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = healthHandler.getClusterHealth(testClusterId, "indices");

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getReason()).contains("not yet implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testGetIndexHealth_NotImplemented() {
        // Given
        String indexName = "test-index";
        when(healthManager.getIndexHealth(anyString(), anyString(), anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = healthHandler.getIndexHealth(testClusterId, indexName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getReason()).contains("not yet implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
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
    void testGetClusterHealth_InternalError() {
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
}