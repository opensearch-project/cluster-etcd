package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.requests.AliasRequest;
import io.clustercontroller.api.models.responses.AliasResponse;
import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.indices.AliasManager;
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

class AliasHandlerTest {

    @Mock
    private AliasManager aliasManager;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private AliasHandler aliasHandler;
    
    private final String testClusterId = "test-cluster";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testCreateAlias_Success() throws Exception {
        // Given
        String index = "test-index";
        String alias = "test-alias";
        AliasRequest request = AliasRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doNothing().when(aliasManager).createAlias(anyString(), anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = aliasHandler.createAlias(testClusterId, index, alias, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(AliasResponse.class);

        AliasResponse aliasResponse = (AliasResponse) response.getBody();
        assertThat(aliasResponse.isAcknowledged()).isTrue();
        assertThat(aliasResponse.getAlias()).isEqualTo(alias);
        assertThat(aliasResponse.getIndex()).isEqualTo(index);

        verify(aliasManager).createAlias(testClusterId, alias, index, "{}");
    }

    @Test
    void testCreateAlias_UnsupportedOperation() throws Exception {
        // Given
        String index = "test-index";
        String alias = "test-alias";
        AliasRequest request = AliasRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doThrow(new UnsupportedOperationException("Not implemented"))
            .when(aliasManager).createAlias(anyString(), anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = aliasHandler.createAlias(testClusterId, index, alias, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testDeleteAlias_Success() throws Exception {
        // Given
        String index = "test-index";
        String alias = "test-alias";
        doNothing().when(aliasManager).deleteAlias(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = aliasHandler.deleteAlias(testClusterId, index, alias);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(AliasResponse.class);

        AliasResponse aliasResponse = (AliasResponse) response.getBody();
        assertThat(aliasResponse.isAcknowledged()).isTrue();
        assertThat(aliasResponse.getAlias()).isEqualTo(alias);
        assertThat(aliasResponse.getIndex()).isEqualTo(index);

        verify(aliasManager).deleteAlias(testClusterId, alias, index);
    }

    @Test
    void testGetAlias_NotImplemented() throws Exception {
        // Given
        String alias = "test-alias";
        when(aliasManager.getAlias(anyString(), anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = aliasHandler.getAlias(testClusterId, alias);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getReason()).contains("Get alias is not yet implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }
}