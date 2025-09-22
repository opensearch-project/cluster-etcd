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
        doNothing().when(aliasManager).createAlias(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = aliasHandler.createAlias(index, alias, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(AliasResponse.class);

        AliasResponse aliasResponse = (AliasResponse) response.getBody();
        assertThat(aliasResponse.isAcknowledged()).isTrue();
        assertThat(aliasResponse.getAlias()).isEqualTo(alias);
        assertThat(aliasResponse.getIndex()).isEqualTo(index);

        verify(aliasManager).createAlias(alias, index, "{}");
    }

    @Test
    void testCreateAlias_UnsupportedOperation() throws Exception {
        // Given
        String index = "test-index";
        String alias = "test-alias";
        AliasRequest request = AliasRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doThrow(new UnsupportedOperationException("Not implemented"))
            .when(aliasManager).createAlias(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = aliasHandler.createAlias(index, alias, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testCreateAlias_InternalError() throws Exception {
        // Given
        String index = "test-index";
        String alias = "test-alias";
        AliasRequest request = AliasRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doThrow(new RuntimeException("Database error"))
            .when(aliasManager).createAlias(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = aliasHandler.createAlias(index, alias, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Database error");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testDeleteAlias_Success() {
        // Given
        String index = "test-index";
        String alias = "test-alias";
        doNothing().when(aliasManager).deleteAlias(anyString(), anyString());

        // When
        ResponseEntity<Object> response = aliasHandler.deleteAlias(index, alias);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(AliasResponse.class);

        AliasResponse aliasResponse = (AliasResponse) response.getBody();
        assertThat(aliasResponse.isAcknowledged()).isTrue();
        assertThat(aliasResponse.getAlias()).isEqualTo(alias);
        assertThat(aliasResponse.getIndex()).isEqualTo(index);

        verify(aliasManager).deleteAlias(alias, index);
    }

    @Test
    void testDeleteAlias_UnsupportedOperation() {
        // Given
        String index = "test-index";
        String alias = "test-alias";
        doThrow(new UnsupportedOperationException("Not implemented"))
            .when(aliasManager).deleteAlias(anyString(), anyString());

        // When
        ResponseEntity<Object> response = aliasHandler.deleteAlias(index, alias);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testGetAlias_NotImplemented() {
        // Given
        String alias = "test-alias";
        when(aliasManager.getAlias(anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = aliasHandler.getAlias(alias);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getReason()).contains("Get alias is not yet implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testGetIndexAliases_NotImplemented() {
        // Given
        String index = "test-index";
        when(aliasManager.getAlias(anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = aliasHandler.getIndexAliases(index);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getReason()).contains("Get index aliases is not yet implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }
}
