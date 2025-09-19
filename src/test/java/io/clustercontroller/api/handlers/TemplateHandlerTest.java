package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.requests.TemplateRequest;
import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.api.models.responses.TemplateResponse;
import io.clustercontroller.templates.TemplateManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class TemplateHandlerTest {

    @Mock
    private TemplateManager templateManager;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private TemplateHandler templateHandler;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testCreateTemplate_Success() throws Exception {
        // Given
        String templateName = "test-template";
        TemplateRequest request = TemplateRequest.builder()
            .indexPatterns(List.of("logs-*"))
            .priority(100)
            .build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{\"index_patterns\":[\"logs-*\"],\"priority\":100}");
        doNothing().when(templateManager).putTemplate(anyString(), anyString());

        // When
        ResponseEntity<Object> response = templateHandler.createTemplate(templateName, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(TemplateResponse.class);

        TemplateResponse templateResponse = (TemplateResponse) response.getBody();
        assertThat(templateResponse.isAcknowledged()).isTrue();
        assertThat(templateResponse.getTemplate()).isEqualTo(templateName);

        verify(templateManager).putTemplate(templateName, "{\"index_patterns\":[\"logs-*\"],\"priority\":100}");
    }

    @Test
    void testCreateTemplate_UnsupportedOperation() throws Exception {
        // Given
        String templateName = "test-template";
        TemplateRequest request = TemplateRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doThrow(new UnsupportedOperationException("Not implemented"))
            .when(templateManager).putTemplate(anyString(), anyString());

        // When
        ResponseEntity<Object> response = templateHandler.createTemplate(templateName, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testCreateTemplate_InternalError() throws Exception {
        // Given
        String templateName = "test-template";
        TemplateRequest request = TemplateRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doThrow(new RuntimeException("Template validation failed"))
            .when(templateManager).putTemplate(anyString(), anyString());

        // When
        ResponseEntity<Object> response = templateHandler.createTemplate(templateName, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("internal_server_error");
        assertThat(errorResponse.getReason()).contains("Template validation failed");
        assertThat(errorResponse.getStatus()).isEqualTo(500);
    }

    @Test
    void testGetTemplate_NotImplemented() {
        // Given
        String templateName = "test-template";
        when(templateManager.getTemplate(anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = templateHandler.getTemplate(templateName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getReason()).contains("Get template is not yet implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testDeleteTemplate_Success() {
        // Given
        String templateName = "test-template";
        doNothing().when(templateManager).deleteTemplate(anyString());

        // When
        ResponseEntity<Object> response = templateHandler.deleteTemplate(templateName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(TemplateResponse.class);

        TemplateResponse templateResponse = (TemplateResponse) response.getBody();
        assertThat(templateResponse.isAcknowledged()).isTrue();
        assertThat(templateResponse.getTemplate()).isEqualTo(templateName);

        verify(templateManager).deleteTemplate(templateName);
    }

    @Test
    void testDeleteTemplate_UnsupportedOperation() {
        // Given
        String templateName = "test-template";
        doThrow(new UnsupportedOperationException("Not implemented"))
            .when(templateManager).deleteTemplate(anyString());

        // When
        ResponseEntity<Object> response = templateHandler.deleteTemplate(templateName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }

    @Test
    void testGetAllTemplates_NotImplemented() {
        // Given
        when(templateManager.getTemplate(anyString()))
            .thenThrow(new UnsupportedOperationException("Not implemented"));

        // When
        ResponseEntity<Object> response = templateHandler.getAllTemplates();

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_IMPLEMENTED);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("not_implemented");
        assertThat(errorResponse.getReason()).contains("Get all templates is not yet implemented");
        assertThat(errorResponse.getStatus()).isEqualTo(501);
    }
}
