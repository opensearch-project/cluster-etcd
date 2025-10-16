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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class TemplateHandlerTest {

    @Mock
    private TemplateManager templateManager;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private TemplateHandler templateHandler;
    
    private final String testClusterId = "test-cluster";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testCreateTemplate_Success() throws Exception {
        // Given
        String templateName = "test-template";
        TemplateRequest request = TemplateRequest.builder()
            .instanceName("prod-cluster")
            .region("us-west-2")
            .indexTemplateName("test-template")
            .indexTemplatePattern("logs-*")
            .build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{\"instance_name\":\"prod-cluster\",\"region\":\"us-west-2\",\"index_template_name\":\"test-template\",\"index_template_pattern\":\"logs-*\"}");
        doNothing().when(templateManager).putTemplate(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = templateHandler.createTemplate(testClusterId, templateName, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(TemplateResponse.class);

        TemplateResponse templateResponse = (TemplateResponse) response.getBody();
        assertThat(templateResponse.isAcknowledged()).isTrue();
        assertThat(templateResponse.getTemplate()).isEqualTo(templateName);

        verify(templateManager).putTemplate(eq(testClusterId), eq(templateName), anyString());
    }

    @Test
    void testCreateTemplate_InvalidRequest() throws Exception {
        // Given
        String templateName = "test-template";
        TemplateRequest request = TemplateRequest.builder().build();

        when(objectMapper.writeValueAsString(any())).thenReturn("{}");
        doThrow(new IllegalArgumentException("Template must have an index pattern"))
            .when(templateManager).putTemplate(anyString(), anyString(), anyString());

        // When
        ResponseEntity<Object> response = templateHandler.createTemplate(testClusterId, templateName, request);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("bad_request");
    }

    @Test
    void testGetTemplate_NotFound() throws Exception {
        // Given
        String templateName = "test-template";
        when(templateManager.getTemplate(anyString(), anyString()))
            .thenThrow(new IllegalArgumentException("Template 'test-template' not found"));

        // When
        ResponseEntity<Object> response = templateHandler.getTemplate(testClusterId, templateName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody()).isInstanceOf(ErrorResponse.class);

        ErrorResponse errorResponse = (ErrorResponse) response.getBody();
        assertThat(errorResponse.getError()).isEqualTo("resource_not_found_exception");
    }

    @Test
    void testDeleteTemplate_Success() throws Exception {
        // Given
        String templateName = "test-template";
        doNothing().when(templateManager).deleteTemplate(anyString(), anyString());

        // When
        ResponseEntity<Object> response = templateHandler.deleteTemplate(testClusterId, templateName);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(TemplateResponse.class);

        TemplateResponse templateResponse = (TemplateResponse) response.getBody();
        assertThat(templateResponse.isAcknowledged()).isTrue();
        assertThat(templateResponse.getTemplate()).isEqualTo(templateName);

        verify(templateManager).deleteTemplate(testClusterId, templateName);
    }

    @Test
    void testGetAllTemplates_Success() throws Exception {
        // Given
        String templatesJson = "{\"index_templates\":{}}";
        when(templateManager.getAllTemplates(anyString())).thenReturn(templatesJson);

        // When
        ResponseEntity<Object> response = templateHandler.getAllTemplates(testClusterId);

        // Then
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(templatesJson);

        verify(templateManager).getAllTemplates(testClusterId);
    }
}