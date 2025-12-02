package io.clustercontroller.api.models.responses;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ErrorResponseTest {

    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }
    
    @Test
    void testNotFound_StaticFactory() {
        // When
        ErrorResponse response = ErrorResponse.notFound("test-index");
        
        // Then
        assertThat(response.getError()).isEqualTo("resource_not_found_exception");
        assertThat(response.getReason()).isEqualTo("test-index not found");
        assertThat(response.getStatus()).isEqualTo(404);
        assertThat(response.getType()).isNull(); // Not set by factory method
    }
    
    @Test
    void testNotImplemented_StaticFactory() {
        // When
        ErrorResponse response = ErrorResponse.notImplemented("Index creation");
        
        // Then
        assertThat(response.getError()).isEqualTo("not_implemented");
        assertThat(response.getReason()).isEqualTo("Index creation is not yet implemented");
        assertThat(response.getStatus()).isEqualTo(501);
    }
    
    @Test
    void testInternalError_StaticFactory() {
        // When
        ErrorResponse response = ErrorResponse.internalError("Database connection failed");
        
        // Then
        assertThat(response.getError()).isEqualTo("internal_server_error");
        assertThat(response.getReason()).isEqualTo("Database connection failed");
        assertThat(response.getStatus()).isEqualTo(500);
    }
    
    @Test
    void testSerialization_SnakeCaseConversion() throws Exception {
        // Given
        ErrorResponse response = ErrorResponse.builder()
            .error("test_error")
            .reason("Test reason")
            .status(400)
            .build();
        
        // When
        String json = objectMapper.writeValueAsString(response);
        
        // Then
        assertThat(json).contains("\"error\":\"test_error\"");
        assertThat(json).contains("\"reason\":\"Test reason\"");
        assertThat(json).contains("\"status\":400");
        assertThat(json).doesNotContain("\"type\""); // Null values excluded by @JsonInclude(NON_EMPTY)
    }
    
    @Test
    void testDeserialization_FromJson() throws Exception {
        // Given
        String json = """
            {
                "error": "validation_error",
                "reason": "Invalid request format",
                "status": 400,
                "unknown_field": "ignored"
            }
            """;
        
        // When
        ErrorResponse response = objectMapper.readValue(json, ErrorResponse.class);
        
        // Then
        assertThat(response.getError()).isEqualTo("validation_error");
        assertThat(response.getReason()).isEqualTo("Invalid request format");
        assertThat(response.getStatus()).isEqualTo(400);
        assertThat(response.getType()).isNull();
        // unknown_field is ignored due to @JsonIgnoreProperties(ignoreUnknown = true)
    }
    
    @Test
    void testBuilder_AllFields() {
        // When
        ErrorResponse response = ErrorResponse.builder()
            .error("custom_error")
            .type("validation")
            .reason("Custom validation failed")
            .status(422)
            .build();
        
        // Then
        assertThat(response.getError()).isEqualTo("custom_error");
        assertThat(response.getType()).isEqualTo("validation");
        assertThat(response.getReason()).isEqualTo("Custom validation failed");
        assertThat(response.getStatus()).isEqualTo(422);
    }
}
