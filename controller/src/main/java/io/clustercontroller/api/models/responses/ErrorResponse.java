package io.clustercontroller.api.models.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Standard error response model for all API operations.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class ErrorResponse {
    private String error;
    private String type;
    private String reason;
    private Integer status;
    
    public static ErrorResponse notFound(String resource) {
        return ErrorResponse.builder()
            .error("resource_not_found_exception")
            .reason(resource + " not found")
            .status(404)
            .build();
    }
    
    public static ErrorResponse notImplemented(String operation) {
        return ErrorResponse.builder()
            .error("not_implemented")
            .reason(operation + " is not yet implemented")
            .status(501)
            .build();
    }
    
    public static ErrorResponse internalError(String message) {
        return ErrorResponse.builder()
            .error("internal_server_error")
            .reason(message)
            .status(500)
            .build();
    }
    
    public static ErrorResponse badRequest(String message) {
        return ErrorResponse.builder()
            .error("bad_request")
            .reason(message)
            .status(400)
            .build();
    }
}