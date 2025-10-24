package io.clustercontroller.api.handlers;

import io.clustercontroller.api.models.responses.ErrorResponse;
import io.clustercontroller.api.models.responses.ProxyResponse;
import io.clustercontroller.proxy.CoordinatorProxy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * REST API handler for proxying search requests to coordinator nodes.
 *
 * Provides endpoints for forwarding client search requests to healthy coordinator nodes
 * in the cluster. The proxy automatically selects a healthy coordinator using round-robin
 * load balancing and forwards the request with all original headers and body.
 *
 * Multi-cluster supported operations:
 * - GET /{clusterId}/{indexName}/_search - Proxy search queries
 * - POST /{clusterId}/{indexName}/_search - Proxy search queries with request body
 */
@Slf4j
@RestController
@RequestMapping("/{clusterId}")
public class ProxyHandler {

    private final CoordinatorProxy coordinatorProxy;

    public ProxyHandler(CoordinatorProxy coordinatorProxy) {
        this.coordinatorProxy = coordinatorProxy;
    }

    /**
     * Proxy GET requests (e.g., search queries without body).
     * GET /{clusterId}/{indexName}/_search
     */
    @GetMapping("/{indexName}/_search")
    public ResponseEntity<Object> proxyGetRequest(
            @PathVariable String clusterId,
            @PathVariable String indexName,
            HttpServletRequest request) {
        try {
            log.info("Proxying GET request for index '{}' in cluster '{}'", indexName, clusterId);
            
            // Extract headers from request
            Map<String, String> headers = extractHeaders(request);
            
            // Build the target path (everything after cluster ID)
            String targetPath = "/" + indexName + "/_search";
            if (request.getQueryString() != null) {
                targetPath += "?" + request.getQueryString();
            }
            
            // Delegate to business logic
            ProxyResponse response = coordinatorProxy.forwardRequest(
                clusterId, 
                "GET", 
                targetPath, 
                null,  // No body for GET
                headers
            );
            
            return ResponseEntity.status(response.getStatus()).body(response);
        } catch (Exception e) {
            log.error("Error proxying GET request for index '{}' in cluster '{}': {}", indexName, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Proxy POST requests (e.g., search queries with request body).
     * POST /{clusterId}/{indexName}/_search
     */
    @PostMapping("/{indexName}/_search")
    public ResponseEntity<Object> proxyPostRequest(
            @PathVariable String clusterId,
            @PathVariable String indexName,
            @RequestBody(required = false) String body,
            HttpServletRequest request) {
        try {
            log.info("Proxying POST request for index '{}' in cluster '{}'", indexName, clusterId);
            
            // Extract headers from request
            Map<String, String> headers = extractHeaders(request);
            
            // Build the target path
            String targetPath = "/" + indexName + "/_search";
            if (request.getQueryString() != null) {
                targetPath += "?" + request.getQueryString();
            }
            
            // Delegate to business logic
            ProxyResponse response = coordinatorProxy.forwardRequest(
                clusterId, 
                "POST", 
                targetPath, 
                body,
                headers
            );
            
            return ResponseEntity.status(response.getStatus()).body(response);
        } catch (Exception e) {
            log.error("Error proxying POST request for index '{}' in cluster '{}': {}", indexName, clusterId, e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse.internalError(e.getMessage()));
        }
    }

    /**
     * Extract headers from the incoming request
     */
    private Map<String, String> extractHeaders(HttpServletRequest request) {
        Map<String, String> headers = new HashMap<>();
        Enumeration<String> headerNames = request.getHeaderNames();
        
        if (headerNames != null) {
            while (headerNames.hasMoreElements()) {
                String headerName = headerNames.nextElement();
                String headerValue = request.getHeader(headerName);
                headers.put(headerName, headerValue);
            }
        }
        
        return headers;
    }
}

