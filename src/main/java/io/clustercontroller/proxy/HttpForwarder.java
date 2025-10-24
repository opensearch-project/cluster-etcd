package io.clustercontroller.proxy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

/**
 * Utility for forwarding HTTP requests to coordinator nodes.
 * Handles making HTTP calls with proper headers and body.
 */
@Slf4j
public class HttpForwarder {

    private final RestTemplate restTemplate;

    public HttpForwarder() {
        this.restTemplate = new RestTemplate();
    }

    /**
     * Forward an HTTP request to a coordinator node.
     *
     * @param coordinatorUrl Full URL of coordinator (e.g., "http://10.0.0.5:9200")
     * @param method HTTP method (GET, POST, etc.)
     * @param path Target path (e.g., "/logs-2024/_search?size=10")
     * @param body Request body (can be null for GET)
     * @param headers HTTP headers to forward
     * @return ResponseEntity with status code and body
     */
    public ResponseEntity<String> forward(
            String coordinatorUrl,
            String method,
            String path,
            String body,
            Map<String, String> headers) {
        
        try {
            // Build full target URL
            String targetUrl = coordinatorUrl + path;
            log.info("Forwarding {} request to: {}", method, targetUrl);

            // Build HTTP headers
            HttpHeaders httpHeaders = new HttpHeaders();
            if (headers != null) {
                headers.forEach(httpHeaders::set);
            }

            // Build HTTP entity with headers and body
            HttpEntity<String> entity = new HttpEntity<>(body, httpHeaders);

            // Make HTTP call
            ResponseEntity<String> response = restTemplate.exchange(
                targetUrl,
                HttpMethod.valueOf(method),
                entity,
                String.class
            );

            log.info("Received response from coordinator: status={}", response.getStatusCode());
            return response;

        } catch (Exception e) {
            log.error("Error forwarding request to {}: {}", coordinatorUrl, e.getMessage(), e);
            // Return error response
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Error forwarding request: " + e.getMessage());
        }
    }
}

