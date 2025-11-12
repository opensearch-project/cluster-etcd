package io.clustercontroller.proxy;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Utility for forwarding HTTP requests to coordinator nodes.
 * Handles making HTTP calls with proper headers and body.
 */
@Slf4j
public class HttpForwarder {

    private final HttpClient httpClient;

    public HttpForwarder() {
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(30))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    }

    /**
     * Forward an HTTP request to a coordinator node.
     *
     * @param coordinatorUrl Full URL of coordinator (e.g., "http://10.0.0.5:9200")
     * @param method HTTP method (GET, POST, etc.)
     * @param path Target path (e.g., "/index/_search?size=10")
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

            // Build HTTP request
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .timeout(Duration.ofSeconds(60));

            // Add headers (filter out restricted headers that HttpClient sets automatically)
            if (headers != null) {
                headers.forEach((key, value) -> {
                    if (!isRestrictedHeader(key)) {
                        requestBuilder.header(key, value);
                    }
                });
            }
            
            requestBuilder.header("Accept", "application/json");

            // Set method and body
            if (body != null && !body.isEmpty()) {
                requestBuilder.header("Content-Type", "application/json");
                requestBuilder.method(method, HttpRequest.BodyPublishers.ofString(body));
            } else {
                requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
            }

            HttpRequest request = requestBuilder.build();

            // Make HTTP call
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            log.info("Response: status={}", response.statusCode());

            // Convert to Spring ResponseEntity
            return ResponseEntity
                .status(response.statusCode())
                .body(response.body());

        } catch (Exception e) {
            log.error("Error forwarding request to {}: {}", coordinatorUrl, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Error forwarding request: " + e.getMessage());
        }
    }

    /**
     * Check if a header is restricted by Java's HttpClient.
     * These headers are set automatically and cannot be overridden.
     */
    private boolean isRestrictedHeader(String headerName) {
        String lower = headerName.toLowerCase();
        return lower.equals("connection") ||
               lower.equals("content-length") ||
               lower.equals("expect") ||
               lower.equals("host") ||
               lower.equals("upgrade");
    }
}
