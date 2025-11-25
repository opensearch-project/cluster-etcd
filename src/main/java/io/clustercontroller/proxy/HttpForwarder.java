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
            // Validate coordinator URL
            if (coordinatorUrl == null || coordinatorUrl.trim().isEmpty()) {
                log.error("Coordinator URL is null or empty");
                throw new IllegalArgumentException("Coordinator URL cannot be null or empty");
            }
            
            // Build full target URL
            String targetUrl = coordinatorUrl + path;
            log.info("=== HTTP FORWARD START ===");
            log.info("Coordinator URL: {}", coordinatorUrl);
            log.info("Target Path: {}", path);
            log.info("Full URL: {}", targetUrl);
            log.info("Method: {}", method);
            log.info("Body size: {} bytes", body != null ? body.length() : 0);
            log.info("Headers: {}", headers != null ? headers.keySet() : "none");

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

            // Set method and body
            if (body != null && !body.isEmpty()) {
                requestBuilder.method(method, HttpRequest.BodyPublishers.ofString(body));
            } else {
                requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
            }

            HttpRequest request = requestBuilder.build();

            log.info("Sending HTTP request to coordinator...");
            long startTime = System.currentTimeMillis();
            
            // Make HTTP call
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            
            long duration = System.currentTimeMillis() - startTime;
            log.info("Response received: status={}, duration={}ms", response.statusCode(), duration);
            log.info("Response body size: {} bytes", response.body() != null ? response.body().length() : 0);

            // Convert to Spring ResponseEntity and forward response headers
            ResponseEntity.BodyBuilder responseBuilder = ResponseEntity.status(response.statusCode());
            
            // Forward response headers from coordinator to client
            response.headers().map().forEach((headerName, headerValues) -> {
                // Forward all response headers
                for (String headerValue : headerValues) {
                    responseBuilder.header(headerName, headerValue);
                }
            });
            
            log.info("=== HTTP FORWARD SUCCESS ===");
            return responseBuilder.body(response.body());

        } catch (java.net.ConnectException e) {
            log.error("=== HTTP FORWARD FAILED: ConnectException ===");
            log.error("Target URL: {}", coordinatorUrl);
            log.error("Error details: {}", e.getMessage());
            log.error("Possible causes:");
            log.error("  1. Coordinator is not running at {}", coordinatorUrl);
            log.error("  2. Coordinator port {} is not accessible", coordinatorUrl);
            log.error("  3. Network firewall blocking connection");
            log.error("  4. Wrong host/port configuration");
            
            String errorMsg = String.format("Failed to connect to coordinator at %s. " +
                "Please verify the coordinator is running and the address/port are correct. Error: %s", 
                coordinatorUrl, e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("Error forwarding request: Connection failed - " + errorMsg);
        } catch (java.net.UnknownHostException e) {
            log.error("=== HTTP FORWARD FAILED: UnknownHostException ===");
            log.error("Target URL: {}", coordinatorUrl);
            log.error("Hostname cannot be resolved: {}", e.getMessage());
            log.error("Possible causes:");
            log.error("  1. Hostname '{}' is not in DNS", e.getMessage());
            log.error("  2. /etc/hosts does not have an entry for this hostname");
            log.error("  3. Hostname is a tunnel/alias that's not configured");
            log.error("  4. Network DNS is not accessible");
            
            String errorMsg = String.format("Cannot resolve coordinator hostname from URL %s. " +
                "Please verify the coordinator address is correct and DNS-resolvable. Error: %s", 
                coordinatorUrl, e.getMessage());
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body("Error forwarding request: Unknown host - " + errorMsg);
        } catch (java.net.SocketTimeoutException e) {
            log.error("=== HTTP FORWARD FAILED: SocketTimeoutException ===");
            log.error("Target URL: {}", coordinatorUrl);
            log.error("Request timed out: {}", e.getMessage());
            log.error("Coordinator may be too slow to respond or experiencing high load");
            
            String errorMsg = String.format("Request to coordinator at %s timed out. Error: %s", 
                coordinatorUrl, e.getMessage());
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .body("Error forwarding request: Timeout - " + errorMsg);
        } catch (Exception e) {
            log.error("=== HTTP FORWARD FAILED: {} ===", e.getClass().getSimpleName());
            log.error("Target URL: {}", coordinatorUrl);
            log.error("Error type: {}", e.getClass().getName());
            log.error("Error message: {}", e.getMessage());
            log.error("Stack trace:", e);
            
            String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Error forwarding request: " + errorMsg);
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
