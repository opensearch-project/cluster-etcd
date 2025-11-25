package io.clustercontroller.proxy;

import io.clustercontroller.api.models.responses.ProxyResponse;
import io.clustercontroller.models.SearchUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;

import java.util.Map;

/**
 * Business logic for proxying requests to coordinator nodes.
 * Orchestrates coordinator selection and request forwarding.
 */
@Slf4j
public class CoordinatorProxy {

    private final CoordinatorSelector coordinatorSelector;
    private final HttpForwarder httpForwarder;

    public CoordinatorProxy(CoordinatorSelector coordinatorSelector, HttpForwarder httpForwarder) {
        this.coordinatorSelector = coordinatorSelector;
        this.httpForwarder = httpForwarder;
    }

    /**
     * Forward a request to a healthy coordinator in the specified cluster.
     *
     * @param clusterId Cluster ID
     * @param method HTTP method (GET, POST, etc.)
     * @param path Target path (e.g., "/logs-2024/_search?size=10")
     * @param body Request body (can be null)
     * @param headers HTTP headers to forward
     * @return ProxyResponse with status, body, and coordinator info
     */
    public ProxyResponse forwardRequest(
            String clusterId,
            String method,
            String path,
            String body,
            Map<String, String> headers) {

        SearchUnit selectedCoordinator = null;
        try {
            log.info("========================================");
            log.info("PROXY REQUEST STARTED");
            log.info("Cluster ID: {}", clusterId);
            log.info("Method: {}", method);
            log.info("Path: {}", path);
            log.info("Body present: {}", body != null && !body.isEmpty());
            log.info("Headers: {}", headers != null ? headers.size() : 0);
            log.info("========================================");

            // Step 1: Select a healthy coordinator
            log.info("Step 1: Selecting coordinator for cluster '{}'", clusterId);
            selectedCoordinator = coordinatorSelector.selectCoordinator(clusterId);
            log.info("Step 1: Coordinator selected: '{}'", selectedCoordinator.getName());
            
            // Step 2: Build coordinator URL
            log.info("Step 2: Building coordinator URL");
            String coordinatorUrl = coordinatorSelector.buildCoordinatorUrl(selectedCoordinator);
            log.info("Step 2: Coordinator URL built: {}", coordinatorUrl);

            // Step 3: Forward request to coordinator
            log.info("Step 3: Forwarding request to coordinator via HTTP");
            ResponseEntity<String> response = httpForwarder.forward(
                    coordinatorUrl,
                    method,
                    path,
                    body,
                    headers
            );
            log.info("Step 3: HTTP forward completed with status: {}", response.getStatusCode());

            // Step 4: Build proxy response
            log.info("Step 4: Building proxy response");
            ProxyResponse proxyResponse = ProxyResponse.builder()
                    .status(response.getStatusCode().value())
                    .body(response.getBody())
                    .coordinator(selectedCoordinator.getName())
                    .build();

            log.info("========================================");
            log.info("PROXY REQUEST COMPLETED SUCCESSFULLY");
            log.info("Coordinator: {}", selectedCoordinator.getName());
            log.info("Status: {}", response.getStatusCode().value());
            log.info("Response body size: {} bytes", response.getBody() != null ? response.getBody().length() : 0);
            log.info("========================================");

            return proxyResponse;

        } catch (IllegalArgumentException e) {
            log.error("========================================");
            log.error("PROXY REQUEST FAILED: Invalid Configuration");
            log.error("Cluster: {}", clusterId);
            log.error("Coordinator: {}", selectedCoordinator != null ? selectedCoordinator.getName() : "unknown");
            log.error("Error: {}", e.getMessage());
            log.error("========================================");
            
            return ProxyResponse.builder()
                    .status(503)
                    .error("Invalid coordinator configuration: " + e.getMessage())
                    .coordinator(selectedCoordinator != null ? selectedCoordinator.getName() : "unknown")
                    .build();
        } catch (Exception e) {
            log.error("========================================");
            log.error("PROXY REQUEST FAILED: Exception");
            log.error("Cluster: {}", clusterId);
            log.error("Coordinator: {}", selectedCoordinator != null ? selectedCoordinator.getName() : "unknown");
            log.error("Exception type: {}", e.getClass().getName());
            log.error("Error message: {}", e.getMessage());
            log.error("Stack trace:", e);
            log.error("========================================");
            
            // Return error response with coordinator info if available
            return ProxyResponse.builder()
                    .status(500)
                    .error("Error proxying request: " + e.getMessage())
                    .coordinator(selectedCoordinator != null ? selectedCoordinator.getName() : "unknown")
                    .build();
        }
    }
}

