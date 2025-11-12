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

        try {
            log.info("Proxying {} request for cluster '{}', path: {}", method, clusterId, path);

            // Step 1: Select a healthy coordinator
            SearchUnit coordinator = coordinatorSelector.selectCoordinator(clusterId);
            String coordinatorUrl = coordinatorSelector.buildCoordinatorUrl(coordinator);

            log.info("Selected coordinator '{}' at {}", coordinator.getName(), coordinatorUrl);

            // Step 2: Forward request to coordinator
            ResponseEntity<String> response = httpForwarder.forward(
                    coordinatorUrl,
                    method,
                    path,
                    body,
                    headers
            );

            // Step 3: Build proxy response
            ProxyResponse proxyResponse = ProxyResponse.builder()
                    .status(response.getStatusCode().value())
                    .body(response.getBody())
                    .coordinator(coordinator.getName())
                    .build();

            log.info("Successfully proxied request to coordinator '{}', status: {}", 
                    coordinator.getName(), response.getStatusCode().value());

            return proxyResponse;

        } catch (Exception e) {
            log.error("Error proxying request for cluster '{}': {}", clusterId, e.getMessage(), e);
            
            // Return error response
            return ProxyResponse.builder()
                    .status(500)
                    .error("Error proxying request: " + e.getMessage())
                    .build();
        }
    }
}

