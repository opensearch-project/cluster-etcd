package io.clustercontroller.proxy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HttpForwarderTest {

    private HttpForwarder httpForwarder;

    @BeforeEach
    void setUp() {
        httpForwarder = new HttpForwarder();
    }

    @Test
    void testForward_BuildsCorrectUrl() {
        String coordinatorUrl = "http://10.0.0.1:9200";
        String path = "/my-index/_search?size=10";
        String method = "GET";
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");

        // Note: This will fail because we're not actually connecting to a coordinator
        // In a real test, you'd mock RestTemplate
        // For now, we're just testing the structure
        ResponseEntity<String> response = httpForwarder.forward(coordinatorUrl, method, path, null, headers);

        // Should return error response since no real coordinator is available
        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void testForward_HandlesNullHeaders() {
        String coordinatorUrl = "http://10.0.0.1:9200";
        String path = "/my-index/_search";
        String method = "GET";

        ResponseEntity<String> response = httpForwarder.forward(coordinatorUrl, method, path, null, null);

        assertThat(response).isNotNull();
        // Will fail to connect, but shouldn't throw NPE
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void testForward_HandlesPostWithBody() {
        String coordinatorUrl = "http://10.0.0.1:9200";
        String path = "/my-index/_search";
        String method = "POST";
        String body = "{\"query\": {\"match_all\": {}}}";
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");

        ResponseEntity<String> response = httpForwarder.forward(coordinatorUrl, method, path, body, headers);

        assertThat(response).isNotNull();
        // Should return SERVICE_UNAVAILABLE for connection errors
        assertThat(response.getStatusCode()).isIn(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    void testForward_NullCoordinatorUrl() {
        String path = "/my-index/_search";
        String method = "GET";

        ResponseEntity<String> response = httpForwarder.forward(null, method, path, null, null);

        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).contains("cannot be null or empty");
    }

    @Test
    void testForward_EmptyCoordinatorUrl() {
        String path = "/my-index/_search";
        String method = "GET";

        ResponseEntity<String> response = httpForwarder.forward("", method, path, null, null);

        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(response.getBody()).contains("cannot be null or empty");
    }

    @Test
    void testForward_InvalidHostname() {
        String coordinatorUrl = "http://this-host-does-not-exist-12345.invalid:9200";
        String path = "/my-index/_search";
        String method = "GET";

        ResponseEntity<String> response = httpForwarder.forward(coordinatorUrl, method, path, null, null);

        assertThat(response).isNotNull();
        // Should return SERVICE_UNAVAILABLE for hostname resolution errors
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody()).containsAnyOf("Connection failed", "Unknown host");
    }
}

