package io.clustercontroller.proxy;

import io.clustercontroller.api.models.responses.ProxyResponse;
import io.clustercontroller.models.SearchUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoordinatorProxyTest {

    @Mock
    private CoordinatorSelector coordinatorSelector;

    @Mock
    private HttpForwarder httpForwarder;

    private CoordinatorProxy coordinatorProxy;

    private final String testClusterId = "test-cluster";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        coordinatorProxy = new CoordinatorProxy(coordinatorSelector, httpForwarder);
    }

    @Test
    void testForwardRequest_Success() throws Exception {
        // Given
        String method = "POST";
        String path = "/logs-2024/_search?size=10";
        String body = "{\"query\": {\"match_all\": {}}}";
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("RPC-Caller", "test-user");

        SearchUnit coordinator = createSearchUnit("coord-1", "10.0.0.1");
        String coordinatorUrl = "http://10.0.0.1:9200";

        ResponseEntity<String> httpResponse = ResponseEntity.ok("{\"took\": 15, \"hits\": {}}");

        when(coordinatorSelector.selectCoordinator(testClusterId)).thenReturn(coordinator);
        when(coordinatorSelector.buildCoordinatorUrl(coordinator)).thenReturn(coordinatorUrl);
        when(httpForwarder.forward(coordinatorUrl, method, path, body, headers)).thenReturn(httpResponse);

        // When
        ProxyResponse response = coordinatorProxy.forwardRequest(testClusterId, method, path, body, headers);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo("{\"took\": 15, \"hits\": {}}");
        assertThat(response.getCoordinator()).isEqualTo("coord-1");
        assertThat(response.getError()).isNull();

        verify(coordinatorSelector).selectCoordinator(testClusterId);
        verify(httpForwarder).forward(coordinatorUrl, method, path, body, headers);
    }

    @Test
    void testForwardRequest_CoordinatorSelectionFails() throws Exception {
        // Given
        when(coordinatorSelector.selectCoordinator(testClusterId))
            .thenThrow(new Exception("No healthy coordinators found"));

        // When
        ProxyResponse response = coordinatorProxy.forwardRequest(
            testClusterId, "GET", "/logs/_search", null, new HashMap<>()
        );

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(500);
        assertThat(response.getError()).contains("No healthy coordinators found");
        assertThat(response.getBody()).isNull();
        assertThat(response.getCoordinator()).isNull();
    }

    @Test
    void testForwardRequest_HttpForwardingFails() throws Exception {
        // Given
        SearchUnit coordinator = createSearchUnit("coord-1", "10.0.0.1");
        String coordinatorUrl = "http://10.0.0.1:9200";

        when(coordinatorSelector.selectCoordinator(testClusterId)).thenReturn(coordinator);
        when(coordinatorSelector.buildCoordinatorUrl(coordinator)).thenReturn(coordinatorUrl);
        when(httpForwarder.forward(anyString(), anyString(), anyString(), any(), any()))
            .thenThrow(new RuntimeException("Connection refused"));

        // When
        ProxyResponse response = coordinatorProxy.forwardRequest(
            testClusterId, "GET", "/logs/_search", null, new HashMap<>()
        );

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(500);
        assertThat(response.getError()).contains("Connection refused");
    }

    @Test
    void testForwardRequest_GetRequest() throws Exception {
        // Given
        String method = "GET";
        String path = "/logs-2024/_search?q=error";
        Map<String, String> headers = new HashMap<>();

        SearchUnit coordinator = createSearchUnit("coord-2", "10.0.0.2");
        String coordinatorUrl = "http://10.0.0.2:9200";

        ResponseEntity<String> httpResponse = ResponseEntity.ok("{\"results\": []}");

        when(coordinatorSelector.selectCoordinator(testClusterId)).thenReturn(coordinator);
        when(coordinatorSelector.buildCoordinatorUrl(coordinator)).thenReturn(coordinatorUrl);
        when(httpForwarder.forward(coordinatorUrl, method, path, null, headers)).thenReturn(httpResponse);

        // When
        ProxyResponse response = coordinatorProxy.forwardRequest(testClusterId, method, path, null, headers);

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(response.getBody()).isEqualTo("{\"results\": []}");
        assertThat(response.getCoordinator()).isEqualTo("coord-2");
    }

    @Test
    void testForwardRequest_NonOkStatus() throws Exception {
        // Given
        SearchUnit coordinator = createSearchUnit("coord-1", "10.0.0.1");
        String coordinatorUrl = "http://10.0.0.1:9200";

        ResponseEntity<String> httpResponse = ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body("{\"error\": \"Index not found\"}");

        when(coordinatorSelector.selectCoordinator(testClusterId)).thenReturn(coordinator);
        when(coordinatorSelector.buildCoordinatorUrl(coordinator)).thenReturn(coordinatorUrl);
        when(httpForwarder.forward(anyString(), anyString(), anyString(), any(), any()))
            .thenReturn(httpResponse);

        // When
        ProxyResponse response = coordinatorProxy.forwardRequest(
            testClusterId, "GET", "/nonexistent/_search", null, new HashMap<>()
        );

        // Then
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(404);
        assertThat(response.getBody()).isEqualTo("{\"error\": \"Index not found\"}");
        assertThat(response.getCoordinator()).isEqualTo("coord-1");
    }

    // Helper methods

    private SearchUnit createSearchUnit(String name, String host) {
        SearchUnit unit = new SearchUnit();
        unit.setName(name);
        unit.setHost(host);
        unit.setRole("COORDINATOR");
        unit.setPortHttp(9200);
        return unit;
    }
}

