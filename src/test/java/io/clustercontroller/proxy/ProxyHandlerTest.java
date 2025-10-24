package io.clustercontroller.proxy;

import io.clustercontroller.api.handlers.ProxyHandler;
import io.clustercontroller.api.models.responses.ProxyResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProxyHandlerTest {

    @Mock
    private CoordinatorProxy coordinatorProxy;

    @Mock
    private HttpServletRequest httpServletRequest;

    private ProxyHandler proxyHandler;

    private final String testClusterId = "test-cluster";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        proxyHandler = new ProxyHandler(coordinatorProxy);
    }

    @Test
    void testProxyGetRequest_Success() {
        String indexName = "my-index";
        String queryString = "size=10&from=0";

        ProxyResponse proxyResponse = ProxyResponse.builder()
            .status(200)
            .body("{\"took\": 15, \"hits\": {}}")
            .coordinator("coord-1")
            .build();

        when(httpServletRequest.getHeaderNames()).thenReturn(createHeaderNames());
        when(httpServletRequest.getHeader("Content-Type")).thenReturn("application/json");
        when(httpServletRequest.getHeader("RPC-Caller")).thenReturn("test-user");
        when(httpServletRequest.getQueryString()).thenReturn(queryString);
        when(coordinatorProxy.forwardRequest(
            eq(testClusterId),
            eq("GET"),
            eq("/my-index/_search?size=10&from=0"),
            isNull(),
            any(Map.class)
        )).thenReturn(proxyResponse);

        ResponseEntity<Object> response = proxyHandler.proxyGetRequest(testClusterId, indexName, httpServletRequest);

        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(ProxyResponse.class);
        
        ProxyResponse body = (ProxyResponse) response.getBody();
        assertThat(body.getStatus()).isEqualTo(200);
        assertThat(body.getBody()).isEqualTo("{\"took\": 15, \"hits\": {}}");
        assertThat(body.getCoordinator()).isEqualTo("coord-1");

        verify(coordinatorProxy).forwardRequest(
            eq(testClusterId),
            eq("GET"),
            eq("/my-index/_search?size=10&from=0"),
            isNull(),
            any(Map.class)
        );
    }

    @Test
    void testProxyPostRequest_Success() {
        String indexName = "my-index";
        String body = "{\"query\": {\"match_all\": {}}}";

        ProxyResponse proxyResponse = ProxyResponse.builder()
            .status(200)
            .body("{\"took\": 25, \"hits\": {\"total\": 100}}")
            .coordinator("coord-2")
            .build();

        when(httpServletRequest.getHeaderNames()).thenReturn(createHeaderNames());
        when(httpServletRequest.getHeader("Content-Type")).thenReturn("application/json");
        when(httpServletRequest.getHeader("RPC-Caller")).thenReturn("test-user");
        when(httpServletRequest.getQueryString()).thenReturn(null);
        when(coordinatorProxy.forwardRequest(
            eq(testClusterId),
            eq("POST"),
            eq("/my-index/_search"),
            eq(body),
            any(Map.class)
        )).thenReturn(proxyResponse);

        ResponseEntity<Object> response = proxyHandler.proxyPostRequest(testClusterId, indexName, body, httpServletRequest);

        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isInstanceOf(ProxyResponse.class);
        
        ProxyResponse responseBody = (ProxyResponse) response.getBody();
        assertThat(responseBody.getStatus()).isEqualTo(200);
        assertThat(responseBody.getCoordinator()).isEqualTo("coord-2");

        verify(coordinatorProxy).forwardRequest(
            eq(testClusterId),
            eq("POST"),
            eq("/my-index/_search"),
            eq(body),
            any(Map.class)
        );
    }

    @Test
    void testProxyGetRequest_WithoutQueryString() {
        String indexName = "my-index";

        ProxyResponse proxyResponse = ProxyResponse.builder()
            .status(200)
            .body("{\"results\": []}")
            .coordinator("coord-1")
            .build();

        when(httpServletRequest.getHeaderNames()).thenReturn(createHeaderNames());
        when(httpServletRequest.getHeader("Content-Type")).thenReturn("application/json");
        when(httpServletRequest.getQueryString()).thenReturn(null);
        when(coordinatorProxy.forwardRequest(
            eq(testClusterId),
            eq("GET"),
            eq("/my-index/_search"),
            isNull(),
            any(Map.class)
        )).thenReturn(proxyResponse);

        ResponseEntity<Object> response = proxyHandler.proxyGetRequest(testClusterId, indexName, httpServletRequest);

        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        
        verify(coordinatorProxy).forwardRequest(
            eq(testClusterId),
            eq("GET"),
            eq("/my-index/_search"),
            isNull(),
            any(Map.class)
        );
    }

    @Test
    void testProxyGetRequest_ProxyError() {
        String indexName = "my-index";

        ProxyResponse proxyResponse = ProxyResponse.builder()
            .status(500)
            .error("No healthy coordinators found")
            .build();

        when(httpServletRequest.getHeaderNames()).thenReturn(createHeaderNames());
        when(httpServletRequest.getQueryString()).thenReturn(null);
        when(coordinatorProxy.forwardRequest(
            eq(testClusterId),
            eq("GET"),
            eq("/my-index/_search"),
            isNull(),
            any(Map.class)
        )).thenReturn(proxyResponse);

        ResponseEntity<Object> response = proxyHandler.proxyGetRequest(testClusterId, indexName, httpServletRequest);

        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        
        ProxyResponse body = (ProxyResponse) response.getBody();
        assertThat(body.getStatus()).isEqualTo(500);
        assertThat(body.getError()).isEqualTo("No healthy coordinators found");
    }

    @Test
    void testProxyPostRequest_ProxyThrowsException() {
        String indexName = "my-index";
        String body = "{\"query\": {}}";

        when(httpServletRequest.getHeaderNames()).thenReturn(createHeaderNames());
        when(httpServletRequest.getQueryString()).thenReturn(null);
        when(coordinatorProxy.forwardRequest(
            eq(testClusterId),
            eq("POST"),
            eq("/my-index/_search"),
            eq(body),
            any(Map.class)
        )).thenThrow(new RuntimeException("Connection timeout"));

        ResponseEntity<Object> response = proxyHandler.proxyPostRequest(testClusterId, indexName, body, httpServletRequest);

        assertThat(response).isNotNull();
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    // Helper methods

    private Enumeration<String> createHeaderNames() {
        return Collections.enumeration(Collections.singletonList("Content-Type"));
    }
}

