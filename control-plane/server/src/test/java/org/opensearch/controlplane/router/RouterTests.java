/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.router;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RouterTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Router generatedRouter() {
        return Router.fromGeneratedRoutes(HandlerRegistry.withAllStubbed(), TestContexts.factory());
    }

    private static JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    @Test
    void generatedSurfaceIsNonEmptyAndBuildsWithoutConflicts() {
        Router router = generatedRouter();
        assertFalse(GeneratedRoutes.routes().isEmpty(), "generated route table should not be empty");
        assertEquals(GeneratedRoutes.routes().size(), router.trie().size());
    }

    @Test
    void everyGeneratedRouteResolvesToItsOwnOperationGroup() {
        Router router = generatedRouter();
        for (Route route : GeneratedRoutes.routes()) {
            // Substitute a concrete value for each placeholder.
            String concrete = route.path().replaceAll("\\{[^}]+}", "x");
            PathTrie.Match match = router.trie().resolve(route.method(), concrete);
            assertNotNull(match, () -> "no match for " + route.method() + " " + concrete
                + " (from template " + route.path() + ")");
            assertEquals(
                route.operationGroup(),
                match.route().operationGroup(),
                () -> route.method() + " " + concrete + " resolved to the wrong operation group"
            );
        }
    }

    @Test
    void stubbedOperationReturns501WithStructuredError() throws Exception {
        RestResponse response = generatedRouter().dispatch("PUT", "/my-index");

        assertEquals(RestStatus.NOT_IMPLEMENTED, response.status());
        JsonNode json = body(response);
        assertEquals(501, json.get("status").asInt());
        assertTrue(json.get("error").isObject(), "handler errors use the structured ErrorCause shape");
        assertEquals("not_implemented_exception", json.get("error").get("type").asText());
        assertEquals("indices.create", json.get("error").get("operation_group").asText());
    }

    @Test
    void unknownPathReturns400WithBareStringError() throws Exception {
        // Note a single junk segment would NOT land here: GET /anything matches
        // GET /{index} (indices.get), because any one segment is a valid index
        // name. That is OpenSearch's behaviour too. Three junk segments do not
        // route anywhere.
        RestResponse response = generatedRouter().dispatch("GET", "/_nope/_nope/_nope");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode json = body(response);
        // Verified against a live cluster: lower-case message, and no status field at all -- the
        // only response in the API that omits it.
        assertTrue(json.get("error").isTextual(), "no-handler errors use the bare string shape");
        assertEquals("no handler found for uri [/_nope/_nope/_nope] and method [GET]", json.get("error").asText());
        assertFalse(json.has("status"), "handleBadRequest hand-builds the body with only an error field");
        assertEquals(1, json.size());
    }

    @Test
    void aSingleUnknownSegmentIsTreatedAsAnIndexName() {
        // Documents the consequence of /{index} being a registered route.
        RestResponse response = generatedRouter().dispatch("GET", "/_definitely_not_an_api");
        assertEquals(RestStatus.NOT_IMPLEMENTED, response.status());
    }

    @Test
    void wrongMethodReturns405WithAllowHeader() throws Exception {
        Router router = generatedRouter();
        // /_cluster/health is GET-only in the spec.
        RestResponse response = router.dispatch("DELETE", "/_cluster/health");

        assertEquals(RestStatus.METHOD_NOT_ALLOWED, response.status());
        assertEquals("GET", response.headers().get("Allow"));
        // Verified against a live cluster, including the capitalisation and the presence of status.
        JsonNode json = body(response);
        assertEquals(
            "Incorrect HTTP method for uri [/_cluster/health] and method [DELETE], allowed: [GET]",
            json.get("error").asText()
        );
        assertEquals(405, json.get("status").asInt());
    }

    @Test
    void structuredErrorsCarryARootCauseArray() throws Exception {
        // Verified: the server repeats the cause in root_cause, extra fields included.
        JsonNode error = body(generatedRouter().dispatch("PUT", "/my-index")).get("error");

        assertTrue(error.get("root_cause").isArray());
        assertEquals(1, error.get("root_cause").size());
        JsonNode rootCause = error.get("root_cause").get(0);
        assertEquals(error.get("type").asText(), rootCause.get("type").asText());
        assertEquals(error.get("reason").asText(), rootCause.get("reason").asText());
        assertEquals("indices.create", rootCause.get("operation_group").asText());
    }

    @Test
    void headRequestsGetNoBody() {
        // indices.exists is HEAD /{index}
        RestResponse response = generatedRouter().dispatch("HEAD", "/my-index");

        assertEquals(RestStatus.NOT_IMPLEMENTED, response.status());
        assertEquals(0, response.body().length, "OpenSearch never sends a body for HEAD");
    }

    @Test
    void prettyParameterFormatsTheResponse() {
        Router router = generatedRouter();
        RestResponse compact = router.dispatch("PUT", "/my-index");
        RestResponse pretty = router.dispatch(
            "PUT", "/my-index", "/my-index?pretty", Map.of("pretty", List.of("")), Map.of(), new byte[0]
        );

        assertFalse(new String(compact.body(), StandardCharsets.UTF_8).contains("\n"));
        assertTrue(new String(pretty.body(), StandardCharsets.UTF_8).contains("\n"), "bare ?pretty means true");
    }

    // ------------------------------------------------------------------
    // global parameters, applied for every operation
    // ------------------------------------------------------------------

    private static RestResponse dispatchWith(Router router, String method, String path, Map<String, List<String>> params) {
        return router.dispatch(method, path, path, params, Map.of(), new byte[0]);
    }

    /**
     * A router with the real handlers registered. The global parameters apply to actual responses, so
     * they cannot be exercised against the all-stubbed registry that most of these tests use.
     */
    private static Router implementedRouter() {
        return Router.fromGeneratedRoutes(
            org.opensearch.controlplane.handler.ControlPlaneHandlers.build(), TestContexts.factory());
    }

    @Test
    void filterPathAppliesToAnySuccessResponse() throws Exception {
        Router router = implementedRouter();
        // info is implemented, so this exercises a real response rather than a 501.
        JsonNode json = body(dispatchWith(router, "GET", "/", Map.of("filter_path", List.of("cluster_name"))));

        assertEquals(1, json.size(), "only the requested path should survive");
        assertTrue(json.has("cluster_name"));
    }

    @Test
    void filterPathIsNotAppliedToErrorResponses() throws Exception {
        // Verified against a live cluster: filter_path is ignored when rendering an exception.
        JsonNode json = body(dispatchWith(
            implementedRouter(), "GET", "/_cluster/health", Map.of("filter_path", List.of("status"))));

        assertTrue(json.has("error"), "the whole error body should come back");
        assertTrue(json.get("error").has("type"));
    }

    @Test
    void filterPathComposesWithPretty() {
        RestResponse response = dispatchWith(implementedRouter(), "GET", "/",
            Map.of("filter_path", List.of("cluster_name"), "pretty", List.of("")));
        String out = new String(response.body(), StandardCharsets.UTF_8);

        assertTrue(out.contains("\n"), "still pretty");
        assertFalse(out.contains("tagline"), "still filtered");
    }

    @Test
    void sourceParameterSuppliesTheRequestBody() throws Exception {
        // The body arrives in the query string. ingest.put_pipeline needs a body, so a 200 proves the
        // substitution happened before the handler ran.
        Router router = implementedRouter();
        RestResponse response = router.dispatch(
            "PUT", "/_ingest/pipeline/p", "/_ingest/pipeline/p",
            Map.of(
                "source", List.of("{\"processors\":[]}"),
                "source_content_type", List.of("application/json")
            ),
            Map.of(), new byte[0]
        );

        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).get("acknowledged").asBoolean());
    }

    @Test
    void sourceWithoutContentTypeIs500() throws Exception {
        // Verified against a live cluster, including the surprising status: 500, not 400.
        RestResponse response = dispatchWith(implementedRouter(), "PUT", "/_ingest/pipeline/p",
            Map.of("source", List.of("{\"processors\":[]}")));

        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("illegal_state_exception", error.get("type").asText());
        assertEquals("source and source_content_type parameters are required", error.get("reason").asText());
    }

    @Test
    void anActualBodyBeatsTheSourceParameter() throws Exception {
        Router router = implementedRouter();
        RestResponse response = router.dispatch(
            "PUT", "/_ingest/pipeline/p", "/_ingest/pipeline/p",
            Map.of("source", List.of("{\"not\":\"used\"}"), "source_content_type", List.of("application/json")),
            Map.of(),
            "{\"processors\":[]}".getBytes(StandardCharsets.UTF_8)
        );

        assertEquals(RestStatus.OK, response.status());
    }

    @Test
    void errorTraceAddsAStackTraceForAGenuineException() throws Exception {
        HandlerRegistry registry = HandlerRegistry.withAllStubbed().register(new OperationHandler() {
            @Override
            public String operationGroup() {
                return "cluster.health";
            }

            @Override
            public RestResponse handle(RestRequest request, OperationContext context) {
                throw new IllegalStateException("deliberate");
            }
        });
        Router router = Router.fromGeneratedRoutes(registry, TestContexts.factory());

        JsonNode without = body(dispatchWith(router, "GET", "/_cluster/health", Map.of()));
        assertFalse(without.get("error").has("stack_trace"));

        JsonNode with = body(dispatchWith(router, "GET", "/_cluster/health", Map.of("error_trace", List.of("true"))));
        assertEquals("illegal_state_exception", with.get("error").get("type").asText());
        assertTrue(with.get("error").get("stack_trace").asText().contains("IllegalStateException"));
        // Extra fields are mirrored into root_cause, as the server does.
        assertTrue(with.get("error").get("root_cause").get(0).has("stack_trace"));
    }

    /**
     * A constructed error has no stack to report, and fabricating one would point at nothing. See
     * {@code ErrorResponses.ERROR_TRACE_LIMITATION}.
     */
    @Test
    void errorTraceAddsNothingToAConstructedError() throws Exception {
        JsonNode json = body(dispatchWith(implementedRouter(), "DELETE", "/nope",
            Map.of("error_trace", List.of("true"))));

        assertEquals("index_not_found_exception", json.get("error").get("type").asText());
        assertFalse(json.get("error").has("stack_trace"));
    }

    @Test
    void humanIsAcceptedAndIgnoredRatherThanRejected() throws Exception {
        // Deliberately unsupported, but a caller sending it gets a correct response, not an error.
        RestResponse response = dispatchWith(implementedRouter(), "GET", "/", Map.of("human", List.of("true")));

        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).has("version"));
    }

    @Test
    void aliasedRoutesShareOneHandler() {
        Router router = generatedRouter();
        // indices.put_alias is many (method, path) pairs but one operation.
        for (String path : List.of("/my-index/_alias/a", "/my-index/_aliases/a")) {
            for (String method : List.of("PUT", "POST")) {
                PathTrie.Match match = router.trie().resolve(method, path);
                assertNotNull(match, () -> method + " " + path + " should be routed");
                assertEquals("indices.put_alias", match.route().operationGroup());
            }
        }
    }

    @Test
    void registeredHandlerReplacesTheStub() throws Exception {
        HandlerRegistry registry = HandlerRegistry.withAllStubbed().register(new OperationHandler() {
            @Override
            public String operationGroup() {
                return "cluster.health";
            }

            @Override
            public RestResponse handle(RestRequest request, OperationContext context) {
                return RestResponse.json(RestStatus.OK, "{\"status\":\"green\"}");
            }
        });
        Router router = Router.fromGeneratedRoutes(registry, TestContexts.factory());

        RestResponse response = router.dispatch("GET", "/_cluster/health");
        assertEquals(RestStatus.OK, response.status());
        assertEquals("green", body(response).get("status").asText());

        assertTrue(registry.implemented().contains("cluster.health"));
        assertFalse(registry.notImplemented().contains("cluster.health"));
    }

    @Test
    void pathParametersReachTheHandler() throws Exception {
        var captured = new java.util.concurrent.atomic.AtomicReference<Map<String, String>>();
        HandlerRegistry registry = HandlerRegistry.withAllStubbed().register(new OperationHandler() {
            @Override
            public String operationGroup() {
                return "indices.put_alias";
            }

            @Override
            public RestResponse handle(RestRequest request, OperationContext context) {
                captured.set(request.pathParams());
                return RestResponse.json(RestStatus.OK, "{}");
            }
        });

        Router.fromGeneratedRoutes(registry, TestContexts.factory()).dispatch("PUT", "/logs-000001/_alias/current");

        assertEquals("logs-000001", captured.get().get("index"));
        assertEquals("current", captured.get().get("name"));
    }
}
