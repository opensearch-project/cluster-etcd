/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The three template kinds, all verified against OpenSearch 3.9.0.
 *
 * <p>Unlike the pipelines, these are genuinely different APIs rather than one API twice, so the tests
 * assert the differences as much as the commonality.
 */
class TemplateHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = Router.fromGeneratedRoutes(ControlPlaneHandlers.build(), TestContexts.factory(store));
    }

    private RestResponse put(String path, String body) {
        return router.dispatch("PUT", path, path, Map.of(), Map.of(), body.getBytes(StandardCharsets.UTF_8));
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------
    // shared: upsert, delete, exists
    // ------------------------------------------------------------------

    @Test
    void allThreeKindsRoundTrip() throws Exception {
        assertEquals(RestStatus.OK, put("/_template/lt", "{\"index_patterns\":[\"a-*\"]}").status());
        assertEquals(RestStatus.OK, put("/_index_template/it", "{\"index_patterns\":[\"b-*\"]}").status());
        assertEquals(RestStatus.OK, put("/_component_template/ct", "{\"template\":{}}").status());

        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/_template/lt").status());
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/_index_template/it").status());
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/_component_template/ct").status());

        for (String path : List.of("/_template/lt", "/_index_template/it", "/_component_template/ct")) {
            assertTrue(body(router.dispatch("DELETE", path)).get("acknowledged").asBoolean(), path);
            assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", path).status(), path);
        }
    }

    /** All three report a missing delete identically, including component templates. */
    @Test
    void deletingAMissingTemplateIsIndexTemplateMissingForEveryKind() throws Exception {
        for (String path : List.of("/_template/nope", "/_index_template/nope", "/_component_template/nope")) {
            RestResponse response = router.dispatch("DELETE", path);
            assertEquals(RestStatus.NOT_FOUND, response.status(), path);
            JsonNode error = body(response).get("error");
            assertEquals("index_template_missing_exception", error.get("type").asText(), path);
            assertEquals("index_template [nope] missing", error.get("reason").asText(), path);
        }
    }

    @Test
    void putIsAnUpsert() throws Exception {
        put("/_index_template/it", "{\"index_patterns\":[\"a-*\"]}");
        assertEquals(RestStatus.OK, put("/_index_template/it", "{\"index_patterns\":[\"b-*\"]}").status());

        JsonNode item = body(router.dispatch("GET", "/_index_template/it")).get("index_templates").get(0);
        assertEquals("b-*", item.get("index_template").get("index_patterns").get(0).asText());
    }

    // ------------------------------------------------------------------
    // the differences
    // ------------------------------------------------------------------

    /** Legacy answers a name-keyed map; the newer kinds answer an array under a wrapper key. */
    @Test
    void getShapeDiffersByKind() throws Exception {
        put("/_template/lt", "{\"index_patterns\":[\"a-*\"]}");
        put("/_index_template/it", "{\"index_patterns\":[\"b-*\"]}");
        put("/_component_template/ct", "{\"template\":{}}");

        JsonNode legacy = body(router.dispatch("GET", "/_template/lt"));
        assertEquals(List.of("lt"), legacy.propertyStream().map(Map.Entry::getKey).toList());
        assertEquals("a-*", legacy.get("lt").get("index_patterns").get(0).asText());

        JsonNode index = body(router.dispatch("GET", "/_index_template/it"));
        assertEquals("it", index.get("index_templates").get(0).get("name").asText());
        assertTrue(index.get("index_templates").get(0).has("index_template"));

        JsonNode component = body(router.dispatch("GET", "/_component_template/ct"));
        assertEquals("ct", component.get("component_templates").get(0).get("name").asText());
        assertTrue(component.get("component_templates").get(0).has("component_template"));
    }

    /** Legacy answers a bare {@code {}} with its 404; the newer kinds answer an error document. */
    @Test
    void getMissingDiffersByKind() throws Exception {
        RestResponse legacy = router.dispatch("GET", "/_template/nope");
        assertEquals(RestStatus.NOT_FOUND, legacy.status());
        assertEquals(0, body(legacy).size(), "bare empty object, as with GET /_ingest/pipeline");

        RestResponse index = router.dispatch("GET", "/_index_template/nope");
        assertEquals(RestStatus.NOT_FOUND, index.status());
        assertEquals("resource_not_found_exception", body(index).get("error").get("type").asText());
        assertEquals("index template matching [nope] not found", body(index).get("error").get("reason").asText());

        RestResponse component = router.dispatch("GET", "/_component_template/nope");
        assertEquals(
            "component template matching [nope] not found",
            body(component).get("error").get("reason").asText()
        );
    }

    /** The required property differs, and so does the error for omitting it. */
    @Test
    void requiredPropertyAndItsErrorDifferByKind() throws Exception {
        RestResponse legacy = put("/_template/lt", "{}");
        assertEquals(RestStatus.BAD_REQUEST, legacy.status());
        JsonNode legacyError = body(legacy).get("error");
        assertEquals("action_request_validation_exception", legacyError.get("type").asText());
        assertEquals("Validation Failed: 1: index patterns are missing;", legacyError.get("reason").asText());

        RestResponse index = put("/_index_template/it", "{\"template\":{}}");
        assertEquals(RestStatus.BAD_REQUEST, index.status());
        assertEquals("illegal_argument_exception", body(index).get("error").get("type").asText());
        assertEquals("Required [index_patterns]", body(index).get("error").get("reason").asText());

        // Component templates require `template`, not `index_patterns`.
        RestResponse component = put("/_component_template/ct", "{\"index_patterns\":[\"a-*\"]}");
        assertEquals(RestStatus.BAD_REQUEST, component.status());
        assertEquals("Required [template]", body(component).get("error").get("reason").asText());
    }

    // ------------------------------------------------------------------
    // name expressions
    // ------------------------------------------------------------------

    @Test
    void getSupportsWildcardsCommaListsAndListingEverything() throws Exception {
        put("/_index_template/a-one", "{\"index_patterns\":[\"x\"]}");
        put("/_index_template/a-two", "{\"index_patterns\":[\"y\"]}");
        put("/_index_template/b-one", "{\"index_patterns\":[\"z\"]}");

        assertEquals(2, body(router.dispatch("GET", "/_index_template/a-*")).get("index_templates").size());
        assertEquals(2, body(router.dispatch("GET", "/_index_template/a-one,b-one")).get("index_templates").size());
        assertEquals(3, body(router.dispatch("GET", "/_index_template")).get("index_templates").size());
    }

    @Test
    void listingEverythingIsNotA404WhenEmpty() throws Exception {
        RestResponse response = router.dispatch("GET", "/_index_template");

        assertEquals(RestStatus.OK, response.status(), "no name given, so nothing to miss");
        assertEquals(0, body(response).get("index_templates").size());
    }

    @Test
    void kindsDoNotShareAKeySpace() {
        put("/_template/same", "{\"index_patterns\":[\"a\"]}");
        put("/_index_template/same", "{\"index_patterns\":[\"a\"]}");
        put("/_component_template/same", "{\"template\":{}}");

        assertEquals(1, store.scanPrefix("tenant/default/templates/legacy/").size());
        assertEquals(1, store.scanPrefix("tenant/default/templates/index/").size());
        assertEquals(1, store.scanPrefix("tenant/default/templates/component/").size());
    }

    @Test
    void allTwelveGroupsAreRegistered() {
        HandlerRegistry registry = ControlPlaneHandlers.build();

        for (String group : List.of(
            "indices.put_template", "indices.get_template", "indices.delete_template", "indices.exists_template",
            "indices.put_index_template", "indices.get_index_template",
            "indices.delete_index_template", "indices.exists_index_template",
            "cluster.put_component_template", "cluster.get_component_template",
            "cluster.delete_component_template", "cluster.exists_component_template")) {
            assertTrue(registry.implemented().contains(group), group);
        }
    }
}
