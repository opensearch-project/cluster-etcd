/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.ingest;

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
import org.opensearch.controlplane.tenant.SingleTenantResolver;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the pipeline handlers through the real router, so path templates, placeholder binding, and
 * error shapes are exercised rather than the handler being called directly.
 *
 * <p>Every status code and error body asserted here was verified against OpenSearch 3.9.0-SNAPSHOT.
 */
class IngestPipelineHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Handlers write through a tenant-scoped view, so the root store sees every key under this
     * prefix. Assertions here deliberately use the full key to show the scoping is real.
     */
    private static final String TENANT_PREFIX = SingleTenantResolver.DEFAULT_TENANT.keyPrefix();

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = Router.fromGeneratedRoutes(ControlPlaneHandlers.registerAll(HandlerRegistry.withAllStubbed()), TestContexts.factory(store));
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    /** A minimal valid definition. {@code processors} is required by the server. */
    private static String definition(String description) {
        return "{\"description\":\"" + description + "\",\"processors\":[]}";
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private RestResponse put(String id, String definition) {
        String path = "/_ingest/pipeline/" + id;
        return router.dispatch("PUT", path, path, Map.of(), Map.of(), definition.getBytes(StandardCharsets.UTF_8));
    }

    private RestResponse put(String id) {
        return put(id, definition(id));
    }

    private RestResponse get(String path) {
        return router.dispatch("GET", path);
    }

    private RestResponse delete(String path) {
        return router.dispatch("DELETE", path);
    }

    /**
     * Stored pipelines only. {@code store.size()} would also count the ingest-metadata marker, which
     * is bookkeeping rather than a pipeline.
     */
    private int pipelineCount() {
        return store.scanPrefix(TENANT_PREFIX + PipelineStore.KEY_PREFIX).size();
    }

    // ------------------------------------------------------------------
    // put
    // ------------------------------------------------------------------

    @Test
    void putStoresThePipelineAndAcknowledges() throws Exception {
        RestResponse response = put("my-pipeline");

        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).get("acknowledged").asBoolean());
        assertEquals(1, pipelineCount());
        assertTrue(store.get(TENANT_PREFIX + "ingest/pipeline/my-pipeline").isPresent(), "stored under ingest/pipeline/<id>");
    }

    @Test
    void putIsAnUpsert() throws Exception {
        put("my-pipeline", definition("first"));
        RestResponse response = put("my-pipeline", definition("second"));

        assertEquals(RestStatus.OK, response.status());
        assertEquals(1, pipelineCount());
        assertEquals(
            "second",
            body(get("/_ingest/pipeline/my-pipeline")).get("my-pipeline").get("description").asText()
        );
    }

    @Test
    void putRejectsAMissingBody() throws Exception {
        RestResponse response = router.dispatch("PUT", "/_ingest/pipeline/my-pipeline");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("parse_exception", error.get("type").asText());
        assertEquals("request body or source parameter is required", error.get("reason").asText());
        assertEquals(0, pipelineCount());
    }

    @Test
    void putRejectsMalformedJson() throws Exception {
        RestResponse response = put("my-pipeline", "{not json");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("parse_exception", body(response).get("error").get("type").asText());
        assertEquals(0, pipelineCount());
    }

    @Test
    void putRejectsANonObjectBody() throws Exception {
        RestResponse response = put("my-pipeline", "[1,2,3]");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("parse_exception", body(response).get("error").get("type").asText());
        assertEquals(0, pipelineCount());
    }

    /**
     * The server requires {@code processors} even though the spec's {@code Pipeline} schema does not
     * mark it required. Server behaviour wins.
     */
    @Test
    void putRejectsABodyWithoutProcessors() throws Exception {
        RestResponse response = put("my-pipeline", "{\"description\":\"no processors\"}");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("parse_exception", error.get("type").asText());
        assertEquals("[processors] required property is missing", error.get("reason").asText());
        assertEquals("processors", error.get("property_name").asText());
        // The extra field is repeated inside root_cause, as the server does.
        assertEquals("processors", error.get("root_cause").get(0).get("property_name").asText());
        assertEquals(0, pipelineCount());
    }

    @Test
    void putRejectsAnEmptyObject() throws Exception {
        RestResponse response = put("my-pipeline", "{}");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("processors", body(response).get("error").get("property_name").asText());
    }

    @Test
    void putNormalisesStoredJsonSoOutputIsIndependentOfRequestFormatting() {
        put("a", "{\n  \"description\" : \"spaced out\",\n  \"processors\" : [ ]\n}");

        assertEquals(
            "{\"description\":\"spaced out\",\"processors\":[]}",
            store.getAsString(TENANT_PREFIX + "ingest/pipeline/a").orElseThrow()
        );
    }

    // ------------------------------------------------------------------
    // get
    // ------------------------------------------------------------------

    @Test
    void getReturnsABareMapWithNoEnvelope() throws Exception {
        put("a", definition("A"));

        JsonNode json = body(get("/_ingest/pipeline/a"));
        assertEquals(List.of("a"), json.propertyStream().map(Map.Entry::getKey).toList());
        assertEquals("A", json.get("a").get("description").asText());
    }

    @Test
    void getWithoutAnIdReturnsEverything() throws Exception {
        put("a");
        put("b");

        RestResponse response = get("/_ingest/pipeline");
        assertEquals(RestStatus.OK, response.status());
        JsonNode json = body(response);
        assertEquals(2, json.size());
        assertTrue(json.has("a") && json.has("b"));
    }

    @Test
    void getSupportsCommaSeparatedIds() throws Exception {
        put("a");
        put("b");
        put("c");

        JsonNode json = body(get("/_ingest/pipeline/a,c"));
        assertEquals(2, json.size());
        assertTrue(json.has("a") && json.has("c"));
        assertFalse(json.has("b"));
    }

    @Test
    void getSupportsWildcards() throws Exception {
        put("logs-a");
        put("logs-b");
        put("metrics");

        JsonNode json = body(get("/_ingest/pipeline/logs-*"));
        assertEquals(2, json.size());
        assertTrue(json.has("logs-a") && json.has("logs-b"));
        assertFalse(json.has("metrics"));
    }

    @Test
    void getIgnoresMissingIdsInAList() throws Exception {
        put("a");

        JsonNode json = body(get("/_ingest/pipeline/a,nonexistent"));
        assertEquals(1, json.size());
        assertTrue(json.has("a"));
    }

    /** Verified: 404 with a bare {@code {}}, even though the spec declares only a 200. */
    @Test
    void getReturns404WithAnEmptyObjectWhenNothingMatches() throws Exception {
        put("a");

        RestResponse response = get("/_ingest/pipeline/nonexistent");
        assertEquals(RestStatus.NOT_FOUND, response.status());
        JsonNode json = body(response);
        assertTrue(json.isObject());
        assertEquals(0, json.size());
    }

    /** Verified: even a bare GET on a cluster with no pipelines is a 404. */
    @Test
    void getReturns404WhenNoPipelinesExistAtAll() {
        assertEquals(RestStatus.NOT_FOUND, get("/_ingest/pipeline").status());
        assertEquals(RestStatus.NOT_FOUND, get("/_ingest/pipeline/*").status());
    }

    // ------------------------------------------------------------------
    // delete -- three verified branches, see DeletePipelineHandler
    // ------------------------------------------------------------------

    @Test
    void deleteRemovesThePipelineAndAcknowledges() throws Exception {
        put("a");

        RestResponse response = delete("/_ingest/pipeline/a");
        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).get("acknowledged").asBoolean());
        assertEquals(0, pipelineCount());
    }

    /** Branch 1: no ingest metadata has ever existed, so any delete is a no-op 200. */
    @Test
    void deleteOnAPristineStoreIsAcknowledgedEvenForAMissingId() throws Exception {
        RestResponse response = delete("/_ingest/pipeline/never-existed");

        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).get("acknowledged").asBoolean());
    }

    /**
     * Branch 3: once any pipeline has ever existed, the same call is a 404 — even with zero
     * pipelines currently stored. The trigger is the presence of ingest metadata, not the count.
     */
    @Test
    void deleteOfAMissingIdIs404OnceAnyPipelineHasEverExisted() throws Exception {
        put("a");
        delete("/_ingest/pipeline/a");
        assertEquals(0, pipelineCount(), "no pipelines remain, but ingest metadata does");

        RestResponse response = delete("/_ingest/pipeline/never-existed");
        assertEquals(RestStatus.NOT_FOUND, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("resource_not_found_exception", error.get("type").asText());
        assertEquals("pipeline [never-existed] is missing", error.get("reason").asText());
    }

    @Test
    void deleteOfAConcreteMissingIdIs404WhenOthersExist() throws Exception {
        put("a");

        RestResponse response = delete("/_ingest/pipeline/nonexistent");
        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals("pipeline [nonexistent] is missing", body(response).get("error").get("reason").asText());
        assertEquals(1, pipelineCount(), "the failed delete must not have removed anything");
    }

    /** Branch 3 again: a partial wildcard is not exempt from the not-found check. */
    @Test
    void deleteByAPartialWildcardMatchingNothingIs404() throws Exception {
        put("a");

        RestResponse response = delete("/_ingest/pipeline/logs-*");
        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals("pipeline [logs-*] is missing", body(response).get("error").get("reason").asText());
    }

    /** Branch 2: only the exact match-all pattern is exempt. */
    @Test
    void deleteByMatchAllIsAcknowledgedEvenWhenNothingMatches() throws Exception {
        put("a");
        delete("/_ingest/pipeline/a");

        RestResponse response = delete("/_ingest/pipeline/*");
        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).get("acknowledged").asBoolean());
    }

    @Test
    void deleteByWildcardRemovesEveryMatch() {
        put("logs-a");
        put("logs-b");
        put("metrics");

        assertEquals(RestStatus.OK, delete("/_ingest/pipeline/logs-*").status());
        assertEquals(1, pipelineCount());
        assertTrue(store.get(TENANT_PREFIX + "ingest/pipeline/metrics").isPresent());
    }

    @Test
    void deleteAllRemovesEverything() {
        put("a");
        put("b");

        assertEquals(RestStatus.OK, delete("/_ingest/pipeline/*").status());
        assertEquals(0, pipelineCount());
    }

    // ------------------------------------------------------------------
    // registration and cross-cutting concerns
    // ------------------------------------------------------------------

    @Test
    void allThreeOperationsAreRegisteredAndTheRestStayStubbed() {
        HandlerRegistry registry = ControlPlaneHandlers.registerAll(HandlerRegistry.withAllStubbed());

        assertTrue(
            registry.implemented().containsAll(
                Set.of("ingest.put_pipeline", "ingest.get_pipeline", "ingest.delete_pipeline")
            )
        );
        // ingest.simulate is out of scope entirely; processor_grok is in scope but unimplemented.
        assertTrue(registry.notImplemented().contains("ingest.processor_grok"));
        assertFalse(registry.handlers().containsKey("ingest.simulate"));
    }

    @Test
    void prettyPrintingAppliesToPipelineResponses() {
        put("a");
        RestResponse pretty = router.dispatch(
            "GET", "/_ingest/pipeline/a", "/_ingest/pipeline/a?pretty",
            Map.of("pretty", List.of("")), Map.of(), new byte[0]
        );

        assertTrue(new String(pretty.body(), StandardCharsets.UTF_8).contains("\n"));
    }
}
