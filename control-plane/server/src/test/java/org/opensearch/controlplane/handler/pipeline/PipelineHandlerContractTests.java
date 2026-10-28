/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.pipeline;

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
 * The pipeline API contract, run against both {@link PipelineKind}s.
 *
 * <p>Drives the real router, so path templates, placeholder binding, and error shapes are exercised
 * rather than handlers being called directly. Every status code and error body asserted here was
 * verified against OpenSearch 3.9.0 for <em>both</em> ingest and search pipelines — they behave
 * identically, which is why one set of assertions covers both.
 *
 * <p>The single divergence is expressed by {@link #requiredProperty()}: ingest insists on
 * {@code processors}, a search pipeline accepts {@code {}}.
 */
abstract class PipelineHandlerContractTests {

    /** The URL prefix under test, e.g. {@code /_ingest/pipeline}. */
    protected abstract String urlPrefix();

    /** The kind under test, for key-layout assertions. */
    protected abstract PipelineKind kind();

    /** A body property the server requires, or {@code null} if none. */
    protected abstract String requiredProperty();

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
    private String definition(String description) {
        String required = requiredProperty() == null ? "" : ",\"" + requiredProperty() + "\":[]";
        return "{\"description\":\"" + description + "\"" + required + "}";
    }

    /** The minimal body this kind accepts. */
    private String minimalDefinition() {
        return requiredProperty() == null ? "{}" : "{\"" + requiredProperty() + "\":[]}";
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private RestResponse put(String id, String definition) {
        String path = urlPrefix() + "/" + id;
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
        return store.scanPrefix(TENANT_PREFIX + kind().keyPrefix()).size();
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
        assertTrue(store.get(TENANT_PREFIX + kind().keyPrefix() + "my-pipeline").isPresent(), "stored under <prefix>/<id>");
    }

    @Test
    void putIsAnUpsert() throws Exception {
        put("my-pipeline", definition("first"));
        RestResponse response = put("my-pipeline", definition("second"));

        assertEquals(RestStatus.OK, response.status());
        assertEquals(1, pipelineCount());
        assertEquals(
            "second",
            body(get(urlPrefix() + "/my-pipeline")).get("my-pipeline").get("description").asText()
        );
    }

    @Test
    void putRejectsAMissingBody() throws Exception {
        RestResponse response = router.dispatch("PUT", urlPrefix() + "/my-pipeline");

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
     * The one place the two kinds differ. Ingest requires {@code processors} even though the spec's
     * {@code Pipeline} schema does not mark it required; a search pipeline has three independently
     * optional processor lists and so accepts {@code {}}. Both verified against a live cluster.
     */
    @Test
    void enforcesTheRequiredPropertyIfThisKindHasOne() throws Exception {
        RestResponse response = put("my-pipeline", "{\"description\":\"nothing else\"}");

        if (requiredProperty() == null) {
            assertEquals(RestStatus.OK, response.status(), "this kind requires nothing");
            assertEquals(1, pipelineCount());
            return;
        }

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("parse_exception", error.get("type").asText());
        assertEquals("[" + requiredProperty() + "] required property is missing", error.get("reason").asText());
        assertEquals(requiredProperty(), error.get("property_name").asText());
        // The extra field is repeated inside root_cause, as the server does.
        assertEquals(requiredProperty(), error.get("root_cause").get(0).get("property_name").asText());
        assertEquals(0, pipelineCount());
    }

    @Test
    void handlesAnEmptyObjectAccordingToKind() throws Exception {
        RestResponse response = put("my-pipeline", "{}");

        if (requiredProperty() == null) {
            assertEquals(RestStatus.OK, response.status());
        } else {
            assertEquals(RestStatus.BAD_REQUEST, response.status());
            assertEquals(requiredProperty(), body(response).get("error").get("property_name").asText());
        }
    }

    @Test
    void putNormalisesStoredJsonSoOutputIsIndependentOfRequestFormatting() {
        put("a", "{\n  \"description\" : \"spaced out\"\n}".replace("\"spaced out\"",
            requiredProperty() == null ? "\"spaced out\"" : "\"spaced out\",\n  \"" + requiredProperty() + "\" : [ ]"));

        assertEquals(
            definition("spaced out"),
            store.getAsString(TENANT_PREFIX + kind().keyPrefix() + "a").orElseThrow(),
            "stored form must be canonical, not however the client indented it"
        );
    }

    // ------------------------------------------------------------------
    // get
    // ------------------------------------------------------------------

    @Test
    void getReturnsABareMapWithNoEnvelope() throws Exception {
        put("a", definition("A"));

        JsonNode json = body(get(urlPrefix() + "/a"));
        assertEquals(List.of("a"), json.propertyStream().map(Map.Entry::getKey).toList());
        assertEquals("A", json.get("a").get("description").asText());
    }

    @Test
    void getWithoutAnIdReturnsEverything() throws Exception {
        put("a");
        put("b");

        RestResponse response = get(urlPrefix());
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

        JsonNode json = body(get(urlPrefix() + "/a,c"));
        assertEquals(2, json.size());
        assertTrue(json.has("a") && json.has("c"));
        assertFalse(json.has("b"));
    }

    @Test
    void getSupportsWildcards() throws Exception {
        put("logs-a");
        put("logs-b");
        put("metrics");

        JsonNode json = body(get(urlPrefix() + "/logs-*"));
        assertEquals(2, json.size());
        assertTrue(json.has("logs-a") && json.has("logs-b"));
        assertFalse(json.has("metrics"));
    }

    @Test
    void getIgnoresMissingIdsInAList() throws Exception {
        put("a");

        JsonNode json = body(get(urlPrefix() + "/a,nonexistent"));
        assertEquals(1, json.size());
        assertTrue(json.has("a"));
    }

    /** Verified: 404 with a bare {@code {}}, even though the spec declares only a 200. */
    @Test
    void getReturns404WithAnEmptyObjectWhenNothingMatches() throws Exception {
        put("a");

        RestResponse response = get(urlPrefix() + "/nonexistent");
        assertEquals(RestStatus.NOT_FOUND, response.status());
        JsonNode json = body(response);
        assertTrue(json.isObject());
        assertEquals(0, json.size());
    }

    /** Verified: even a bare GET on a cluster with no pipelines is a 404. */
    @Test
    void getReturns404WhenNoPipelinesExistAtAll() {
        assertEquals(RestStatus.NOT_FOUND, get(urlPrefix()).status());
        assertEquals(RestStatus.NOT_FOUND, get(urlPrefix() + "/*").status());
    }

    // ------------------------------------------------------------------
    // delete -- three verified branches, see DeletePipelineHandler
    // ------------------------------------------------------------------

    @Test
    void deleteRemovesThePipelineAndAcknowledges() throws Exception {
        put("a");

        RestResponse response = delete(urlPrefix() + "/a");
        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).get("acknowledged").asBoolean());
        assertEquals(0, pipelineCount());
    }

    /** Branch 1: no ingest metadata has ever existed, so any delete is a no-op 200. */
    @Test
    void deleteOnAPristineStoreIsAcknowledgedEvenForAMissingId() throws Exception {
        RestResponse response = delete(urlPrefix() + "/never-existed");

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
        delete(urlPrefix() + "/a");
        assertEquals(0, pipelineCount(), "no pipelines remain, but ingest metadata does");

        RestResponse response = delete(urlPrefix() + "/never-existed");
        assertEquals(RestStatus.NOT_FOUND, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("resource_not_found_exception", error.get("type").asText());
        assertEquals("pipeline [never-existed] is missing", error.get("reason").asText());
    }

    @Test
    void deleteOfAConcreteMissingIdIs404WhenOthersExist() throws Exception {
        put("a");

        RestResponse response = delete(urlPrefix() + "/nonexistent");
        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals("pipeline [nonexistent] is missing", body(response).get("error").get("reason").asText());
        assertEquals(1, pipelineCount(), "the failed delete must not have removed anything");
    }

    /** Branch 3 again: a partial wildcard is not exempt from the not-found check. */
    @Test
    void deleteByAPartialWildcardMatchingNothingIs404() throws Exception {
        put("a");

        RestResponse response = delete(urlPrefix() + "/logs-*");
        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals("pipeline [logs-*] is missing", body(response).get("error").get("reason").asText());
    }

    /** Branch 2: only the exact match-all pattern is exempt. */
    @Test
    void deleteByMatchAllIsAcknowledgedEvenWhenNothingMatches() throws Exception {
        put("a");
        delete(urlPrefix() + "/a");

        RestResponse response = delete(urlPrefix() + "/*");
        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).get("acknowledged").asBoolean());
    }

    @Test
    void deleteByWildcardRemovesEveryMatch() {
        put("logs-a");
        put("logs-b");
        put("metrics");

        assertEquals(RestStatus.OK, delete(urlPrefix() + "/logs-*").status());
        assertEquals(1, pipelineCount());
        assertTrue(store.get(TENANT_PREFIX + kind().keyPrefix() + "metrics").isPresent());
    }

    @Test
    void deleteAllRemovesEverything() {
        put("a");
        put("b");

        assertEquals(RestStatus.OK, delete(urlPrefix() + "/*").status());
        assertEquals(0, pipelineCount());
    }

    // ------------------------------------------------------------------
    // registration and cross-cutting concerns
    // ------------------------------------------------------------------

    @Test
    void allThreeOperationsForThisKindAreRegistered() {
        HandlerRegistry registry = ControlPlaneHandlers.registerAll(HandlerRegistry.withAllStubbed());

        assertTrue(registry.implemented().containsAll(
            Set.of(kind().putGroup(), kind().getGroup(), kind().deleteGroup())));
        // ingest.simulate is out of scope entirely; processor_grok is in scope but unimplemented.
        assertTrue(registry.notImplemented().contains("ingest.processor_grok"));
        assertFalse(registry.handlers().containsKey("ingest.simulate"));
    }

    // ------------------------------------------------------------------
    // processor type validation
    // ------------------------------------------------------------------

    /**
     * A body naming one processor in {@code field}. The required property, if this kind has one, is
     * always present so that a rejection can only be about the processor type.
     *
     * <p>Field names are taken in sorted order rather than from the map's iteration order, which for
     * {@code Map.of} is unspecified.
     */
    private String definitionWithProcessor(String field, String type) {
        StringBuilder body = new StringBuilder("{");
        if (requiredProperty() != null && requiredProperty().equals(field) == false) {
            body.append("\"").append(requiredProperty()).append("\":[],");
        }
        body.append("\"").append(field).append("\":[{\"").append(type).append("\":{}}]}");
        return body.toString();
    }

    /** Processor-holding fields in a deterministic order. */
    private List<Map.Entry<String, org.opensearch.controlplane.capability.ProcessorKind>> processorFields() {
        return kind().processorFields().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .toList();
    }

    private Router routerWith(org.opensearch.controlplane.capability.DataPlaneCapabilities capabilities) {
        return Router.fromGeneratedRoutes(
            ControlPlaneHandlers.registerAll(
                HandlerRegistry.withAllStubbed(),
                org.opensearch.controlplane.handler.core.ClusterIdentity.defaults(),
                capabilities
            ),
            TestContexts.factory(store)
        );
    }

    /** The default: nothing declared, so a processor the data plane lacks is still accepted. */
    @Test
    void permissiveCapabilitiesAcceptAnUnknownProcessor() {
        String field = processorFields().getFirst().getKey();
        RestResponse response = put("p", definitionWithProcessor(field, "definitely-not-a-processor"));

        assertEquals(RestStatus.OK, response.status());
    }

    @Test
    void declaredCapabilitiesRejectAnUnsupportedProcessor() throws Exception {
        var field = processorFields().getFirst();
        // Declare something, but not the type the request names.
        router = routerWith(new org.opensearch.controlplane.capability.DeclaredCapabilities(
            Map.of(field.getValue(), Set.of("a-supported-one"))));

        RestResponse response = put("p", definitionWithProcessor(field.getKey(), "unsupported-one"));
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");

        // The two kinds report this differently; both wordings verified against a live cluster.
        switch (kind().errorStyle()) {
            case INGEST -> {
                assertEquals("parse_exception", error.get("type").asText());
                assertEquals("No processor type exists with name [unsupported-one]", error.get("reason").asText());
                assertEquals("unsupported-one", error.get("processor_type").asText());
            }
            case SEARCH -> {
                assertEquals("illegal_argument_exception", error.get("type").asText());
                assertEquals("Invalid processor type unsupported-one", error.get("reason").asText());
            }
        }
        assertEquals(0, pipelineCount(), "a rejected definition must not be stored");
    }

    @Test
    void declaredCapabilitiesAcceptASupportedProcessor() {
        var field = processorFields().getFirst();
        router = routerWith(new org.opensearch.controlplane.capability.DeclaredCapabilities(
            Map.of(field.getValue(), Set.of("supported-one"))));

        assertEquals(RestStatus.OK, put("p", definitionWithProcessor(field.getKey(), "supported-one")).status());
        assertEquals(1, pipelineCount());
    }

    /** Kinds are separate namespaces, so declaring a type for one must not license it for another. */
    @Test
    void supportForOneProcessorKindDoesNotLicenseAnother() {
        var fields = processorFields();
        if (fields.size() < 2) {
            return;
        }
        router = routerWith(new org.opensearch.controlplane.capability.DeclaredCapabilities(
            Map.of(fields.get(0).getValue(), Set.of("shared-name"))));

        // Only meaningful when the two fields hold genuinely different kinds.
        if (fields.get(0).getValue() != fields.get(1).getValue()) {
            assertEquals(RestStatus.BAD_REQUEST,
                put("p", definitionWithProcessor(fields.get(1).getKey(), "shared-name")).status());
        }
    }

    @Test
    void theTwoKindsDoNotShareAKeySpace() {
        put("shared", minimalDefinition());

        PipelineKind other = kind().keyPrefix().startsWith("ingest")
            ? PipelineKind.search()
            : PipelineKind.ingest();
        assertEquals(0, store.scanPrefix(TENANT_PREFIX + other.keyPrefix()).size(),
            "an ingest pipeline must not appear as a search pipeline, or vice versa");
    }

    @Test
    void prettyPrintingAppliesToPipelineResponses() {
        put("a");
        RestResponse pretty = router.dispatch(
            "GET", urlPrefix() + "/a", urlPrefix() + "/a?pretty",
            Map.of("pretty", List.of("")), Map.of(), new byte[0]
        );

        assertTrue(new String(pretty.body(), StandardCharsets.UTF_8).contains("\n"));
    }
}
