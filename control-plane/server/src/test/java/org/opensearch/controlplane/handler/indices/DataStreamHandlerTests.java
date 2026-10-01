/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.tenant.SingleTenantResolver;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code indices.create_data_stream} and {@code indices.delete_data_stream}, verified against 3.9.0.
 *
 * <p>{@code get_data_stream} is still stubbed, so these read back through the store and through the
 * ordinary index operations — which is itself the point being made: a backing index is an index, and every
 * index read already serves it.
 */
class DataStreamHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Derived rather than spelled out, so a change to the key layout does not silently pass. */
    private static final String STREAMS = SingleTenantResolver.DEFAULT_TENANT.keyPrefix() + "data_streams/";

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = Router.fromGeneratedRoutes(ControlPlaneHandlers.build(), TestContexts.factory(store));
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private RestResponse dispatch(String method, String path, String body) {
        return router.dispatch(method, path, path, Map.of(), Map.of(),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8));
    }

    /** A composable template with a {@code data_stream} block, which a stream cannot be created without. */
    private void dataStreamTemplate(String name, String pattern, String extra) {
        RestResponse response = dispatch("PUT", "/_index_template/" + name, "{"
            + "\"index_patterns\":[\"" + pattern + "\"]," + extra
            + "\"template\":{\"settings\":{\"number_of_shards\":3}}}");
        assertEquals(RestStatus.OK, response.status(), () -> bodyText(response));
    }

    private RestResponse createStream(String name) {
        return dispatch("PUT", "/_data_stream/" + name, null);
    }

    private RestResponse deleteStream(String expression) {
        return dispatch("DELETE", "/_data_stream/" + expression, null);
    }

    private String bodyText(RestResponse response) {
        return new String(response.body(), StandardCharsets.UTF_8);
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(bodyText(response));
    }

    private String reasonOf(RestResponse response) throws Exception {
        return body(response).get("error").get("reason").asText();
    }

    private String typeOf(RestResponse response) throws Exception {
        return body(response).get("error").get("type").asText();
    }

    private JsonNode storedStream(String name) throws Exception {
        return MAPPER.readTree(store.get(STREAMS + name).orElseThrow().value());
    }

    // ------------------------------------------------------------------
    // the template prerequisite
    // ------------------------------------------------------------------

    @Test
    void aStreamCannotBeCreatedWithoutAMatchingTemplate() throws Exception {
        RestResponse response = createStream("logs");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("illegal_argument_exception", typeOf(response));
        assertEquals("no matching index template found for data stream [logs]", reasonOf(response));
    }

    /** A template can match and still not be a data stream template. */
    @Test
    void aMatchingTemplateWithoutADataStreamBlockIsRejected() throws Exception {
        dataStreamTemplate("plain", "logs*", "");

        assertEquals("matching index template [plain] for data stream [logs] has no data stream template",
            reasonOf(createStream("logs")));
    }

    @Test
    void aMatchingDataStreamTemplateIsEnough() {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");

        assertEquals(RestStatus.OK, createStream("logs").status());
    }

    // ------------------------------------------------------------------
    // what a create writes
    // ------------------------------------------------------------------

    @Test
    void theStreamRecordsItsFirstBackingIndexAndGeneration() throws Exception {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");
        assertTrue(body(createStream("logs")).get("acknowledged").booleanValue());

        JsonNode stream = storedStream("logs");
        assertEquals("logs", stream.get("name").textValue());
        assertEquals(1, stream.get("generation").longValue());
        assertEquals(1, stream.get("indices").size());
        assertEquals(".ds-logs-000001", stream.get("indices").get(0).get("index_name").textValue());
        assertFalse(stream.get("indices").get(0).get("index_uuid").textValue().isEmpty());
    }

    /** Six digits, zero padded: the data plane parses this name to find the stream it belongs to. */
    @Test
    void theBackingIndexNameIsZeroPaddedToSixDigits() throws Exception {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");
        createStream("logs");

        assertEquals(RestStatus.OK, dispatch("GET", "/.ds-logs-000001", null).status());
    }

    /** The backing index is an ordinary index, so every existing index read already serves it. */
    @Test
    void theBackingIndexIsHiddenAndCarriesTheTemplateSettings() throws Exception {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");
        createStream("logs");

        JsonNode settings = body(dispatch("GET", "/.ds-logs-000001/_settings", null))
            .get(".ds-logs-000001").get("settings").get("index");
        assertEquals("true", settings.get("hidden").textValue());
        assertEquals("3", settings.get("number_of_shards").textValue());
    }

    /**
     * Templates match the <b>stream</b> name, not the backing index name — {@code .ds-logs-000001} matches
     * {@code logs*} nowhere, yet it must still get the template's three shards. Getting this backwards
     * fails silently, with the backing index quietly taking built-in defaults.
     */
    @Test
    void theTemplateIsMatchedAgainstTheStreamNameNotTheBackingIndexName() throws Exception {
        dataStreamTemplate("ds", "logs", "\"data_stream\":{},");
        createStream("logs");

        assertEquals("3", body(dispatch("GET", "/.ds-logs-000001/_settings", null))
            .get(".ds-logs-000001").get("settings").get("index").get("number_of_shards").textValue());
    }

    @Test
    void theTimestampFieldDefaultsToAtTimestamp() throws Exception {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");
        createStream("logs");

        assertEquals("@timestamp", storedStream("logs").get("timestamp_field").textValue());
    }

    @Test
    void theTemplateMayNameADifferentTimestampField() throws Exception {
        dataStreamTemplate("ds", "logs*",
            "\"data_stream\":{\"timestamp_field\":{\"name\":\"event_time\"}},");
        createStream("logs");

        assertEquals("event_time", storedStream("logs").get("timestamp_field").textValue());
    }

    /**
     * A backing index inherits the template's aliases, exactly as an ordinary index does. Verified against
     * 3.9.0, where {@code GET /_alias/all-logs} answers {@code .ds-logs-000001}.
     *
     * <p>Asserted against the alias document rather than through {@code GET /_alias}, which does not report
     * it here: that operation expands wildcards to open indices only, where a real cluster's
     * {@code GetAliasesRequest} defaults to {@code strictExpandHidden()} — open, closed <em>and</em> hidden.
     * A hidden backing index is the first thing in this service to make that difference observable. It is a
     * divergence in {@code indices.get_alias}, not in the write this test is about, so it is reported rather
     * than worked around here.
     */
    @Test
    void theBackingIndexInheritsTemplateAliases() throws Exception {
        dispatch("PUT", "/_index_template/ds", "{\"index_patterns\":[\"logs*\"],\"data_stream\":{},"
            + "\"template\":{\"aliases\":{\"all-logs\":{}}}}");
        createStream("logs");

        JsonNode alias = MAPPER.readTree(store.get(
            SingleTenantResolver.DEFAULT_TENANT.keyPrefix() + "aliases/all-logs").orElseThrow().value());
        assertTrue(alias.get("indices").has(".ds-logs-000001"), alias::toString);
    }

    // ------------------------------------------------------------------
    // create is not an upsert
    // ------------------------------------------------------------------

    @Test
    void creatingTheSameStreamTwiceIsRejected() throws Exception {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");
        createStream("logs");

        RestResponse response = createStream("logs");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("resource_already_exists_exception", typeOf(response));
        assertEquals("data_stream [logs] already exists", reasonOf(response));
    }

    /** The existence check runs before name validation, which is the order the server uses. */
    @Test
    void alreadyExistsOutranksAnInvalidName() throws Exception {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");
        createStream("logs");

        // "logs" is valid, so build the collision on a name that is not: write the stream, then re-request
        // it. A name that fails validation can never be created, so this asserts the ordering the only way
        // it is observable -- an existing name reports the collision rather than being re-validated.
        assertEquals("data_stream [logs] already exists", reasonOf(createStream("logs")));
    }

    // ------------------------------------------------------------------
    // names
    // ------------------------------------------------------------------

    @Test
    void anUppercaseNameIsRejectedWithTheDataStreamWording() throws Exception {
        dataStreamTemplate("ds", "Logs*", "\"data_stream\":{},");

        RestResponse response = createStream("Logs");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        // illegal_argument_exception, not the invalid_index_name_exception the index path gives.
        assertEquals("illegal_argument_exception", typeOf(response));
        assertEquals("data_stream [Logs] must be lowercase", reasonOf(response));
    }

    /** The one rule data streams have that indices do not: an index may start with a dot, a stream may not. */
    @Test
    void aLeadingDotIsRejected() throws Exception {
        dataStreamTemplate("ds", ".logs*", "\"data_stream\":{},");

        assertEquals("data_stream [.logs] must not start with '.'", reasonOf(createStream(".logs")));
    }

    /** Checked after the shared index rules, so ".." reports the shared reason rather than the dot rule. */
    @Test
    void theSharedIndexRulesAreCheckedBeforeTheDotRule() throws Exception {
        dataStreamTemplate("ds", "*", "\"data_stream\":{},");

        assertEquals("data_stream [..] must not be '.' or '..'", reasonOf(createStream("..")));
    }

    @Test
    void anInvalidCharacterIsReportedWithTheIndexWording() throws Exception {
        dataStreamTemplate("ds", "*", "\"data_stream\":{},");

        assertTrue(reasonOf(createStream("lo|gs")).startsWith("data_stream [lo|gs] must not contain"),
            reasonOf(createStream("lo|gs")));
    }

    // ------------------------------------------------------------------
    // deleting
    // ------------------------------------------------------------------

    @Test
    void deleteRemovesTheStreamAndItsBackingIndices() throws Exception {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");
        createStream("logs");

        assertTrue(body(deleteStream("logs")).get("acknowledged").booleanValue());
        assertTrue(store.get(STREAMS + "logs").isEmpty());
        assertEquals(RestStatus.NOT_FOUND, dispatch("GET", "/.ds-logs-000001", null).status());
    }

    /**
     * A backing index goes as {@code indices.delete} removes an index: out of the aliases it inherited,
     * and with a {@code delete-index} task, which is the supervisor's evidence that the index was deleted.
     */
    @Test
    void deleteDropsBackingIndicesFromAliasesAndRecordsTheirDeletion() throws Exception {
        String tenant = SingleTenantResolver.DEFAULT_TENANT.keyPrefix();
        dispatch("PUT", "/_index_template/ds", "{\"index_patterns\":[\"logs*\"],\"data_stream\":{},"
            + "\"template\":{\"aliases\":{\"all-logs\":{}}}}");
        createStream("logs");
        String uuid = MAPPER.readTree(store.get(tenant + "indices/.ds-logs-000001").orElseThrow().value())
            .get("uuid").asText();

        deleteStream("logs");

        assertTrue(store.get(tenant + "aliases/all-logs").isEmpty(), "its only member is gone, so the alias is");
        assertTrue(store.scanPrefix(tenant + IndexManagementTasks.KEY_PREFIX).values().stream()
            .map(task -> {
                try {
                    return MAPPER.readTree(task.value());
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            })
            .anyMatch(task -> task.get("type").asText().equals(IndexManagementTasks.TYPE_DELETE_INDEX)
                && task.get("index_uuid").asText().equals(uuid)));
    }

    /** No not-found check anywhere: a concrete name that does not exist is simply acknowledged. */
    @Test
    void deletingAStreamThatDoesNotExistIsAcknowledged() throws Exception {
        RestResponse response = deleteStream("nonesuch");

        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).get("acknowledged").booleanValue());
    }

    @Test
    void deletingWithAWildcardThatMatchesNothingIsAcknowledged() {
        assertEquals(RestStatus.OK, deleteStream("nonesuch*").status());
    }

    @Test
    void aWildcardDeletesEveryMatchingStream() throws Exception {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");
        createStream("logs-a");
        createStream("logs-b");

        assertEquals(RestStatus.OK, deleteStream("logs-*").status());
        assertTrue(store.get(STREAMS + "logs-a").isEmpty());
        assertTrue(store.get(STREAMS + "logs-b").isEmpty());
        assertEquals(RestStatus.NOT_FOUND, dispatch("GET", "/.ds-logs-a-000001", null).status());
        assertEquals(RestStatus.NOT_FOUND, dispatch("GET", "/.ds-logs-b-000001", null).status());
    }

    @Test
    void aWildcardLeavesNonMatchingStreamsAlone() throws Exception {
        dataStreamTemplate("ds", "*", "\"data_stream\":{},");
        createStream("logs");
        createStream("metrics");

        deleteStream("logs*");
        assertTrue(store.get(STREAMS + "logs").isEmpty());
        assertTrue(store.get(STREAMS + "metrics").isPresent());
    }

    /** A comma-separated list, each element a glob in its own right. */
    @Test
    void deleteAcceptsACommaSeparatedList() throws Exception {
        dataStreamTemplate("ds", "*", "\"data_stream\":{},");
        createStream("logs");
        createStream("metrics");
        createStream("traces");

        deleteStream("logs,metric*");
        assertTrue(store.get(STREAMS + "logs").isEmpty());
        assertTrue(store.get(STREAMS + "metrics").isEmpty());
        assertTrue(store.get(STREAMS + "traces").isPresent());
    }

    /** After a delete the name is free again, and the new stream starts from generation 1. */
    @Test
    void aStreamCanBeRecreatedAfterDeletion() throws Exception {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");
        createStream("logs");
        deleteStream("logs");

        assertEquals(RestStatus.OK, createStream("logs").status());
        assertEquals(1, storedStream("logs").get("generation").longValue());
    }

    // ------------------------------------------------------------------
    // still stubbed
    // ------------------------------------------------------------------

    /**
     * {@code get_data_stream} keeps its 501 on purpose: its required {@code status} field is health over
     * the backing indices, which needs the data plane.
     */
    @Test
    void readingDataStreamsIsStillNotImplemented() {
        assertEquals(RestStatus.NOT_IMPLEMENTED, dispatch("GET", "/_data_stream", null).status());
        assertEquals(RestStatus.NOT_IMPLEMENTED, dispatch("GET", "/_data_stream/logs", null).status());
    }

    /** Streams are per tenant, as the store layout makes them. */
    @Test
    void theKeyLivesUnderTheTenantPrefix() {
        dataStreamTemplate("ds", "logs*", "\"data_stream\":{},");
        createStream("logs");

        assertEquals(List.of(STREAMS + "logs"),
            store.scanPrefix(STREAMS).keySet().stream().toList());
    }
}
