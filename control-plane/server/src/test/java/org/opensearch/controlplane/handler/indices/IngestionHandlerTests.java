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
 * Pausing and resuming pull-based ingestion.
 *
 * <p>Shapes and wording are transcribed from OpenSearch's sources rather than diffed against a live cluster:
 * exercising these upstream needs an index with a configured streaming source. What these tests pin down is
 * the part this service is responsible for — which field is written, where, and what reaches the supervisor.
 */
class IngestionHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TENANT = SingleTenantResolver.DEFAULT_TENANT.keyPrefix();

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = Router.fromGeneratedRoutes(ControlPlaneHandlers.build(), TestContexts.factory(store));
    }

    private RestResponse dispatch(String method, String path, String body) {
        return router.dispatch(method, path, path, Map.of(), Map.of(),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8));
    }

    private void createIndex() {
        assertEquals(RestStatus.OK, dispatch("PUT", "/my-index", null).status());
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private JsonNode storedIndex() throws Exception {
        return MAPPER.readTree(store.get(TENANT + "indices/my-index").orElseThrow().value());
    }

    private List<JsonNode> tasks() {
        return store.scanPrefix(TENANT + "tasks/index-management/").values().stream()
            .map(versioned -> {
                try {
                    return (JsonNode) MAPPER.readTree(versioned.value());
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            })
            .toList();
    }

    private JsonNode ingestionTask() {
        return tasks().stream()
            .filter(task -> task.get("type").asText().endsWith("-ingestion"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no ingestion task was submitted: " + tasks()));
    }

    // ------------------------------------------------------------------
    // where the flag goes
    // ------------------------------------------------------------------

    /**
     * A first-class field on the index document, not a setting. The obvious guess is
     * {@code index.ingestion.paused} in the settings block, where a supervisor would find nothing.
     */
    @Test
    void pausingWritesIngestionStatusOnTheIndexDocument() throws Exception {
        createIndex();

        assertEquals(RestStatus.OK, dispatch("POST", "/my-index/ingestion/_pause", null).status());

        JsonNode index = storedIndex();
        assertTrue(index.get("ingestion_status").get("is_paused").booleanValue());
        assertFalse(index.get("settings").properties().stream()
            .anyMatch(setting -> setting.getKey().contains("ingestion")),
            "the flag is metadata, not a setting: " + index.get("settings"));
    }

    @Test
    void resumingClearsIt() throws Exception {
        createIndex();
        dispatch("POST", "/my-index/ingestion/_pause", null);

        assertEquals(RestStatus.OK, dispatch("POST", "/my-index/ingestion/_resume", null).status());
        assertFalse(storedIndex().get("ingestion_status").get("is_paused").booleanValue());
    }

    /** A new index has no ingestion status at all; absent means not paused. */
    @Test
    void aNewIndexHasNoIngestionStatus() throws Exception {
        createIndex();

        assertFalse(storedIndex().has("ingestion_status"),
            "absent rather than an explicit false, so the field appears only once it has been set");
    }

    /** Nothing else on the document may be disturbed by the read-modify-write. */
    @Test
    void pausingLeavesTheRestOfTheDocumentAlone() throws Exception {
        assertEquals(RestStatus.OK, dispatch("PUT", "/my-index",
            "{\"settings\":{\"number_of_shards\":3},"
                + "\"mappings\":{\"properties\":{\"host\":{\"type\":\"keyword\"}}}}").status());
        JsonNode before = storedIndex();

        dispatch("POST", "/my-index/ingestion/_pause", null);

        JsonNode after = storedIndex();
        assertEquals(before.get("uuid"), after.get("uuid"));
        assertEquals(before.get("settings"), after.get("settings"));
        assertEquals(before.get("mappings"), after.get("mappings"));
        assertEquals(before.get("state"), after.get("state"));
    }

    // ------------------------------------------------------------------
    // the response
    // ------------------------------------------------------------------

    /** Both fields are required by the spec, and shards_acknowledged is honestly false. */
    @Test
    void theResponseAcknowledgesWithoutClaimingTheShardsActed() throws Exception {
        createIndex();

        JsonNode response = body(dispatch("POST", "/my-index/ingestion/_pause", null));
        assertTrue(response.get("acknowledged").booleanValue());
        assertFalse(response.get("shards_acknowledged").booleanValue(),
            "this returns before any poller has seen the change");
        assertFalse(response.has("failures"), "a shard failure is not observable from a metadata write");
    }

    @Test
    void pausingAnIndexThatDoesNotExistIsNotFound() throws Exception {
        RestResponse response = dispatch("POST", "/nonesuch/ingestion/_pause", null);

        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals("index_not_found_exception", body(response).get("error").get("type").asText());
    }

    /** Pausing twice is acknowledged; there is nowhere in the response to say "already paused". */
    @Test
    void pausingTwiceIsAcknowledged() throws Exception {
        createIndex();

        assertEquals(RestStatus.OK, dispatch("POST", "/my-index/ingestion/_pause", null).status());
        assertEquals(RestStatus.OK, dispatch("POST", "/my-index/ingestion/_pause", null).status());
        assertTrue(storedIndex().get("ingestion_status").get("is_paused").booleanValue());
    }

    // ------------------------------------------------------------------
    // the handoff to the supervisor
    // ------------------------------------------------------------------

    @Test
    void pausingSubmitsATask() throws Exception {
        createIndex();
        dispatch("POST", "/my-index/ingestion/_pause", null);

        JsonNode task = ingestionTask();
        assertEquals("pause-ingestion", task.get("type").asText());
        assertEquals("my-index", task.get("index").asText());
        assertEquals(storedIndex().get("uuid").asText(), task.get("index_uuid").asText());
        assertEquals("pending", task.get("state").asText());
        assertFalse(task.has("details"), "a pause carries no payload");
    }

    @Test
    void resumingSubmitsATask() throws Exception {
        createIndex();
        dispatch("POST", "/my-index/ingestion/_resume", null);

        assertEquals("resume-ingestion", ingestionTask().get("type").asText());
    }

    /**
     * Consumer-pointer resets ride on the task, not on the index document. They are one-shot instructions,
     * and state that persisted would either be replayed for ever or need the supervisor to delete it — which
     * would mean the supervisor writing into this service's key space.
     */
    @Test
    void resetSettingsRideOnTheTaskRatherThanTheIndexDocument() throws Exception {
        createIndex();

        assertEquals(RestStatus.OK, dispatch("POST", "/my-index/ingestion/_resume",
            "{\"reset_settings\":[{\"shard\":0,\"mode\":\"OFFSET\",\"value\":\"1234\"},"
                + "{\"shard\":1,\"mode\":\"TIMESTAMP\",\"value\":\"1788969977933\"}]}").status());

        JsonNode resets = ingestionTask().get("details").get("reset_settings");
        assertEquals(2, resets.size());
        assertEquals(0, resets.get(0).get("shard").asInt());
        assertEquals("OFFSET", resets.get(0).get("mode").asText());
        assertEquals("1234", resets.get(0).get("value").asText());
        assertEquals(1, resets.get(1).get("shard").asInt());
        assertEquals("TIMESTAMP", resets.get(1).get("mode").asText());

        assertFalse(storedIndex().has("reset_settings"), "an instruction is not desired state");
        assertFalse(storedIndex().get("ingestion_status").has("reset_settings"));
    }

    @Test
    void anEmptyResetListCarriesNoPayload() throws Exception {
        createIndex();

        assertEquals(RestStatus.OK,
            dispatch("POST", "/my-index/ingestion/_resume", "{\"reset_settings\":[]}").status());
        assertFalse(ingestionTask().has("details"));
    }

    // ------------------------------------------------------------------
    // validating resets
    // ------------------------------------------------------------------

    /** One message covers all three fields, matching ResumeIngestionRequest.validate. */
    @Test
    void aResetMissingAnyFieldIsRejected() throws Exception {
        createIndex();

        for (String reset : List.of(
            "{\"mode\":\"OFFSET\",\"value\":\"1\"}",                  // no shard
            "{\"shard\":0,\"value\":\"1\"}",                          // no mode
            "{\"shard\":0,\"mode\":\"OFFSET\"}",                       // no value
            "{\"shard\":-1,\"mode\":\"OFFSET\",\"value\":\"1\"}",      // negative shard
            "{\"shard\":0,\"mode\":\"NONSENSE\",\"value\":\"1\"}"      // unknown mode
        )) {
            RestResponse response =
                dispatch("POST", "/my-index/ingestion/_resume", "{\"reset_settings\":[" + reset + "]}");
            assertEquals(RestStatus.BAD_REQUEST, response.status(), reset);
            assertEquals("Validation Failed: 1: ResetSettings is missing either shard, mode or value;",
                body(response).get("error").get("reason").asText(), reset);
        }
    }

    /** A rejected request must not have written the flag or a task. */
    @Test
    void aRejectedResetChangesNothing() throws Exception {
        createIndex();
        dispatch("POST", "/my-index/ingestion/_pause", null);

        dispatch("POST", "/my-index/ingestion/_resume", "{\"reset_settings\":[{\"shard\":0}]}");

        assertTrue(storedIndex().get("ingestion_status").get("is_paused").booleanValue(),
            "still paused: validation runs before the write");
        assertEquals(1, tasks().stream().filter(t -> t.get("type").asText().endsWith("-ingestion")).count(),
            "only the pause task exists");
    }

    @Test
    void aNonArrayResetSettingsIsRejected() throws Exception {
        createIndex();

        RestResponse response =
            dispatch("POST", "/my-index/ingestion/_resume", "{\"reset_settings\":\"nope\"}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertTrue(body(response).get("error").get("reason").asText().contains("must be an array"));
    }

    @Test
    void anUnparseableBodyIsRejected() throws Exception {
        createIndex();

        assertEquals(RestStatus.BAD_REQUEST,
            dispatch("POST", "/my-index/ingestion/_resume", "{not json").status());
    }

    // ------------------------------------------------------------------
    // scope
    // ------------------------------------------------------------------

    /**
     * Reading the state back stays out of scope: every field it returns is per-shard poller runtime, so it is
     * blocked on the same data plane projection as {@code cluster.health}. Routed, so a client gets an honest
     * answer rather than a 404.
     */
    @Test
    void readingIngestionStateIsNotRouted() {
        createIndex();

        // Excluded from the scope file, so no route is generated at all.
        assertEquals(RestStatus.BAD_REQUEST, dispatch("GET", "/my-index/ingestion/_state", null).status());
    }
}
