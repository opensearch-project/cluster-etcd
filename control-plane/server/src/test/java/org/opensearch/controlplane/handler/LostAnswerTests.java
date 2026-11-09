/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.store.LosingKeyValueStore;
import org.opensearch.controlplane.store.LosingKeyValueStore.Outcome;
import org.opensearch.controlplane.tenant.SingleTenantResolver;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Writes whose answer the store loses ({@code OutcomeUnknownException}): settled by reading back where the value is
 * unique to the writer, and otherwise reported as upstream reports a change that may or may not have applied.
 */
class LostAnswerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TENANT = SingleTenantResolver.DEFAULT_TENANT.keyPrefix();

    private InMemoryKeyValueStore root;
    private LosingKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        root = new InMemoryKeyValueStore();
        store = new LosingKeyValueStore(root);
        router = Router.fromGeneratedRoutes(ControlPlaneHandlers.build(), TestContexts.factory(store));
    }

    private RestResponse send(String method, String path, String body) {
        return router.dispatch(method, path, path, Map.of(), Map.of(),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8));
    }

    private static JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(response.body());
    }

    private JsonNode stored(String key) throws Exception {
        return MAPPER.readTree(root.get(TENANT + key).orElseThrow().value());
    }

    private int pendingTasks() {
        return root.scanPrefix(TENANT + "tasks/index-management/").size();
    }

    // ------------------------------------------------------------------ indices.create

    /** The case that mattered: an applied create used to answer an error, and its retry "already exists". */
    @Test
    void aCreateThatAppliedIsACreateWhateverTheAnswer() throws Exception {
        store.lose(TENANT + "indices/logs", Outcome.APPLIED);

        RestResponse response = send("PUT", "/logs", null);
        assertEquals(RestStatus.OK, response.status(), () -> new String(response.body(), StandardCharsets.UTF_8));
        assertTrue(body(response).get("acknowledged").asBoolean());
        assertEquals(1, pendingTasks(), "and it went on to record its task");
    }

    @Test
    void aCreateThatDidNotApplyIsTriedAgain() throws Exception {
        store.lose(TENANT + "indices/logs", Outcome.NOT_APPLIED);

        assertEquals(RestStatus.OK, send("PUT", "/logs", null).status());
        assertEquals("logs", stored("indices/logs").get("index").asText());
    }

    /** The retry meets its own first attempt, applied late: a conflict that is still ours. */
    @Test
    void aRetryThatConflictsWithItsOwnLateFirstAttemptIsStillACreate() throws Exception {
        store.lose(TENANT + "indices/logs", Outcome.APPLIED_LATE);

        assertEquals(RestStatus.OK, send("PUT", "/logs", null).status());
        assertEquals(1, pendingTasks());
    }

    @Test
    void anotherWriterInTheGapStillWins() throws Exception {
        store.lose(TENANT + "indices/logs", Outcome.NOT_APPLIED)
            .meanwhile(TENANT + "indices/logs", () -> root.put(TENANT + "indices/logs",
                "{\"index\":\"logs\",\"uuid\":\"someone-else\",\"state\":\"open\",\"settings\":{}}"
                    .getBytes(StandardCharsets.UTF_8)));

        RestResponse response = send("PUT", "/logs", null);
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("resource_already_exists_exception", error.get("type").asText());
        assertEquals("someone-else", error.get("index_uuid").asText(), "the winner's uuid, not the one this request made");
        assertEquals(0, pendingTasks());
    }

    /** Lost every time and nothing stored: unknown, so said as upstream says it. */
    @Test
    void anOutcomeTheStoreKeepsLosingIsReportedAsMaybeApplied() throws Exception {
        store.lose(TENANT + "indices/logs", Outcome.NOT_APPLIED, Outcome.NOT_APPLIED, Outcome.NOT_APPLIED);

        RestResponse response = send("PUT", "/logs", null);
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("failed_to_commit_cluster_state_exception", error.get("type").asText());
        assertTrue(error.get("reason").asText().startsWith(
            "the store lost its answer, so this change may or may not have been applied; read it back to find out"),
            error.get("reason").asText());
        assertFalse(root.get(TENANT + "indices/logs").isPresent());
    }

    // ------------------------------------------------------------------ the other creates

    @Test
    void aDataStreamThatAppliedIsCreated() throws Exception {
        send("PUT", "/_index_template/ds", "{\"index_patterns\":[\"logs*\"],\"data_stream\":{}}");
        store.lose(TENANT + "data_streams/logs", Outcome.APPLIED);

        RestResponse response = send("PUT", "/_data_stream/logs", null);
        assertEquals(RestStatus.OK, response.status(), () -> new String(response.body(), StandardCharsets.UTF_8));
        assertEquals(1, stored("data_streams/logs").get("generation").asInt());
    }

    @Test
    void theClusterUuidIsWhateverWasStoredWhenTheAnswerIsLost() throws Exception {
        store.lose(TENANT + "cluster/uuid", Outcome.APPLIED);

        String first = body(send("GET", "/", null)).get("cluster_uuid").asText();
        assertEquals(new String(root.get(TENANT + "cluster/uuid").orElseThrow().value(), StandardCharsets.UTF_8), first);
        assertEquals(first, body(send("GET", "/", null)).get("cluster_uuid").asText());
    }

    // ------------------------------------------------------------------ a write nothing settles

    /** An upsert carries nothing unique to read back, so its lost answer reaches the client as maybe-applied. */
    @Test
    void anUnsettledWriteElsewhereIsReportedTheSameWay() throws Exception {
        store.lose(TENANT + "ingest/pipeline/p", Outcome.APPLIED);

        RestResponse response = send("PUT", "/_ingest/pipeline/p", "{\"processors\":[]}");
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, response.status());
        assertEquals("failed_to_commit_cluster_state_exception", body(response).get("error").get("type").asText());
    }
}
