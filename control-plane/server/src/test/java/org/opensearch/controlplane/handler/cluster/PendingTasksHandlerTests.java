/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code GET /_cluster/pending_tasks} — a read over the tasks this service wrote for the shard manager.
 *
 * <p>A fixed clock throughout, so queue times are exact rather than approximately asserted.
 */
class PendingTasksHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long START = 1_760_000_000_000L;

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore();

    private Router at(long millis) {
        return Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(),
            TestContexts.factory(store, Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC))
        );
    }

    private void create(Router router, String index) {
        String path = "/" + index;
        assertEquals(RestStatus.OK,
            router.dispatch("PUT", path, path, Map.of(), Map.of(), new byte[0]).status(), index);
    }

    private JsonNode pendingTasks(Router router) throws Exception {
        RestResponse response = router.dispatch("GET", "/_cluster/pending_tasks");
        assertEquals(RestStatus.OK, response.status());
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8)).get("tasks");
    }

    @Test
    void anIdleControlPlaneReportsNoTasks() throws Exception {
        assertEquals(0, pendingTasks(at(START)).size());
    }

    /** Field names and their order follow PendingClusterTasksResponse. */
    @Test
    void aTaskCarriesEveryFieldInTheServersOrder() throws Exception {
        create(at(START), "my-index");

        JsonNode task = pendingTasks(at(START + 1500)).get(0);
        // No time_in_queue or time_in_execution: formatting a duration for a person to read is a
        // front-end concern. See PendingTasksHandler and the README's considered divergences.
        assertEquals(
            List.of("insert_order", "priority", "source", "executing",
                "time_in_queue_millis", "time_in_execution_millis"),
            task.propertyStream().map(Map.Entry::getKey).toList()
        );
        assertEquals(1, task.get("insert_order").asInt());
        assertEquals("NORMAL", task.get("priority").asText());
        assertEquals("create-index [my-index]", task.get("source").asText());
        assertFalse(task.get("executing").asBoolean());
        assertEquals(1500, task.get("time_in_queue_millis").asLong());
        assertEquals(0, task.get("time_in_execution_millis").asLong());
    }

    /** Oldest first, and insert_order counts from one in that order. */
    @Test
    void tasksAreOrderedBySubmissionWithInsertOrderToMatch() throws Exception {
        create(at(START), "first");
        create(at(START + 1000), "second");
        create(at(START + 2000), "third");

        JsonNode tasks = pendingTasks(at(START + 3000));
        assertEquals(3, tasks.size());
        assertEquals(
            List.of("create-index [first]", "create-index [second]", "create-index [third]"),
            List.of(tasks.get(0).get("source").asText(), tasks.get(1).get("source").asText(),
                tasks.get(2).get("source").asText())
        );
        assertEquals(List.of(1, 2, 3),
            List.of(tasks.get(0).get("insert_order").asInt(), tasks.get(1).get("insert_order").asInt(),
                tasks.get(2).get("insert_order").asInt()));
        // Oldest has waited longest.
        assertEquals(3000, tasks.get(0).get("time_in_queue_millis").asLong());
        assertEquals(1000, tasks.get(2).get("time_in_queue_millis").asLong());
    }

    /** Every operation that hands work off shows up, which is what makes this useful. */
    @Test
    void reportsEveryKindOfTaskWeWrite() throws Exception {
        Router router = at(START);
        create(router, "my-index");
        router.dispatch("PUT", "/my-index/_mapping", "/my-index/_mapping", Map.of(), Map.of(),
            "{\"properties\":{\"t\":{\"type\":\"text\"}}}".getBytes(StandardCharsets.UTF_8));
        router.dispatch("PUT", "/my-index/_settings", "/my-index/_settings", Map.of(), Map.of(),
            "{\"index\":{\"number_of_replicas\":2}}".getBytes(StandardCharsets.UTF_8));
        router.dispatch("POST", "/my-index/_close");
        router.dispatch("POST", "/my-index/_open");
        router.dispatch("DELETE", "/my-index");

        List<String> sources = pendingTasks(at(START)).valueStream()
            .map(task -> task.get("source").asText()).toList();
        assertEquals(
            List.of("create-index [my-index]", "update-mapping [my-index]", "update-settings [my-index]",
                "close-index [my-index]", "open-index [my-index]", "delete-index [my-index]"),
            sources
        );
    }

    /**
     * Nothing marks a task complete, so the list only grows — reported honestly rather than filtered.
     * See {@link PendingTasksHandler}: the fix is a task lifecycle, not a smaller read.
     */
    @Test
    void completedWorkStillShowsBecauseNothingMarksItDone() throws Exception {
        Router router = at(START);
        create(router, "my-index");
        router.dispatch("DELETE", "/my-index");

        assertEquals(2, pendingTasks(at(START)).size(),
            "the index is gone and both tasks are still pending");
    }

    /** Tenant-confined like everything else: one tenant's queue is not another's. */
    @Test
    void tasksAreScopedToTheRequestingTenant() throws Exception {
        create(at(START), "my-index");

        assertEquals(1, pendingTasks(at(START)).size());
        assertTrue(store.scanPrefix("tenant/default/tasks/index-management/").size() == 1,
            "stored under the tenant prefix");
    }

}
