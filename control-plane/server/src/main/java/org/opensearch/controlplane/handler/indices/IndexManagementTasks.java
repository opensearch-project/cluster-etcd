/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Index management tasks written for the shard manager to pick up.
 *
 * <p>This is the handoff point between layers. The control plane validates the request, writes index
 * metadata, and records a task here; the shard manager reads pending tasks, decides which data nodes
 * get which shards, and writes per-node configuration. Nothing calls anything directly.
 *
 * <p><b>Provisional.</b> The task record shape is a first cut and will need revisiting alongside two
 * decisions that are still open:
 *
 * <ul>
 *   <li><b>Task identity.</b> These IDs are opaque UUIDs, deliberately not OpenSearch's
 *       {@code {node_id}:{task_num}} form, because a standalone control plane has no node IDs. That
 *       is also why the {@code tasks} namespace is still out of scope — see the scope file. Exposing
 *       {@code _tasks} means committing to an ID format on the wire.</li>
 *   <li><b>Lifecycle.</b> The control plane owns retries and cleanup, so this will need attempt
 *       counts, leases or visibility timeouts, and a terminal-state sweep. None of that is here yet:
 *       a task is written once and never revisited.</li>
 * </ul>
 *
 * <p>Timestamps are taken from the caller rather than the clock so that tests are deterministic and
 * so a future implementation can use whatever clock the rest of the service agrees on.
 */
public final class IndexManagementTasks {

    public static final String KEY_PREFIX = "tasks/index-management/";

    static final String TYPE_CREATE_INDEX = "create-index";
    static final String TYPE_DELETE_INDEX = "delete-index";
    static final String TYPE_UPDATE_MAPPING = "update-mapping";
    static final String TYPE_UPDATE_SETTINGS = "update-settings";
    static final String TYPE_CLOSE_INDEX = "close-index";
    static final String TYPE_OPEN_INDEX = "open-index";

    static final String STATE_PENDING = "pending";

    /** Field names, shared with {@code cluster.pending_tasks}, which reports these records. */
    public static final String TYPE = "type";
    public static final String INDEX = "index";
    public static final String TASK_ID = "task_id";
    public static final String SUBMITTED_AT = "submitted_at";

    /**
     * Submission order within this process, so tasks written in the same millisecond stay ordered.
     *
     * <p>Needed because {@code submitted_at} is millisecond-resolution and one request can submit
     * several tasks — {@code put_settings} across a wildcard writes one per index, all in the same
     * millisecond. Without this they would be ordered by their random IDs, which is to say not at all,
     * and {@code cluster.pending_tasks} would report an {@code insert_order} that did not reflect
     * submission.
     *
     * <p><b>Per process, not global.</b> Two instances each start at zero, so across instances ordering
     * falls back to the timestamp and ties within a millisecond are arbitrary. A global counter would
     * mean a shared key every task write contended on, which is a bad trade for a display field —
     * OpenSearch can afford a real counter because it has one writer.
     */
    public static final String SEQUENCE = "sequence";

    private static final AtomicLong SEQUENCER = new AtomicLong();

    private final KeyValueStore store;

    IndexManagementTasks(KeyValueStore store) {
        this.store = store;
    }

    static String key(String taskId) {
        return KEY_PREFIX + taskId;
    }

    /**
     * Records a pending task.
     *
     * @param type       one of the {@code TYPE_} constants
     * @param index      the index the task concerns
     * @param indexUuid  the index's UUID, so a task remains unambiguous if the name is reused
     * @param submittedAtEpochMillis submission time, supplied by the caller
     * @return the task ID
     */
    String submit(String type, String index, String indexUuid, long submittedAtEpochMillis) {
        String taskId = IndexStore.newUuid();

        ObjectNode task = Json.object();
        task.put(TASK_ID, taskId);
        task.put(TYPE, type);
        task.put(INDEX, index);
        task.put("index_uuid", indexUuid);
        task.put("state", STATE_PENDING);
        task.put(SUBMITTED_AT, submittedAtEpochMillis);
        task.put(SEQUENCE, SEQUENCER.incrementAndGet());

        store.put(key(taskId), Json.toBytes(task));
        return taskId;
    }

    int pendingCount() {
        return store.scanPrefix(KEY_PREFIX).size();
    }

    /**
     * Every task the control plane still believes is pending, oldest first.
     *
     * <p>Ordered by submission time, then by {@link #SEQUENCE}, then by ID. The store scans in key order
     * and the keys are UUIDs, so without the sort the order would be arbitrary. The sequence matters
     * more than it looks: {@code submitted_at} is millisecond-resolution and one request can submit
     * several tasks, so timestamp alone leaves them tied and {@code insert_order} meaningless for
     * exactly the case where a client submitted a batch.
     *
     * <p><b>Nothing removes a task, so this only grows.</b> Completion is the shard manager's half of
     * the contract and is not built; see the class javadoc on lifecycle. {@code cluster.pending_tasks}
     * reports this list faithfully, which makes the gap visible rather than hiding it.
     */
    public static List<JsonNode> pending(KeyValueStore store) {
        List<JsonNode> tasks = new ArrayList<>();
        store.scanPrefix(KEY_PREFIX).forEach((key, versioned) -> tasks.add(parse(versioned.value())));
        tasks.sort(
            Comparator.comparingLong((JsonNode task) -> task.path(SUBMITTED_AT).asLong(0))
                .thenComparingLong(task -> task.path(SEQUENCE).asLong(0))
                .thenComparing(task -> task.path(TASK_ID).asText(""))
        );
        return tasks;
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt index management task", e);
        }
    }
}
