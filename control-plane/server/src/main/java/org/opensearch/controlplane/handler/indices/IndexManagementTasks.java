/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;

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
final class IndexManagementTasks {

    static final String KEY_PREFIX = "tasks/index-management/";

    static final String TYPE_CREATE_INDEX = "create-index";
    static final String TYPE_DELETE_INDEX = "delete-index";

    static final String STATE_PENDING = "pending";

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
        task.put("task_id", taskId);
        task.put("type", type);
        task.put("index", index);
        task.put("index_uuid", indexUuid);
        task.put("state", STATE_PENDING);
        task.put("submitted_at", submittedAtEpochMillis);

        store.put(key(taskId), Json.toBytes(task));
        return taskId;
    }

    int pendingCount() {
        return store.scanPrefix(KEY_PREFIX).size();
    }
}
