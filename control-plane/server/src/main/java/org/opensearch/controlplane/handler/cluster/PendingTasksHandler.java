/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.handler.indices.IndexManagementTasks;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.util.List;

/**
 * {@code GET /_cluster/pending_tasks} — work the control plane has accepted and not yet seen finished.
 *
 * <p>The one operation this architecture makes <em>easier</em> rather than harder. On a real cluster
 * this reports the cluster manager's internal state-update queue, which a standalone service has no
 * equivalent of. Here it is a read over our own keys: every index management task written for the shard
 * manager is, by definition, pending until the shard manager finishes it. Same question, and we already
 * store the answer.
 *
 * <p>Field order follows {@code PendingClusterTasksResponse.toXContent}: {@code insert_order},
 * {@code priority}, {@code source}, {@code executing}, {@code time_in_queue_millis},
 * {@code time_in_execution_millis}.
 *
 * <h2>The two duration strings are deliberately omitted</h2>
 *
 * <p>A real cluster emits {@code time_in_queue} and {@code time_in_execution} alongside their
 * {@code _millis} counterparts — {@code "1.5s"} next to {@code 1500} — and, unlike most such pairs, it
 * does so <em>unconditionally</em> rather than under the {@code human} parameter. That makes it a harder
 * case than {@code human} and the same call: <b>formatting a value for a person to read is a front-end
 * concern, not a server one.</b> A duration is a number; rendering it as {@code 1.5s} is a presentation
 * decision that belongs where the presenting happens.
 *
 * <p>So the machine-readable half is emitted and the prose half is not. The spec marks neither as
 * required, so the response stays valid. A client that wants {@code "1.5s"} has {@code 1500} and knows
 * its own locale, which the server does not. See the README's considered divergences, where this sits
 * alongside {@code human} for the same reason.
 *
 * <h2>The list only grows, and that is worth knowing</h2>
 *
 * <p><b>Nothing marks a task complete.</b> Completion is the shard manager's half of the handoff and is
 * not built — {@code IndexManagementTasks} says as much — so every task ever submitted stays pending and
 * this response grows without bound. That is not a bug in this handler: it reports faithfully what the
 * control plane believes, and what it believes is currently "all of it".
 *
 * <p>Reporting it honestly is the right call, because it makes the gap visible at exactly the place an
 * operator would look. Filtering to a recent window, or capping the list, would hide the missing half of
 * the contract behind a plausible-looking response. The fix is a task lifecycle, not a smaller read.
 *
 * <h2>Three fields we cannot answer the way a real cluster does</h2>
 *
 * <ul>
 *   <li><b>{@code insert_order}</b> — OpenSearch's is a counter from its queue. There is no counter
 *       here, so this is the task's <em>position</em> in submission order, one-based. That satisfies
 *       what the field is documented to mean ("when the task has been inserted") and is stable for a
 *       given set of tasks, but it is not comparable across calls the way a counter would be: inserting
 *       an older task cannot happen, but removing one renumbers those after it.</li>
 *   <li><b>{@code priority}</b> — always {@code NORMAL}. Our tasks are not cluster-state updates and
 *       nothing schedules them by priority, so any other value would be decoration. Left as the neutral
 *       one rather than guessed per operation.</li>
 *   <li><b>{@code executing}</b> — always {@code false}, and {@code time_in_execution} always zero. A
 *       task has no execution state here; the shard manager would have to report that back, which is
 *       the same missing lifecycle.</li>
 * </ul>
 *
 * <p>{@code source} is ours: {@code <type> [<index>]}, e.g. {@code create-index [my-index]}. A real
 * cluster's is prose that includes a cause. This is the same information in the shape we have.
 */
public final class PendingTasksHandler implements OperationHandler {

    private static final String NEUTRAL_PRIORITY = "NORMAL";

    @Override
    public String operationGroup() {
        return "cluster.pending_tasks";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        long now = context.nowEpochMillis();
        List<JsonNode> pending = IndexManagementTasks.pending(context.store());

        ArrayNode tasks = Json.array();
        int insertOrder = 1;
        for (JsonNode task : pending) {
            long queuedMillis = Math.max(0, now - task.path(IndexManagementTasks.SUBMITTED_AT).asLong(now));

            ObjectNode entry = Json.object();
            entry.put("insert_order", insertOrder++);
            entry.put("priority", NEUTRAL_PRIORITY);
            entry.put("source", sourceOf(task));
            entry.put("executing", false);
            entry.put("time_in_queue_millis", queuedMillis);
            // time_in_queue and time_in_execution are deliberately absent; see the class javadoc.
            entry.put("time_in_execution_millis", 0);
            tasks.add(entry);
        }

        ObjectNode body = Json.object();
        body.set("tasks", tasks);
        return Json.response(RestStatus.OK, body, request);
    }

    /** {@code create-index [my-index]} — our wording, carrying what a real cluster's prose would. */
    private static String sourceOf(JsonNode task) {
        return task.path(IndexManagementTasks.TYPE).asText("unknown")
            + " [" + task.path(IndexManagementTasks.INDEX).asText("") + "]";
    }
}
