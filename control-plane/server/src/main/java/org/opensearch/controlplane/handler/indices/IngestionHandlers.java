/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Pausing and resuming pull-based ingestion: {@code POST /{index}/ingestion/_pause} and
 * {@code /_resume}.
 *
 * <p>In {@code handler.indices} rather than a {@code handler.ingestion} mirroring the spec namespace, because
 * everything these do is read-modify-write an index document and submit an index-management task — the same
 * cohesion argument that puts {@code ingest.processor_grok} beside the pipeline handlers. Placing them by
 * namespace would mean widening {@code IndexStore} and {@code IndexManagementTasks} to public for packaging
 * reasons alone.
 *
 * <h2>Why this is the control plane's, when ingestion is the data plane's</h2>
 *
 * <p>The {@code ingestion} namespace was excluded wholesale as "data plane", and that was right about
 * <em>performing</em> ingestion and wrong about <em>controlling</em> it. Consuming from a stream is data plane
 * work. Deciding whether a shard should be consuming at all is a metadata write that finishes inside its own
 * request — the same shape as {@code indices.close} — and the data plane picks the change up through the
 * supervisor. Reading the state back is a third thing again, and stays out of scope; see below.
 *
 * <h2>Where the pause flag lives</h2>
 *
 * <p>{@code ingestion_status} is a <b>first-class field on the index document, not a setting</b>. Upstream it
 * is {@code IndexMetadata.builder(...).ingestionStatus(...)} under the key {@code ingestion_status}, holding a
 * single boolean {@code is_paused} that defaults to false. Worth stating because the obvious guess is
 * {@code index.ingestion.paused} in the settings block, and a supervisor looking there would find nothing.
 *
 * <pre>{@code
 * tenant/<t>/indices/<name>  ->  { …, "ingestion_status": { "is_paused": true } }
 * }</pre>
 *
 * <h2>Consumer-pointer resets are instructions, not state</h2>
 *
 * <p>{@code _resume} optionally carries {@code reset_settings}: per-shard instructions to move the consumer to
 * an offset or timestamp before resuming. These deliberately do <b>not</b> go on the index document. They are
 * one-shot commands, so recording them as state would mean either replaying them on every reconciliation pass
 * or having the supervisor delete them afterwards — and the supervisor writing into this service's key space
 * is precisely what the plane boundary forbids.
 *
 * <p>So a reset rides on the task record instead, under {@link IndexManagementTasks#DETAILS}. Tasks are
 * already the channel for "do this once, then reap it", which is what a reset is.
 *
 * <h2>Verified, and not</h2>
 *
 * <p>The stored shape, the field names and the validation wording are transcribed from OpenSearch's own
 * sources — {@code IngestionStatus}, {@code ResumeIngestionRequest.validate} — rather than diffed against a
 * live cluster. Exercising these upstream needs an index with a configured streaming source, which is more
 * than a probe. Treated as unverified in the README and worth a diff once the data plane has a source running.
 *
 * <h2>Considered divergences</h2>
 *
 * <p><b>{@code shards_acknowledged} is always false.</b> The spec marks it required, and a real cluster reports
 * whether every shard's poller acted on the change — which this service returns before any shard has seen it.
 * Same call as {@code indices.create}, and for the same reason: the honest answer is that we did not wait.
 *
 * <p><b>{@code failures} is never present.</b> It is a map of index name to per-shard failures, and a shard
 * failure is not something a metadata write can observe.
 *
 * <h2>Out of scope: {@code ingestion.get_state}</h2>
 *
 * <p>Every field it returns is per-shard runtime — {@code poller_state}, {@code poller_paused},
 * {@code batch_start_pointer}, {@code error_policy}, {@code is_primary}, and the node holding the shard — and
 * the response pages over shards. None of that exists in metadata, so it is blocked on the same data plane
 * projection as {@code cluster.health}. Note it would report {@code poller_paused} per shard, which is the
 * *observed* pause; {@code is_paused} here is the *desired* one, and the gap between them is exactly what the
 * supervisor closes.
 */
public final class IngestionHandlers {

    /** The index document field, matching {@code IndexMetadata.INGESTION_STATUS_KEY}. */
    static final String INGESTION_STATUS = "ingestion_status";

    /** Matching {@code IngestionStatus.IS_PAUSED}. Defaults to false when the field is absent. */
    static final String IS_PAUSED = "is_paused";

    static final String RESET_SETTINGS = "reset_settings";
    static final String SHARD = "shard";
    static final String MODE = "mode";
    static final String VALUE = "value";

    /** {@code ResumeIngestionRequest.ResetSettings.ResetMode}. Upper case on the wire. */
    private static final Set<String> RESET_MODES = Set.of("OFFSET", "TIMESTAMP");

    private IngestionHandlers() {}

    public static List<OperationHandler> handlers() {
        return List.of(new Pause(), new Resume());
    }

    /** {@code POST /{index}/ingestion/_pause}. */
    static final class Pause implements OperationHandler {

        @Override
        public String operationGroup() {
            return "ingestion.pause";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            return setPaused(request, context, true, null);
        }
    }

    /** {@code POST /{index}/ingestion/_resume}, optionally carrying consumer-pointer resets. */
    static final class Resume implements OperationHandler {

        @Override
        public String operationGroup() {
            return "ingestion.resume";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            // The body is optional: a resume with no resets is the common case.
            ArrayNode resets = null;
            if (request.hasBody()) {
                JsonNode body;
                try {
                    body = Json.parse(request.body());
                } catch (IOException e) {
                    return ErrorResponses.parseFailure(request, "failed to parse resume request body");
                }
                if (body == null || body.isObject() == false) {
                    return ErrorResponses.parseFailure(request, "failed to parse resume request body");
                }
                JsonNode declared = body.path(RESET_SETTINGS);
                if (declared.isMissingNode() == false && declared.isNull() == false) {
                    if (declared.isArray() == false) {
                        return ErrorResponses.illegalArgument(request, RESET_SETTINGS + " must be an array");
                    }
                    RestResponse rejection = validateResets(request, declared);
                    if (rejection != null) {
                        return rejection;
                    }
                    resets = (ArrayNode) declared;
                }
            }
            return setPaused(request, context, false, resets);
        }

        /**
         * {@code ResumeIngestionRequest.validate}: one message for all three fields, raised if <em>any</em>
         * entry is missing any of them. A negative shard counts as missing, since the server's parser
         * defaults it to -1.
         */
        private static RestResponse validateResets(RestRequest request, JsonNode resets) {
            for (JsonNode reset : resets) {
                boolean shardOk = reset.path(SHARD).isIntegralNumber() && reset.path(SHARD).asInt() >= 0;
                boolean modeOk = reset.path(MODE).isTextual()
                    && RESET_MODES.contains(reset.path(MODE).asText().toUpperCase(Locale.ROOT));
                boolean valueOk = reset.path(VALUE).isTextual();
                if (shardOk == false || modeOk == false || valueOk == false) {
                    return ErrorResponses.validationFailed(
                        request, "ResetSettings is missing either shard, mode or value");
                }
            }
            return null;
        }
    }

    /**
     * The whole of both operations: flip one field on one index document, record a task, answer.
     *
     * @param resets consumer-pointer resets to hand to the supervisor, or null
     */
    private static RestResponse setPaused(
        RestRequest request,
        OperationContext context,
        boolean paused,
        ArrayNode resets
    ) {
        String index = request.pathParam("index");
        IndexStore indices = new IndexStore(context.store());

        Optional<KeyValueStore.Versioned> stored = indices.getVersioned(index);
        if (stored.isEmpty()) {
            return ErrorResponses.indexNotFound(request, index);
        }
        ObjectNode metadata;
        try {
            metadata = (ObjectNode) Json.parse(stored.get().value());
        } catch (IOException e) {
            throw new IllegalStateException("corrupt index metadata for [" + index + "]", e);
        }

        // Written even when it already holds this value. Unlike close/open -- which reports "was already
        // closed" by leaving the index out of its response -- these responses have nowhere to say so, and a
        // resume carrying resets has work to do regardless of the flag.
        ObjectNode status = Json.object();
        status.put(IS_PAUSED, paused);
        metadata.set(INGESTION_STATUS, status);

        if (indices.replace(index, metadata, stored.get().revision()) == false) {
            return ErrorResponses.concurrentUpdate(request, index);
        }

        ObjectNode details = null;
        if (resets != null && resets.isEmpty() == false) {
            details = Json.object();
            details.set(RESET_SETTINGS, resets.deepCopy());
        }
        new IndexManagementTasks(context.store()).submit(
            paused ? IndexManagementTasks.TYPE_PAUSE_INGESTION : IndexManagementTasks.TYPE_RESUME_INGESTION,
            index,
            metadata.path("uuid").asText("_na_"),
            context.nowEpochMillis(),
            details
        );

        ObjectNode body = Json.object();
        body.put("acknowledged", true);
        // Always false: this returns before any shard's poller has seen the change. See the class javadoc.
        body.put("shards_acknowledged", false);
        return Json.response(RestStatus.OK, body, request);
    }
}
