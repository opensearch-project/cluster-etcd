/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * {@code POST /{index}/_close} and {@code POST /{index}/_open} — the two index states.
 *
 * <p>One class for both, because they are mirror images: each flips {@code state}, each expands
 * wildcards only to the state it can act on, and the only asymmetries are the response shape and the
 * {@code index.verified_before_close} marker. Keeping them together is what makes those asymmetries
 * visible rather than scattered.
 *
 * <h2>Why these exist here at all</h2>
 *
 * <p>Not for their own sake — they close the last hole in {@code indices.put_settings}. A
 * <b>static</b> setting may be changed on a closed index and not on an open one, so without close there
 * was a branch of that operation no request could reach. See {@link PutIndexSettingsHandler}.
 *
 * <h2>Wildcard expansion is asymmetric, deliberately</h2>
 *
 * <p>{@code close} expands wildcards to <b>open</b> indices, {@code open} expands them to <b>closed</b>
 * ones — {@code CloseIndexRequest} uses {@code strictExpandOpen()} while {@code OpenIndexRequest} uses
 * {@code fromOptions(false, true, false, true)}. It reads oddly in isolation and makes sense as a pair:
 * each operation reaches exactly the indices it would change, so {@code POST /*​/_open} opens everything
 * closed and touches nothing else. Verified — opening a wildcard matching two closed indices opened both.
 *
 * <h2>The response shapes differ</h2>
 *
 * <p>Verified against OpenSearch 3.9.0:
 *
 * <ul>
 *   <li><b>close</b> — {@code {"acknowledged":true,"shards_acknowledged":true,"indices":{"i":{"closed":true}}}}.
 *       The {@code indices} block lists <b>only indices this request actually closed</b>: closing an
 *       already-closed index answers {@code "indices":{}} with
 *       {@code "shards_acknowledged":false}.</li>
 *   <li><b>open</b> — {@code {"acknowledged":true,"shards_acknowledged":true}}, with no
 *       {@code indices} block at all.</li>
 * </ul>
 *
 * <p>{@code shards_acknowledged} is {@code true} here rather than following
 * {@code indices.create}'s deliberate {@code false}, and the reason is that it is honest: on close there
 * are no shards left to wait for, and a request that changed nothing reports {@code false} exactly as a
 * real cluster does. Reopening does await shards on a real cluster, so that one is the same optimism
 * create declines — recorded in the README rather than papered over.
 */
public final class ChangeIndexStateHandler implements OperationHandler {

    private final boolean closing;

    ChangeIndexStateHandler(boolean closing) {
        this.closing = closing;
    }

    /** Both operations, so the registration list reads as a pair. */
    public static List<OperationHandler> handlers() {
        return List.of(new ChangeIndexStateHandler(true), new ChangeIndexStateHandler(false));
    }

    @Override
    public String operationGroup() {
        return closing ? "indices.close" : "indices.open";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        IndexExpressions.Resolved resolved = IndexExpressions.resolve(
            request,
            context.store(),
            closing ? IndicesOptions.forClose() : IndicesOptions.forOpen()
        );
        if (resolved.failed()) {
            return resolved.failure();
        }

        IndexStore indices = new IndexStore(context.store());
        IndexManagementTasks tasks = new IndexManagementTasks(context.store());
        String targetState = closing ? IndexStore.STATE_CLOSE : IndexStore.STATE_OPEN;
        // Every index checked and changed in memory before any is written, as put_mapping does, so a
        // failure on the last leaves the first untouched.
        List<Staged> staged = new ArrayList<>();
        for (String index : resolved.indices().stream().sorted().toList()) {
            Optional<KeyValueStore.Versioned> stored = indices.getVersioned(index);
            if (stored.isEmpty()) {
                return ErrorResponses.indexNotFound(request, index);
            }
            ObjectNode metadata = (ObjectNode) parse(stored.get());
            if (IndexStore.isOpen(metadata) != closing) {
                // Already in the state asked for. Idempotent, and it contributes nothing to the
                // response -- which is how a real cluster distinguishes "closed it" from "was closed".
                continue;
            }

            metadata.put(IndexStore.STATE, targetState);
            metadata.set("settings", markerFor(metadata, closing).toJson(true));
            Optional<RestResponse> tooLarge = IndexStore.rejectIfTooLarge(request, index, metadata);
            if (tooLarge.isPresent()) {
                return tooLarge.get();
            }
            staged.add(new Staged(index, metadata, stored.get().revision()));
        }

        List<String> changed = new ArrayList<>();
        for (Staged each : staged) {
            if (indices.replace(each.index(), each.metadata(), each.revision()) == false) {
                return ErrorResponses.concurrentUpdate(request, each.index());
            }
            tasks.submit(
                closing ? IndexManagementTasks.TYPE_CLOSE_INDEX : IndexManagementTasks.TYPE_OPEN_INDEX,
                each.index(),
                each.metadata().path("uuid").asText("_na_"),
                context.nowEpochMillis()
            );
            changed.add(each.index());
        }

        ObjectNode body = Json.object();
        body.put("acknowledged", true);
        body.put("shards_acknowledged", changed.isEmpty() == false);
        if (closing) {
            ObjectNode byIndex = Json.object();
            changed.forEach(index -> {
                ObjectNode entry = Json.object();
                entry.put("closed", true);
                byIndex.set(index, entry);
            });
            body.set("indices", byIndex);
        }
        return Json.response(RestStatus.OK, body, request);
    }

    /** Closing adds {@code index.verified_before_close}; opening removes it. Verified both ways. */
    private static Settings markerFor(ObjectNode metadata, boolean closing) {
        Settings settings = Settings.forIndex(metadata.path("settings"));
        return closing
            ? settings.with(IndexStore.SETTING_VERIFIED_BEFORE_CLOSE, "true")
            : settings.without(Set.of(IndexStore.SETTING_VERIFIED_BEFORE_CLOSE));
    }

    private static JsonNode parse(KeyValueStore.Versioned versioned) {
        try {
            return Json.parse(versioned.value());
        } catch (IOException e) {
            throw new IllegalStateException("corrupt index metadata", e);
        }
    }

    /** An index whose changed metadata is ready to write, and the revision it must still be at. */
    private record Staged(String index, ObjectNode metadata, long revision) {}
}
