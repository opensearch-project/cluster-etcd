/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.metadata.MappingMerge;
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

/**
 * {@code PUT} and {@code POST /{index}/_mapping} — adds fields to an index's mapping.
 *
 * <p>Additive changes are free. Changes to a field that already exists are checked against the
 * updateability of each parameter — see {@link org.opensearch.controlplane.metadata.MappingParameters},
 * which carries OpenSearch's own {@code updateable} flags — and rejected with the wording a real
 * cluster uses. {@link MappingMerge} does both.
 *
 * <h2>Validate everything, then write everything</h2>
 *
 * <p>The expression can resolve to many indices, and OpenSearch applies the update to all of them or
 * none, because it is one cluster-state update. Here each index is a separate key, so that guarantee is
 * not available. What is available, and what this does, is to <b>merge against every index first and
 * only write once all of them succeed</b>. A conflict on the last index therefore leaves none of them
 * modified, which is the failure that actually matters: a partial mapping update is far worse than a
 * rejected one.
 *
 * <p>What remains is a crash between the first and last write. The window is small, the writes are
 * idempotent, and re-issuing the same request repairs it — a mapping update applied twice is a no-op.
 * See the README on why cross-resource atomicity is not required of this layer.
 *
 * <h2>Concurrency</h2>
 *
 * <p>Each write is a compare-and-set on the revision the metadata was read at, so two updates adding
 * different fields to the same index cannot lose one another. Losing the race is reported as a 409 —
 * this service's own shape, since a real cluster serialises through the cluster manager and has no
 * equivalent. A client should re-read and retry.
 *
 * <h2>Not implemented</h2>
 *
 * <p><b>{@code write_index_only}.</b> Declared by the spec and accepted here, but it only means
 * anything for an alias resolving to several indices with one write index, which needs the write-index
 * flag to be consulted during resolution. Currently the update applies to every resolved index.
 */
public final class PutMappingHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.put_mapping";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        if (request.hasBody() == false) {
            return ErrorResponses.parseFailure(request, "request body or source parameter is required");
        }
        JsonNode update;
        try {
            update = Json.parse(request.body());
        } catch (IOException e) {
            return ErrorResponses.parseFailure(request, "failed to parse mapping: " + e.getMessage());
        }
        if (update == null || update.isObject() == false) {
            return ErrorResponses.parseFailure(request, "mapping must be a JSON object");
        }

        IndexExpressions.Resolved resolved =
            IndexExpressions.resolve(request, context.store(), IndicesOptions.strictExpandOpen());
        if (resolved.failed()) {
            return resolved.failure();
        }

        IndexStore indices = new IndexStore(context.store());

        // Merge against every index before writing any of them, so a conflict on the last leaves the
        // first untouched.
        List<Pending> pending = new ArrayList<>();
        for (String index : resolved.indices().stream().sorted().toList()) {
            Optional<KeyValueStore.Versioned> stored = indices.getVersioned(index);
            if (stored.isEmpty()) {
                // Resolved a moment ago and gone now; treat it as never having been there.
                return ErrorResponses.indexNotFound(request, index);
            }
            ObjectNode metadata = (ObjectNode) parse(stored.get());
            ObjectNode mappings = metadata.path("mappings").isObject()
                ? (ObjectNode) metadata.get("mappings")
                : metadata.putObject("mappings");

            Optional<MappingMerge.Conflict> conflict = MappingMerge.merge(mappings, update);
            if (conflict.isPresent()) {
                return ErrorResponses.structured(
                    RestStatus.BAD_REQUEST, "illegal_argument_exception", conflict.get().message(), request);
            }
            pending.add(new Pending(index, metadata, stored.get().revision()));
        }

        IndexManagementTasks tasks = new IndexManagementTasks(context.store());
        for (Pending each : pending) {
            if (indices.replace(each.index(), each.metadata(), each.revision()) == false) {
                return ErrorResponses.concurrentUpdate(request, each.index());
            }
            tasks.submit(
                IndexManagementTasks.TYPE_UPDATE_MAPPING,
                each.index(),
                each.metadata().path("uuid").asText("_na_"),
                context.nowEpochMillis()
            );
        }
        return Json.acknowledged(request);
    }

    /** An index whose merged metadata is ready to write, and the revision it must still be at. */
    private record Pending(String index, ObjectNode metadata, long revision) {}

    private static JsonNode parse(KeyValueStore.Versioned versioned) {
        try {
            return Json.parse(versioned.value());
        } catch (IOException e) {
            throw new IllegalStateException("corrupt index metadata", e);
        }
    }
}
