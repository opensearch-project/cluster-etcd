/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

/**
 * {@code GET /{index}} — everything about an index in one response.
 *
 * <p>The three blocks {@code get_alias}, {@code get_mapping} and {@code get_settings} answer
 * separately, gathered per index and emitted in that order: {@code aliases}, {@code mappings},
 * {@code settings}. Verified against OpenSearch 3.9.0, including the order.
 *
 * <p><b>Every block is always present, empty as {@code {}} when there is nothing.</b> An index with no
 * aliases and no mappings answers {@code {"i": {"aliases": {}, "mappings": {}, "settings": {...}}}}.
 * Note this is the opposite of {@code get_settings} with a name filter, where an index with nothing to
 * report is dropped from the response entirely — the two rules look alike and are not.
 *
 * <p>{@code include_defaults} is accepted and ignored, for the reason given on
 * {@link GetSettingsHandler}: computed values belong to the layer that computes them.
 *
 * <p>Worth knowing about routing: {@code GET /anything} lands here, because {@code GET /{index}} is a
 * registered route and any single segment is a valid index name. That is OpenSearch's behaviour too —
 * see the README on the hand-written router — so an unknown single-segment path answers
 * {@code index_not_found_exception} rather than the no-handler 400.
 */
public final class GetIndexHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.get";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        IndexExpressions.Resolved resolved =
            IndexExpressions.resolve(request, context.store(), IndicesOptions.strictExpandOpen());
        if (resolved.failed()) {
            return resolved.failure();
        }

        boolean flatSettings = request.paramAsBoolean("flat_settings", false);
        IndexStore indices = new IndexStore(context.store());
        AliasStore aliases = new AliasStore(context.store());

        ObjectNode body = Json.object();
        resolved.indices().stream().sorted().forEach(index -> indices.get(index).ifPresent(metadata -> {
            ObjectNode entry = Json.object();

            ObjectNode aliasesForIndex = Json.object();
            aliases.forIndex(index).forEach((alias, properties) -> aliasesForIndex.set(alias, properties.deepCopy()));
            entry.set("aliases", aliasesForIndex);

            JsonNode mappings = metadata.path("mappings");
            entry.set("mappings", mappings.isObject() ? mappings.deepCopy() : Json.object());

            entry.set("settings", IndexStore.reportedSettings(metadata).toJson(flatSettings));
            body.set(index, entry);
        }));
        return Json.response(RestStatus.OK, body, request);
    }
}
