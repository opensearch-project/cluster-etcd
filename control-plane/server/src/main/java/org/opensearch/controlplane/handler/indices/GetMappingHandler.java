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
 * {@code GET /_mapping} and {@code GET /{index}/_mapping} — an index's mappings.
 *
 * <p>{@code {"<index>": {"mappings": {...}}}}, and <b>an index with no mappings is still reported</b>,
 * as {@code "mappings": {}}. Verified against OpenSearch 3.9.0.
 *
 * <h2>Mappings are stored as sent</h2>
 *
 * <p>Unlike settings, mappings are not normalised. A real cluster parses them through the mapping
 * engine and re-serialises, which reorders them; here the declared order survives. The values agree and
 * only key order differs, which JSON does not treat as meaningful.
 *
 * <p>It is worth being precise about what reordering means, because it decides whether a sort would fix
 * this. Verified against 3.9.0 with a mapping declared
 * {@code {properties: {zz: {..., fields: {zraw, araw}}, aa}, dynamic, _source}}:
 *
 * <ul>
 *   <li>the field dictionaries <b>{@code properties} and {@code fields} come back sorted</b> —
 *       {@code aa} before {@code zz}, {@code araw} before {@code zraw};</li>
 *   <li>the other mapping keys do <b>not</b> — {@code dynamic} came back <em>before</em>
 *       {@code properties}, which is neither the declared order nor alphabetical. It is the order the
 *       mapper itself writes its fields in.</li>
 * </ul>
 *
 * <p>So sorting the field dictionaries on the way out would match a simple mapping and still diverge on
 * any mapping carrying more than {@code properties} — while looking, to anyone diffing, as though the
 * order were faithful. Reproducing it means the mapping engine, not a sort. Left verbatim and recorded
 * as a considered divergence, which is the honest version of the same outcome.
 */
public final class GetMappingHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.get_mapping";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        IndexExpressions.Resolved resolved =
            IndexExpressions.resolve(request, context.store(), IndicesOptions.strictExpandOpen());
        if (resolved.failed()) {
            return resolved.failure();
        }

        IndexStore indices = new IndexStore(context.store());
        ObjectNode body = Json.object();
        resolved.indices().stream().sorted().forEach(index -> indices.get(index).ifPresent(metadata -> {
            JsonNode mappings = metadata.path("mappings");
            ObjectNode entry = Json.object();
            entry.set("mappings", mappings.isObject() ? mappings.deepCopy() : Json.object());
            body.set(index, entry);
        }));
        return Json.response(RestStatus.OK, body, request);
    }
}
