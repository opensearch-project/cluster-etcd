/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.pipeline;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * {@code GET /_ingest/pipeline[/{id}]} and {@code GET /_search/pipeline[/{id}]} — returns pipeline
 * definitions.
 *
 * <p>The body is a bare map of ID to definition with no envelope, per the spec's
 * {@code additionalProperties} response schema. The ID is a comma-separated list supporting {@code *}
 * wildcards; omitting it returns everything.
 *
 * <p>When nothing matches the answer is {@code 404} with an empty object — including for a bare
 * {@code GET} on a cluster with no pipelines at all, and for {@code *} matching nothing. That follows
 * {@code GetPipelineResponse.status()} being {@code isFound() ? OK : NOT_FOUND}. The spec declares
 * only a 200, so the 404 is undocumented; verified against OpenSearch 3.9.0 for both kinds.
 */
public final class GetPipelineHandler implements OperationHandler {

    private final PipelineKind kind;

    public GetPipelineHandler(PipelineKind kind) {
        this.kind = kind;
    }

    @Override
    public String operationGroup() {
        return kind.getGroup();
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) throws IOException {
        // Null for a bare GET, which has no {id} placeholder to bind.
        NavigableMap<String, byte[]> matched =
            new PipelineDefinitions(context.store(), kind).resolve(request.pathParam("id"));

        ObjectNode body = Json.object();
        for (Map.Entry<String, byte[]> pipeline : matched.entrySet()) {
            body.set(pipeline.getKey(), Json.parse(pipeline.getValue()));
        }

        return Json.response(matched.isEmpty() ? RestStatus.NOT_FOUND : RestStatus.OK, body, request);
    }
}
