/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.ingest;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.handler.OperationContext;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

/**
 * {@code GET /_ingest/pipeline} and {@code GET /_ingest/pipeline/{id}} — returns pipeline
 * definitions.
 *
 * <p>The response body is a bare map of ID to definition with no envelope, per the spec's
 * {@code additionalProperties: Pipeline} response schema.
 *
 * <p>The ID is a comma-separated list and supports {@code *} wildcards; omitting it returns
 * everything.
 *
 * <p><b>Unverified:</b> when nothing matches, this returns {@code 404} with an empty object.
 * That follows OpenSearch's {@code GetPipelineResponse.status()}, which is
 * {@code isFound() ? OK : NOT_FOUND} — meaning even a wildcard that matches nothing, and even
 * a bare {@code GET /_ingest/pipeline} on a cluster with no pipelines, is a 404. The spec
 * declares only a 200 for this operation, so the 404 is not documented and needs confirming
 * against a live cluster.
 */
public final class GetPipelineHandler implements OperationHandler {


    @Override
    public String operationGroup() {
        return "ingest.get_pipeline";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) throws IOException {
        // Null for GET /_ingest/pipeline, which has no {id} placeholder to bind.
        NavigableMap<String, byte[]> matched =
            new PipelineStore(context.store()).resolve(request.pathParam("id"));

        ObjectNode body = Json.object();
        for (Map.Entry<String, byte[]> pipeline : matched.entrySet()) {
            body.set(pipeline.getKey(), Json.parse(pipeline.getValue()));
        }

        return Json.response(matched.isEmpty() ? RestStatus.NOT_FOUND : RestStatus.OK, body, request);
    }
}
