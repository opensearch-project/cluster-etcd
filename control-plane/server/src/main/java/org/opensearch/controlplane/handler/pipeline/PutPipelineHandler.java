/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

import java.io.IOException;

/**
 * {@code PUT /_ingest/pipeline/{id}} and {@code PUT /_search/pipeline/{id}} — creates or updates a
 * pipeline definition.
 *
 * <p>An unconditional upsert, matching OpenSearch: putting over an existing pipeline replaces it and
 * still returns {@code 200 {"acknowledged": true}}.
 *
 * <p>The two kinds differ in exactly one way. Ingest requires {@code processors} even though the
 * spec's {@code Pipeline} schema does not mark it required — a live cluster rejects {@code {}} with
 * {@code [processors] required property is missing}, which is a spec bug worth reporting upstream. A
 * search pipeline has three independently optional processor lists, so {@code {}} is accepted. Both
 * verified against OpenSearch 3.9.0.
 */
public final class PutPipelineHandler implements OperationHandler {

    private final PipelineKind kind;

    public PutPipelineHandler(PipelineKind kind) {
        this.kind = kind;
    }

    @Override
    public String operationGroup() {
        return kind.putGroup();
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        if (request.hasBody() == false) {
            return ErrorResponses.parseFailure(request, "request body or source parameter is required");
        }

        JsonNode definition;
        try {
            definition = Json.parse(request.body());
        } catch (IOException e) {
            return ErrorResponses.parseFailure(request, "failed to parse pipeline definition: " + e.getMessage());
        }
        if (definition == null || definition.isObject() == false) {
            return ErrorResponses.parseFailure(request, "pipeline definition must be a JSON object");
        }
        if (kind.hasRequiredProperty() && definition.hasNonNull(kind.requiredProperty()) == false) {
            return ErrorResponses.missingRequiredProperty(request, kind.requiredProperty());
        }

        // Stored canonically rather than verbatim so that output formatting is a function of ?pretty
        // alone, not of however the client happened to indent the request.
        new PipelineDefinitions(context.store(), kind)
            .put(request.pathParam("id"), Json.toBytes(definition));
        return Json.acknowledged(request);
    }
}
