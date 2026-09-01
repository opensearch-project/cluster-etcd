/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.handler.OperationContext;

import java.io.IOException;

/**
 * {@code PUT /_ingest/pipeline/{id}} — creates or updates a pipeline definition.
 *
 * <p>An unconditional upsert, matching OpenSearch: putting over an existing pipeline replaces it
 * and still returns {@code 200 {"acknowledged": true}}.
 *
 * <p><b>Spec divergence:</b> {@code processors} is required, but the spec's {@code Pipeline} schema
 * does not mark it so — it lists {@code description}, {@code on_failure}, {@code processors},
 * {@code version} and {@code _meta} with no {@code required} block. A live cluster rejects
 * {@code {}} with {@code 400 parse_exception} / {@code [processors] required property is missing},
 * which comes from {@code ConfigurationUtils.readList} in {@code Pipeline.create}. Verified against
 * OpenSearch 3.9.0-SNAPSHOT. This is a spec bug worth reporting upstream; the server behaviour is
 * what gets implemented here.
 */
public final class PutPipelineHandler implements OperationHandler {

    /** Required by the server despite being optional in the spec schema. */
    private static final String PROCESSORS = "processors";


    @Override
    public String operationGroup() {
        return "ingest.put_pipeline";
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
        if (definition.hasNonNull(PROCESSORS) == false) {
            return ErrorResponses.missingRequiredProperty(request, PROCESSORS);
        }

        // Stored canonically rather than verbatim so that output formatting is a function of
        // ?pretty alone, not of however the client happened to indent the request.
        new PipelineStore(context.store()).put(request.pathParam("id"), Json.toBytes(definition));
        return Json.acknowledged(request);
    }
}
