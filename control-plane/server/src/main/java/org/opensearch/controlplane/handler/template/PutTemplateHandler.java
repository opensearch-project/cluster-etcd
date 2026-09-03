/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

import java.io.IOException;

/**
 * Creates or updates a template, for any of the three kinds.
 *
 * <p>An upsert; a real cluster replaces an existing template and still answers
 * {@code {"acknowledged": true}}.
 *
 * <p>The required property and the error for omitting it both depend on the kind — see
 * {@link TemplateKind}. Legacy templates validate through a different path and report
 * {@code action_request_validation_exception} where the newer kinds report
 * {@code Required [index_patterns]}.
 *
 * <p>Not validated: whether {@code composed_of} names component templates that exist, and whether
 * priorities overlap ambiguously with other templates. Both are cross-resource checks that belong with
 * template composition, which is not implemented.
 */
public final class PutTemplateHandler implements OperationHandler {

    private final TemplateKind kind;

    PutTemplateHandler(TemplateKind kind) {
        this.kind = kind;
    }

    @Override
    public String operationGroup() {
        return kind.putGroup();
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        if (request.hasBody() == false) {
            return missingRequired(request);
        }

        JsonNode parsed;
        try {
            parsed = Json.parse(request.body());
        } catch (IOException e) {
            return ErrorResponses.parseFailure(request, "failed to parse template: " + e.getMessage());
        }
        if (parsed == null || parsed.isObject() == false) {
            return ErrorResponses.parseFailure(request, "template must be a JSON object");
        }
        if (parsed.hasNonNull(kind.requiredProperty()) == false) {
            return missingRequired(request);
        }

        new TemplateStore(context.store(), kind).put(request.pathParam("name"), (ObjectNode) parsed);
        return Json.acknowledged(request);
    }

    private RestResponse missingRequired(RestRequest request) {
        return kind.legacyValidation()
            ? ErrorResponses.legacyTemplateValidationFailed(request, "index patterns are missing")
            : ErrorResponses.requiredProperty(request, kind.requiredProperty());
    }
}
