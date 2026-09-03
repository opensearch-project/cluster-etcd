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
 * <p>For composable templates, a put is rejected when its patterns overlap another template's at the
 * same priority — there would be no rule for which applies. Overlap at different priorities is allowed.
 *
 * <p>Still not validated: whether {@code composed_of} names component templates that exist. A missing
 * component is silently skipped during composition rather than rejected here.
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

        String name = request.pathParam("name");
        ObjectNode definition = (ObjectNode) parsed;

        // Composable templates may not overlap another template's patterns at the same priority: with
        // equal priority there would be no rule for which one wins. Overlap at *different* priorities is
        // fine, since the higher one simply applies. Verified against a live cluster.
        if (kind.equals(TemplateKind.index())) {
            var conflicts = new TemplateComposition(context.store()).conflictingTemplates(name, definition);
            if (conflicts.isEmpty() == false) {
                return ErrorResponses.conflictingTemplatePriority(
                    request,
                    name,
                    TemplateComposition.patternsOf(definition),
                    conflicts,
                    definition.path("priority").asInt(0)
                );
            }
        }

        new TemplateStore(context.store(), kind).put(name, definition);
        return Json.acknowledged(request);
    }

    private RestResponse missingRequired(RestRequest request) {
        return kind.legacyValidation()
            ? ErrorResponses.legacyTemplateValidationFailed(request, "index patterns are missing")
            : ErrorResponses.requiredProperty(request, kind.requiredProperty());
    }
}
