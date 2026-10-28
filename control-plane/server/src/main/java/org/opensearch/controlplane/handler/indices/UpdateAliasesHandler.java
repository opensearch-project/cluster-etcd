/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code POST /_aliases} — applies a list of alias actions.
 *
 * <p>The reason the alias data model is what it is. A zero-downtime cutover is expressed here:
 *
 * <pre>{@code
 * {"actions": [{"remove": {"index": "logs-v1", "alias": "logs"}},
 *              {"add":    {"index": "logs-v2", "alias": "logs"}}]}
 * }</pre>
 *
 * <p>Because aliases are one document per alias, both actions land in a single write, so no reader ever
 * sees {@code logs} pointing at nothing. See {@link AliasActions} for the validation rules and for why a
 * request spanning several different aliases is deliberately not atomic as a whole.
 */
public final class UpdateAliasesHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.update_aliases";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        if (request.hasBody() == false) {
            return ErrorResponses.parseFailure(request, "request body or source parameter is required");
        }

        JsonNode body;
        try {
            body = Json.parse(request.body());
        } catch (IOException e) {
            return ErrorResponses.parseFailure(request, "failed to parse actions: " + e.getMessage());
        }
        if (body == null || body.isObject() == false) {
            return ErrorResponses.parseFailure(request, "request body must be a JSON object");
        }

        List<ObjectNode> actions = new ArrayList<>();
        JsonNode declared = body.get("actions");
        if (declared != null && declared.isArray()) {
            for (JsonNode action : declared) {
                if (action.isObject()) {
                    actions.add((ObjectNode) action);
                }
            }
        }

        return new AliasActions(context.store()).apply(request, actions)
            .orElseGet(() -> Json.acknowledged(request));
    }
}
