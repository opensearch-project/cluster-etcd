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
import java.util.List;

/**
 * {@code PUT|POST /{index}/_alias/{name}} and its ten aliased routes — adds one alias.
 *
 * <p>A single {@code add} action, so the whole operation goes through {@link AliasActions} and inherits
 * its validation. That is how OpenSearch implements it too.
 *
 * <p>The eleven routes are not all equivalent in where they take their arguments: {@code PUT /_alias} and
 * {@code PUT /{index}/_alias} have no {@code name} in the path, and {@code PUT /_alias/{name}} has no
 * {@code index}. Both fall back to the request body, which may carry {@code index} and {@code alias}
 * alongside the filter and routing properties.
 */
public final class PutAliasHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.put_alias";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        ObjectNode body = Json.object();
        if (request.hasBody()) {
            JsonNode parsed;
            try {
                parsed = Json.parse(request.body());
            } catch (IOException e) {
                return ErrorResponses.parseFailure(request, "failed to parse alias definition: " + e.getMessage());
            }
            if (parsed != null && parsed.isObject()) {
                body = (ObjectNode) parsed;
            }
        }

        // Path wins over body, since a route that has the placeholder is the more specific statement.
        String index = request.pathParam("index");
        String alias = request.pathParam("name");

        ObjectNode add = body.deepCopy();
        if (index != null) {
            add.put("index", index);
        }
        if (alias != null) {
            add.put("alias", alias);
        }
        if (add.hasNonNull("index") == false) {
            return ErrorResponses.structured(
                org.opensearch.controlplane.rest.RestStatus.BAD_REQUEST,
                "illegal_argument_exception",
                "[add] requires an index",
                request
            );
        }
        if (add.hasNonNull("alias") == false) {
            return ErrorResponses.structured(
                org.opensearch.controlplane.rest.RestStatus.BAD_REQUEST,
                "illegal_argument_exception",
                "[add] requires an alias",
                request
            );
        }

        ObjectNode action = Json.object();
        action.set("add", add);
        return new AliasActions(context.store()).apply(request, List.of(action))
            .orElseGet(() -> Json.acknowledged(request));
    }
}
