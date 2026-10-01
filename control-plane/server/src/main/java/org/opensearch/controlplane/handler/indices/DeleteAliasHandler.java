/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

import java.util.List;

/**
 * {@code DELETE /{index}/_alias/{name}} — removes one alias from one or more indices.
 *
 * <p>A single {@code remove} action through {@link AliasActions}, so a missing alias is a
 * {@code 404 aliases_not_found_exception}. Worth noting that a <em>read</em> of the same missing alias
 * reports it through a different shape entirely — see {@link org.opensearch.controlplane.rest.ErrorResponses}.
 */
public final class DeleteAliasHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.delete_alias";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        ObjectNode remove = Json.object();
        remove.put("index", request.pathParam("index"));
        remove.put("alias", request.pathParam("name"));

        ObjectNode action = Json.object();
        action.set("remove", remove);
        return new AliasActions(context.store()).apply(request, List.of(action))
            .orElseGet(() -> Json.acknowledged(request));
    }
}
