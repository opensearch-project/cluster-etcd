/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

/**
 * {@code GET /_alias}, {@code /_alias/{name}}, {@code /{index}/_alias} and
 * {@code /{index}/_alias/{name}} — reports which indices carry which aliases.
 *
 * <p>See {@link AliasLookup} for the two rules that shape this: responses are index-keyed even when the
 * request names an alias, and whether an alias name was supplied decides both which indices appear and
 * whether an empty result is a 404.
 *
 * <p>The missing-alias error is the <b>bare-string</b> shape — {@code {"error":"alias [x] missing",
 * "status":404}} — with singular wording and no exception type. A {@code DELETE} of the same missing
 * alias answers structured {@code aliases_not_found_exception} with plural wording instead. One
 * condition, two shapes, depending on the code path; verified, and the third time this pattern has come
 * up in this API.
 */
public final class GetAliasHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.get_alias";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        String index = request.pathParam("index");
        String alias = request.pathParam("name");

        var result = AliasLookup.lookup(request, context.store(), index, alias);
        if (result.isEmpty()) {
            return ErrorResponses.indexNotFound(request, index);
        }

        if (alias != null && result.get().anyAliasMatched() == false) {
            return ErrorResponses.aliasNotFoundOnRead(request, alias);
        }

        ObjectNode body = Json.object();
        result.get().byIndex().forEach(body::set);
        return Json.response(RestStatus.OK, body, request);
    }
}
