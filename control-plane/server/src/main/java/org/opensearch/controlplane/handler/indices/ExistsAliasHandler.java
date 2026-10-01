/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

/**
 * {@code HEAD /_alias/{name}} and {@code HEAD /{index}/_alias/{name}} — whether an alias exists.
 *
 * <p>The same lookup as {@code get_alias}, reduced to a status. Both routes always carry an alias name,
 * so the "no filter" branch of {@link AliasLookup} never applies here and an empty result is always a
 * 404.
 *
 * <p>Scoping by index matters: {@code HEAD /cp-a/_alias/cp-x} is a 404 when {@code cp-x} is on
 * {@code cp-b}, even though both the index and the alias exist. Verified.
 */
public final class ExistsAliasHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.exists_alias";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        var result = AliasLookup.lookup(
            request, context.store(), request.pathParam("index"), request.pathParam("name"));

        boolean found = result.isPresent() && result.get().anyAliasMatched();
        return RestResponse.empty(found ? RestStatus.OK : RestStatus.NOT_FOUND);
    }
}
