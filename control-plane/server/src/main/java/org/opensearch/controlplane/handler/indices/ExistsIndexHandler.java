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
 * {@code HEAD /{index}} — whether an index exists.
 *
 * <p>Status only, never a body: HEAD carries no payload, so the not-found case is a bare 404 rather
 * than the {@code index_not_found_exception} a GET or DELETE would return. Implemented alongside
 * create because a create you cannot check is not much use.
 *
 * <p>Does not yet resolve aliases or wildcards, both of which a real cluster accepts here.
 */
public final class ExistsIndexHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "indices.exists";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        boolean exists = new IndexStore(context.store()).exists(request.pathParam("index"));
        return RestResponse.empty(exists ? RestStatus.OK : RestStatus.NOT_FOUND);
    }
}
