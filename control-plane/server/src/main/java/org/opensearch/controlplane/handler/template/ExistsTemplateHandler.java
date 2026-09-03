/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

/** Whether a template exists, for any of the three kinds. Status only; HEAD carries no body. */
public final class ExistsTemplateHandler implements OperationHandler {

    private final TemplateKind kind;

    ExistsTemplateHandler(TemplateKind kind) {
        this.kind = kind;
    }

    @Override
    public String operationGroup() {
        return kind.existsGroup();
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        boolean found = new TemplateStore(context.store(), kind)
            .resolve(request.pathParam("name")).isEmpty() == false;
        return RestResponse.empty(found ? RestStatus.OK : RestStatus.NOT_FOUND);
    }
}
