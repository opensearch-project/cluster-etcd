/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

/**
 * Deletes a template, for any of the three kinds.
 *
 * <p>The one place all three agree: a missing template is
 * {@code 404 index_template_missing_exception} with {@code index_template [x] missing} — including for
 * component templates, which are not index templates and are not described that way anywhere else in
 * the API. Verified for all three.
 */
public final class DeleteTemplateHandler implements OperationHandler {

    private final TemplateKind kind;

    DeleteTemplateHandler(TemplateKind kind) {
        this.kind = kind;
    }

    @Override
    public String operationGroup() {
        return kind.deleteGroup();
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        String name = request.pathParam("name");
        return new TemplateStore(context.store(), kind).delete(name)
            ? Json.acknowledged(request)
            : ErrorResponses.templateMissing(request, name);
    }
}
