/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler;

import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

/** Returns 501 for any operation group that has no real handler yet. */
public final class NotImplementedHandler implements OperationHandler {

    private final String operationGroup;

    public NotImplementedHandler(String operationGroup) {
        this.operationGroup = operationGroup;
    }

    @Override
    public String operationGroup() {
        return operationGroup;
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        return ErrorResponses.notImplemented(request);
    }
}
