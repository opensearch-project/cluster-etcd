/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestStatus;

/**
 * {@code HEAD /} — a liveness probe.
 *
 * <p>200 with no body. The router strips bodies from HEAD responses anyway, but returning an empty
 * one keeps that from being load-bearing.
 */
public final class PingHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "ping";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        return RestResponse.empty(RestStatus.OK);
    }
}
