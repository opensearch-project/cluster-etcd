/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

/**
 * {@code GET /_cluster/settings} — the settings a client has set, by scope.
 *
 * <p>{@code {"persistent": {...}, "transient": {...}}}, and <b>both keys are always present</b>, empty
 * as {@code {}} — a cluster that has never had a setting applied answers
 * {@code {"persistent":{},"transient":{}}} rather than an empty object. Verified against 3.9.0.
 *
 * <p>{@code flat_settings=true} switches both blocks to dotted keys, as everywhere else.
 *
 * <p>{@code include_defaults} is accepted and ignored, for the reason given on
 * {@code GetSettingsHandler}: the control plane owns the settings a user explicitly set, and computed
 * values belong to the layer that computes them. It is a considered divergence, not a gap.
 */
public final class GetClusterSettingsHandler implements OperationHandler {

    @Override
    public String operationGroup() {
        return "cluster.get_settings";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        boolean flatSettings = request.paramAsBoolean("flat_settings", false);
        ClusterSettingsStore.Current current = new ClusterSettingsStore(context.store()).read();

        ObjectNode body = Json.object();
        body.set(ClusterSettingsStore.PERSISTENT, current.persistent().toJson(flatSettings));
        body.set(ClusterSettingsStore.TRANSIENT, current.transientSettings().toJson(flatSettings));
        return Json.response(RestStatus.OK, body, request);
    }
}
