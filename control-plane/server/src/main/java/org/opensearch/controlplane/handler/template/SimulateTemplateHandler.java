/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.io.IOException;

/**
 * {@code POST /_index_template/_simulate[/{name}]} and
 * {@code POST /_index_template/_simulate_index/{name}} — shows what composition produces without
 * creating anything.
 *
 * <p>Nearly free once {@link TemplateComposition} exists, and worth having for its own sake: these are
 * pure functions over the composition engine, so they are the cheapest way to check the port is right.
 * Every precedence rule the engine implements was in fact established by probing {@code _simulate_index}
 * against a live cluster.
 *
 * <p>The two operations differ in what they are given. {@code simulate_index_template} takes an
 * <em>index name</em> and reports the template that would apply to it. {@code simulate_template} takes a
 * <em>template name</em>, or a template body to try before saving it.
 */
public final class SimulateTemplateHandler implements OperationHandler {

    private final boolean byIndexName;

    SimulateTemplateHandler(boolean byIndexName) {
        this.byIndexName = byIndexName;
    }

    @Override
    public String operationGroup() {
        return byIndexName ? "indices.simulate_index_template" : "indices.simulate_template";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        TemplateComposition composition = new TemplateComposition(context.store());
        String name = request.pathParam("name");

        ObjectNode template;
        if (byIndexName) {
            var match = composition.findMatching(name);
            if (match.isEmpty()) {
                return ErrorResponses.templateNotFound(request, "index template", name);
            }
            template = match.get().template();
        } else if (request.hasBody()) {
            // A body means "simulate this template as though it were saved".
            JsonNode parsed;
            try {
                parsed = Json.parse(request.body());
            } catch (IOException e) {
                return ErrorResponses.parseFailure(request, "failed to parse template: " + e.getMessage());
            }
            if (parsed == null || parsed.isObject() == false) {
                return ErrorResponses.parseFailure(request, "template must be a JSON object");
            }
            template = (ObjectNode) parsed;
        } else {
            var stored = new TemplateStore(context.store(), TemplateKind.index()).get(name);
            if (stored.isEmpty()) {
                return ErrorResponses.templateNotFound(request, "index template", name);
            }
            template = stored.get();
        }

        ObjectNode body = Json.object();
        body.set("template", composition.compose(template).toJson());
        return Json.response(RestStatus.OK, body, request);
    }
}
