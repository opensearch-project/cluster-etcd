/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.util.NavigableMap;

/**
 * Reads templates, for any of the three kinds.
 *
 * <p>The response shape splits by kind, verified against a live cluster:
 *
 * <ul>
 *   <li>Legacy answers a <b>name-keyed map</b>: {@code {"my-template": {...}}}, and a miss is a
 *       {@code 404} whose body is a bare {@code {}} — the same odd pairing as
 *       {@code GET /_ingest/pipeline}.</li>
 *   <li>The newer kinds answer an <b>array under a wrapper key</b>:
 *       {@code {"index_templates": [{"name": "x", "index_template": {...}}]}}, and a miss is a
 *       structured {@code resource_not_found_exception} naming the kind.</li>
 * </ul>
 */
public final class GetTemplateHandler implements OperationHandler {

    private final TemplateKind kind;

    GetTemplateHandler(TemplateKind kind) {
        this.kind = kind;
    }

    @Override
    public String operationGroup() {
        return kind.getGroup();
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        String name = request.pathParam("name");
        TemplateStore templates = new TemplateStore(context.store(), kind);
        NavigableMap<String, ObjectNode> matched = templates.resolve(name);

        if (matched.isEmpty() && name != null) {
            return kind.isNameKeyedResponse()
                // Legacy: a 404 carrying an empty object rather than an error document.
                ? Json.response(RestStatus.NOT_FOUND, Json.object(), request)
                : ErrorResponses.templateNotFound(request, kind.notFoundNoun(), name);
        }

        // Settings are stored flat and nested on the way out, except where flat_settings says otherwise
        // and the kind honours it. See TemplateStore.rendered.
        boolean flatSettings = request.paramAsBoolean("flat_settings", false);

        if (kind.isNameKeyedResponse()) {
            ObjectNode body = Json.object();
            matched.forEach((templateName, definition) -> body.set(templateName, templates.rendered(definition, flatSettings)));
            return Json.response(RestStatus.OK, body, request);
        }

        ArrayNode items = Json.array();
        matched.forEach((templateName, definition) -> {
            ObjectNode item = Json.object();
            item.put("name", templateName);
            item.set(kind.itemKey(), templates.rendered(definition, flatSettings));
            items.add(item);
        });
        ObjectNode body = Json.object();
        body.set(kind.wrapperKey(), items);
        return Json.response(RestStatus.OK, body, request);
    }
}
