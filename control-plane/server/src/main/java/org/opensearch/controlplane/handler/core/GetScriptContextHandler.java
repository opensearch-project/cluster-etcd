/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.capability.ScriptContexts;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

/**
 * {@code GET /_script_context} — the scripting contexts the data plane has, with their signatures.
 *
 * <p>A pure passthrough of declared capability data, the same shape as
 * {@link GetScriptLanguagesHandler}: which contexts exist depends on installed modules, a cluster
 * exposes this very endpoint so {@code tools/dump-capabilities.sh} can read it, and this handler is
 * registered <b>only when the capabilities file declares contexts</b>. With none declared the operation
 * keeps its 501, because answering {@code {"contexts":[]}} would tell a client the cluster has no
 * scripting contexts at all — a confident wrong answer where the 501 is honest.
 *
 * <p>Unlike script languages, nothing validates against this. Declaring it closes no gap elsewhere; it
 * only makes the read answerable.
 *
 * <h2>Three orders, none of them a sort</h2>
 *
 * <p>Everything is emitted in the order the capabilities file declares, and that matters more here than
 * it looks:
 *
 * <ul>
 *   <li>Contexts are sorted by name <em>upstream</em>, so this level happens to look alphabetical.</li>
 *   <li>Methods are not sorted. {@code ScriptContextInfo.toXContent} emits {@code execute} first and then
 *       the getters sorted among themselves, so re-sorting would move any getter whose name precedes the
 *       word "execute".</li>
 *   <li>Parameters are the signature — {@code moving-function} takes {@code (params, values)} and
 *       {@code similarity} takes {@code (weight, query, field, term, doc)}. Verified against 3.9.0.</li>
 * </ul>
 *
 * <p><b>Parameter fields are {@code type} then {@code name}</b>, which is the reverse of how the rest of
 * this API reads and of how the capabilities file spells it.
 * {@code ScriptContextInfo.ParameterInfo.toXContent} emits {@code type} first, so this does too.
 */
public final class GetScriptContextHandler implements OperationHandler {

    private final ScriptContexts contexts;

    public GetScriptContextHandler(ScriptContexts contexts) {
        this.contexts = contexts;
    }

    @Override
    public String operationGroup() {
        return "get_script_context";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        ArrayNode rendered = Json.array();
        for (ScriptContexts.Context declared : contexts.contexts()) {
            ObjectNode entry = rendered.addObject();
            entry.put("name", declared.name());
            ArrayNode methods = entry.putArray("methods");
            for (ScriptContexts.Method method : declared.methods()) {
                ObjectNode methodNode = methods.addObject();
                methodNode.put("name", method.name());
                methodNode.put("return_type", method.returnType());
                ArrayNode params = methodNode.putArray("params");
                for (ScriptContexts.Parameter parameter : method.params()) {
                    ObjectNode paramNode = params.addObject();
                    // type before name, as the server emits them.
                    paramNode.put("type", parameter.type());
                    paramNode.put("name", parameter.name());
                }
            }
        }

        ObjectNode body = Json.object();
        body.set("contexts", rendered);
        return Json.response(RestStatus.OK, body, request);
    }
}
