/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.capability.ScriptLanguages;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

/**
 * {@code GET /_script_language} — which scripting languages the data plane has, and their contexts.
 *
 * <p>A pure passthrough of declared capability data. The control plane cannot know the answer: which
 * languages exist depends on installed modules, so this is a data plane fact in the same family as
 * processor types. What makes it different, and worth implementing, is that it is the only such fact
 * with a first-class source — a cluster exposes this very endpoint, so
 * {@code tools/dump-capabilities.sh} can read it and put it in the capabilities file.
 *
 * <p>Implementing it also closes a gap elsewhere: with the languages declared,
 * {@link StoredScriptHandlers} can reject a stored script whose {@code lang} the data plane does not
 * have, which it previously accepted for want of a list.
 *
 * <h2>Registered only when the answer is known</h2>
 *
 * <p>This handler is registered <b>only if the capabilities file declares script languages</b>;
 * otherwise the operation keeps its {@code 501} stub. That is deliberate, and the alternative was worse.
 * Answering {@code {"types_allowed":[],"language_contexts":[]}} would tell a client it may use no
 * language at all — a confident wrong answer, where the 501 is an honest "this control plane does not
 * implement it". Every other capability-dependent behaviour degrades to *not validating*; this one has
 * nothing to degrade to, because the capability data <em>is</em> the response.
 *
 * <p>So the implemented count depends on configuration for this one operation. Worth knowing rather than
 * hiding: see the README's status table.
 */
public final class GetScriptLanguagesHandler implements OperationHandler {

    private final ScriptLanguages languages;

    public GetScriptLanguagesHandler(ScriptLanguages languages) {
        this.languages = languages;
    }

    @Override
    public String operationGroup() {
        return "get_script_languages";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        ObjectNode body = Json.object();

        ArrayNode typesAllowed = Json.array();
        languages.typesAllowed().forEach(typesAllowed::add);
        body.set("types_allowed", typesAllowed);

        // An array of {language, contexts} objects rather than a map, which is the shape a cluster uses.
        ArrayNode languageContexts = Json.array();
        languages.contextsByLanguage().forEach((language, contexts) -> {
            ObjectNode entry = Json.object();
            entry.put("language", language);
            ArrayNode names = Json.array();
            contexts.forEach(names::add);
            entry.set("contexts", names);
            languageContexts.add(entry);
        });
        body.set("language_contexts", languageContexts);

        return Json.response(RestStatus.OK, body, request);
    }
}
