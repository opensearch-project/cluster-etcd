/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

/**
 * Stored scripts: {@code PUT}, {@code GET} and {@code DELETE} on {@code /_scripts/{id}}.
 *
 * <p>The cheapest remaining slice — one key per script, no dependencies, and the same shape as the
 * ingest pipelines that were the first thing implemented here. What is stored is the script's
 * <em>definition</em>: a language, a source string and optional {@code options}. Compiling or running it
 * is the data plane's business, and this service has no scripting engine.
 *
 * <h2>Missing reports two different ways</h2>
 *
 * <p>The now-familiar asymmetry, and the fourth instance of it in this API. Verified against 3.9.0:
 *
 * <ul>
 *   <li><b>{@code GET} a missing script</b> — {@code 404} with an ordinary body,
 *       {@code {"_id":"x","found":false}}. No error document at all, like a document GET.</li>
 *   <li><b>{@code DELETE} a missing script</b> — {@code 404 resource_not_found_exception},
 *       {@code stored script [x] does not exist and cannot be deleted}.</li>
 * </ul>
 *
 * <h2>Validation order matters and is not the obvious one</h2>
 *
 * <p>Verified: {@code {"script":{}}} answers <b>{@code must specify lang}</b>, not
 * {@code must specify source}. Lang is checked first. Guessing the other way round would have produced
 * the wrong message for the commonest malformed body.
 *
 * <p>A body whose first field is not {@code script} is a {@code parsing_exception} carrying the token's
 * position — {@code unexpected field [lang], expected [script]} with {@code "line":1,"col":2}. The
 * position is <b>not</b> reproduced; see the class's divergence note below.
 *
 * <h2>Two deliberate divergences</h2>
 *
 * <p><b>No {@code line} and {@code col} on the parse error.</b> A real cluster reports the XContent
 * parser's token location. Ours come from a different parser over a different pass, so any numbers here
 * would be plausible and wrong, which is worse than absent — the {@code reason} already names the
 * offending field.
 *
 * <p><b>{@code lang} is not validated.</b> A real cluster answers
 * {@code unable to put stored script with unsupported lang [x]}, and which languages exist depends on
 * installed modules. Unlike repository types there <em>is</em> a source for this —
 * {@code GET /_script_language} reports the languages and their contexts — so this is the one capability
 * gap with an obvious fix: that operation is still stubbed, and implementing it would give the
 * capabilities file something to carry. Until then any lang is accepted.
 *
 * <h2>One quirk carried forward on purpose</h2>
 *
 * <p>An <b>empty body object</b> stores a script with {@code lang: "mustache"} and an empty source, and
 * answers {@code 200}. That is not a guess: {@code StoredScriptSource.parse} returns
 * {@code new StoredScriptSource(DEFAULT_TEMPLATE_LANG, "", emptyMap())} for an immediately-closing
 * object, alongside a deprecation warning that "empty templates should no longer be used".
 *
 * <p>Reproduced because it is observable and cheap, not because it is good. When upstream removes the
 * deprecated path this should go with it — the same standing as {@code PipelineDefinitions}' metadata
 * marker.
 *
 * <p>{@code context}, whether in the path or the query string, is accepted and ignored. A real cluster
 * uses it to check the script compiles for that context, which needs the engine this service does not
 * have.
 */
public final class StoredScriptHandlers {

    static final String KEY_PREFIX = "scripts/";

    private static final String SCRIPT = "script";
    private static final String LANG = "lang";
    private static final String SOURCE = "source";
    private static final String OPTIONS = "options";

    /** {@code Script.DEFAULT_TEMPLATE_LANG}, which the deprecated empty-body path falls back to. */
    private static final String DEFAULT_TEMPLATE_LANG = "mustache";

    private StoredScriptHandlers() {}

    public static List<OperationHandler> handlers() {
        return List.of(new Put(), new Get(), new Delete());
    }

    static String key(String id) {
        return KEY_PREFIX + id;
    }

    /** {@code PUT} and {@code POST /_scripts/{id}}, with or without a trailing context. */
    static final class Put implements OperationHandler {

        @Override
        public String operationGroup() {
            return "put_script";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            if (request.hasBody() == false) {
                return ErrorResponses.parseFailure(request, "request body is required");
            }
            JsonNode body;
            try {
                body = Json.parse(request.body());
            } catch (IOException e) {
                return ErrorResponses.parseFailure(request, "failed to parse stored script: " + e.getMessage());
            }
            if (body == null || body.isObject() == false) {
                return ErrorResponses.parseFailure(request, "stored script must be a JSON object");
            }

            ObjectNode definition;
            if (body.isEmpty()) {
                // The deprecated empty-template path; see the class javadoc.
                definition = Json.object();
                definition.put(LANG, DEFAULT_TEMPLATE_LANG);
                definition.put(SOURCE, "");
            } else {
                String firstField = body.properties().iterator().next().getKey();
                if (SCRIPT.equals(firstField) == false) {
                    return ErrorResponses.scriptParsingFailure(
                        request, "unexpected field [" + firstField + "], expected [" + SCRIPT + "]");
                }
                JsonNode script = body.get(SCRIPT);
                if (script == null || script.isObject() == false) {
                    return ErrorResponses.illegalArgument(request, "must specify lang for stored script");
                }
                // Lang before source: verified, and the reverse would misreport {"script":{}}.
                if (script.path(LANG).isTextual() == false) {
                    return ErrorResponses.illegalArgument(request, "must specify lang for stored script");
                }
                if (script.path(SOURCE).isTextual() == false) {
                    return ErrorResponses.illegalArgument(request, "must specify source for stored script");
                }
                definition = Json.object();
                definition.put(LANG, script.get(LANG).asText());
                definition.put(SOURCE, script.get(SOURCE).asText());
                if (script.path(OPTIONS).isObject()) {
                    definition.set(OPTIONS, script.get(OPTIONS).deepCopy());
                }
            }

            context.store().put(key(request.pathParam("id")), Json.toBytes(definition));
            return Json.acknowledged(request);
        }
    }

    /** {@code GET /_scripts/{id}} — {@code found: false} rather than an error when absent. */
    static final class Get implements OperationHandler {

        @Override
        public String operationGroup() {
            return "get_script";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            String id = request.pathParam("id");
            Optional<KeyValueStore.Versioned> stored = context.store().get(key(id));

            ObjectNode body = Json.object();
            body.put("_id", id);
            body.put("found", stored.isPresent());
            if (stored.isEmpty()) {
                return Json.response(RestStatus.NOT_FOUND, body, request);
            }
            body.set(SCRIPT, (ObjectNode) parse(stored.get().value()));
            return Json.response(RestStatus.OK, body, request);
        }
    }

    /** {@code DELETE /_scripts/{id}} — a structured error when absent, unlike the read. */
    static final class Delete implements OperationHandler {

        @Override
        public String operationGroup() {
            return "delete_script";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            String id = request.pathParam("id");
            if (context.store().delete(key(id)) == false) {
                return ErrorResponses.resourceNotFound(
                    request, "stored script [" + id + "] does not exist and cannot be deleted");
            }
            return Json.acknowledged(request);
        }
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt stored script", e);
        }
    }
}
