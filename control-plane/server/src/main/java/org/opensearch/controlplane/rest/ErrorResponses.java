/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builders for OpenSearch-shaped error bodies.
 *
 * <p>OpenSearch emits <em>three</em> distinct error shapes, and which one you get depends on how far
 * the request got. All three were verified against OpenSearch 3.9.0-SNAPSHOT.
 *
 * <ul>
 *   <li><b>Structured</b> — {@code {"error": {"root_cause": [...], "type": ..., "reason": ...},
 *       "status": N}}. Every failure raised by an actual handler. Note the {@code root_cause} array,
 *       which repeats the top-level cause including any extra fields such as
 *       {@code property_name}.</li>
 *   <li><b>Bare string with status</b> — {@code {"error": "some text", "status": N}}. Produced by
 *       {@code RestController.handleUnsupportedHttpMethod} via
 *       {@code BytesRestResponse(RestStatus, String)}, i.e. the 405 path.</li>
 *   <li><b>Bare string, no status</b> — {@code {"error": "some text"}}. Produced by
 *       {@code RestController.handleBadRequest}, which hand-builds the object with only an
 *       {@code error} field. This is the unknown-path 400, and it is the one response in the whole
 *       API with no {@code status} field at all.</li>
 * </ul>
 *
 * <p>Clients that assume {@code error} is always an object, or that {@code status} is always
 * present, trip over these. Reproducing them is part of being compatible.
 */
public final class ErrorResponses {

    /** OpenSearch's sentinel for "no UUID applies here". A literal string, not a placeholder. */
    private static final String NOT_APPLICABLE = "_na_";

    private ErrorResponses() {}

    /**
     * The response every generated-but-unimplemented route returns.
     *
     * <p>OpenSearch itself never returns 501 — every route it registers is implemented — so this
     * shape is ours. It uses the structured form and adds {@code operation_group}, which is legal
     * because the spec's {@code ErrorCause} schema permits additional properties.
     */
    public static RestResponse notImplemented(RestRequest request) {
        ObjectNode cause = Json.object();
        cause.put("type", "not_implemented_exception");
        cause.put(
            "reason",
            "operation [" + request.operationGroup() + "] is routed but not implemented by the control plane"
        );
        cause.put("operation_group", request.operationGroup());
        return structured(RestStatus.NOT_IMPLEMENTED, cause, isPretty(request), Map.of());
    }

    /**
     * No registered route serves this path at all.
     *
     * <p>Verified: OpenSearch answers 400 (not 404), the message is lower-case, and the body has
     * <em>no</em> {@code status} field — unlike every other error response.
     *
     * <p>Takes the <b>path</b>, not the full request target: this message omits the query string where
     * {@link #methodNotAllowed} includes it. The two come from different places in
     * {@code RestController} — the same reason one carries a {@code status} field and the other does
     * not, and one is capitalised and the other is not. Easy to miss, because a request with no query
     * parameters makes them look identical.
     *
     * @param rawPath the request path with the query string already stripped
     */
    public static RestResponse noHandler(String method, String rawPath, boolean pretty) {
        ObjectNode root = Json.object();
        root.put("error", "no handler found for uri [" + rawPath + "] and method [" + method + "]");
        return RestResponse.json(RestStatus.BAD_REQUEST, Json.write(root, pretty));
    }

    /**
     * The path is known but not for this method.
     *
     * <p>Verified: 405 with a comma-separated {@code Allow} header, a capitalised message, and a
     * {@code status} field present. Unlike {@link #noHandler}, this message includes the query string.
     */
    public static RestResponse methodNotAllowed(String method, String uri, Set<String> allowedMethods, boolean pretty) {
        String allowed = String.join(",", allowedMethods);
        return bareString(
            RestStatus.METHOD_NOT_ALLOWED,
            "Incorrect HTTP method for uri [" + uri + "] and method [" + method + "], allowed: [" + allowed + "]",
            pretty,
            Map.of("Allow", allowed)
        );
    }

    /**
     * {@code source} supplied without {@code source_content_type}.
     *
     * <p>Verified against OpenSearch 3.9.0, including the status: this is a <b>500</b>
     * {@code illegal_state_exception}, not the 400 a malformed request would normally get.
     */
    public static RestResponse sourceParametersRequired(boolean pretty) {
        return structured(
            RestStatus.INTERNAL_SERVER_ERROR,
            "illegal_state_exception",
            "source and source_content_type parameters are required",
            pretty
        );
    }

    /** An unexpected exception escaped a handler. */
    public static RestResponse internalError(Throwable cause, boolean pretty) {
        return internalError(cause, pretty, false);
    }

    /**
     * An unexpected exception escaped a handler.
     *
     * <h2>Which errors carry a stack trace, and why only those</h2>
     *
     * <p>A real cluster can trace any error, because in OpenSearch every error <em>is</em> a thrown
     * exception. Here most are not: {@code indexNotFound} and the validation failures build a response
     * directly, because a rejected index name is an expected outcome rather than an exceptional one.
     *
     * <p>So {@code error_trace=true} yields a trace when an exception genuinely escaped a handler, and
     * nothing when the failure was explicitly checked for. That is the intended behaviour, not a gap
     * awaiting a refactor:
     *
     * <ul>
     *   <li>{@code error_trace} is a debugging aid for a human reading a response, not a signal a
     *       production client acts on — the same family as {@code human}. Where it helps is exactly
     *       where something unforeseen happened, which is the case that does carry a trace.</li>
     *   <li>Throwing and catching an exception per validation failure would put that cost on an
     *       ordinary path, and the resulting trace would point at our own validation code rather than
     *       at anything the caller could act on. The {@code reason} already says precisely what was
     *       wrong.</li>
     * </ul>
     *
     * @param errorTrace whether {@code ?error_trace} asked for a stack trace
     */
    public static RestResponse internalError(Throwable cause, boolean pretty, boolean errorTrace) {
        ObjectNode node = Json.object();
        node.put("type", exceptionType(cause));
        node.put("reason", cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage());
        if (errorTrace) {
            node.put("stack_trace", stackTraceOf(cause));
        }
        return structured(RestStatus.INTERNAL_SERVER_ERROR, node, pretty, Map.of());
    }

    private static String stackTraceOf(Throwable cause) {
        java.io.StringWriter writer = new java.io.StringWriter();
        cause.printStackTrace(new java.io.PrintWriter(writer));
        return writer.toString();
    }

    public static RestResponse structured(RestStatus status, String type, String reason, boolean pretty) {
        ObjectNode cause = Json.object();
        cause.put("type", type);
        cause.put("reason", reason);
        return structured(status, cause, pretty, Map.of());
    }

    /** Structured error, taking the {@code ?pretty} flag from the request. */
    public static RestResponse structured(RestStatus status, String type, String reason, RestRequest request) {
        return structured(status, type, reason, isPretty(request));
    }

    /** {@code 404} with {@code resource_not_found_exception}, as OpenSearch uses for a missing named resource. */
    public static RestResponse resourceNotFound(RestRequest request, String reason) {
        return structured(RestStatus.NOT_FOUND, "resource_not_found_exception", reason, request);
    }

    /**
     * {@code 400} with {@code parse_exception}, as OpenSearch uses for a missing or unparseable
     * request body.
     */
    public static RestResponse parseFailure(RestRequest request, String reason) {
        return structured(RestStatus.BAD_REQUEST, "parse_exception", reason, request);
    }

    /**
     * {@code 400} for a required property absent from a request body.
     *
     * <p>Verified shape: {@code parse_exception}, reason {@code [x] required property is missing},
     * plus a {@code property_name} field carrying the property. OpenSearch uses this from
     * {@code ConfigurationUtils} across many APIs, and the extra field appears both at the top level
     * and inside {@code root_cause}.
     */
    public static RestResponse missingRequiredProperty(RestRequest request, String propertyName) {
        ObjectNode cause = Json.object();
        cause.put("type", "parse_exception");
        cause.put("reason", "[" + propertyName + "] required property is missing");
        cause.put("property_name", propertyName);
        return structured(RestStatus.BAD_REQUEST, cause, isPretty(request), Map.of());
    }

    /**
     * {@code 404} for an index that does not exist.
     *
     * <p>Field set and order verified against OpenSearch 3.9.0: {@code index}, {@code resource.id},
     * {@code resource.type}, {@code index_uuid}. The dotted keys are literal field names, not nesting.
     * {@code index_or_alias} is what the server reports even for a path that can only be an index.
     */
    public static RestResponse indexNotFound(RestRequest request, String index) {
        ObjectNode cause = Json.object();
        cause.put("type", "index_not_found_exception");
        cause.put("reason", "no such index [" + index + "]");
        cause.put("index", index);
        cause.put("resource.id", index);
        cause.put("resource.type", "index_or_alias");
        cause.put("index_uuid", NOT_APPLICABLE);
        return structured(RestStatus.NOT_FOUND, cause, isPretty(request), Map.of());
    }

    /**
     * {@code 400} for a create against a name that is already taken.
     *
     * <p>Note the reason embeds the existing index's UUID — {@code index [name/uuid] already
     * exists} — so the UUID is client-visible and has to be the one actually stored, not a freshly
     * generated one.
     */
    public static RestResponse indexAlreadyExists(RestRequest request, String index, String indexUuid) {
        ObjectNode cause = Json.object();
        cause.put("type", "resource_already_exists_exception");
        cause.put("reason", "index [" + index + "/" + indexUuid + "] already exists");
        cause.put("index", index);
        cause.put("index_uuid", indexUuid);
        return structured(RestStatus.BAD_REQUEST, cause, isPretty(request), Map.of());
    }

    /**
     * {@code 400} for a name that breaks OpenSearch's index naming rules.
     *
     * <p>{@code index_uuid} is the literal string {@code _na_}, since no index was created.
     *
     * @param reason the rule text, e.g. {@code must be lowercase}
     */
    public static RestResponse invalidIndexName(RestRequest request, String index, String reason) {
        ObjectNode cause = Json.object();
        cause.put("type", "invalid_index_name_exception");
        cause.put("reason", "Invalid index name [" + index + "], " + reason);
        cause.put("index", index);
        cause.put("index_uuid", NOT_APPLICABLE);
        return structured(RestStatus.BAD_REQUEST, cause, isPretty(request), Map.of());
    }

    /**
     * {@code 400} for an ingest processor type the data plane does not have.
     *
     * <p>Verified against OpenSearch 3.9.0: {@code parse_exception}, reason
     * {@code No processor type exists with name [x]} — note the brackets — plus a
     * {@code processor_type} field. Deliberately different from the search pipeline equivalent, which
     * uses a different exception type and different wording for the same condition.
     */
    public static RestResponse unknownIngestProcessor(RestRequest request, String type) {
        ObjectNode cause = Json.object();
        cause.put("type", "parse_exception");
        cause.put("reason", "No processor type exists with name [" + type + "]");
        cause.put("processor_type", type);
        return structured(RestStatus.BAD_REQUEST, cause, isPretty(request), Map.of());
    }

    /**
     * {@code 400} for a search pipeline processor type the data plane does not have.
     *
     * <p>Verified against OpenSearch 3.9.0: {@code illegal_argument_exception}, reason
     * {@code Invalid processor type x} — no brackets, and no extra field.
     */
    public static RestResponse unknownSearchProcessor(RestRequest request, String type) {
        return structured(
            RestStatus.BAD_REQUEST,
            "illegal_argument_exception",
            "Invalid processor type " + type,
            isPretty(request)
        );
    }

    /**
     * {@code 404} for aliases that were asked to be removed but do not exist.
     *
     * <p>Verified against OpenSearch 3.9.0, including the plural wording for a single alias:
     * {@code aliases [x] missing}, with {@code resource.id} and {@code resource.type}. Note that a
     * <em>read</em> of a missing alias reports the same condition through the bare-string shape
     * instead — see {@link #aliasNotFoundOnRead}.
     */
    public static RestResponse aliasesNotFound(RestRequest request, String aliases) {
        ObjectNode cause = Json.object();
        cause.put("type", "aliases_not_found_exception");
        cause.put("reason", "aliases [" + aliases + "] missing");
        cause.put("resource.id", aliases);
        cause.put("resource.type", "aliases");
        return structured(RestStatus.NOT_FOUND, cause, isPretty(request), Map.of());
    }

    /**
     * {@code 404} for reading an alias that does not exist.
     *
     * <p>The same condition as {@link #aliasesNotFound} but reported differently: reads use the
     * bare-string shape with a {@code status} field, singular wording, and no exception type at all.
     * Verified. Third instance of one condition having two shapes depending on the code path.
     */
    public static RestResponse aliasNotFoundOnRead(RestRequest request, String alias) {
        return bareString(
            RestStatus.NOT_FOUND,
            "alias [" + alias + "] missing",
            isPretty(request),
            Map.of()
        );
    }

    /**
     * {@code 400} for an alias name already taken by an index.
     *
     * <p>Verified shape, including that {@code index} names the colliding index rather than the one
     * being aliased.
     */
    public static RestResponse invalidAliasName(RestRequest request, String alias, String index, String indexUuid) {
        ObjectNode cause = Json.object();
        cause.put("type", "invalid_alias_name_exception");
        cause.put("reason", "Invalid alias name [" + alias + "], an index exists with the same name as the alias");
        cause.put("index", index);
        cause.put("index_uuid", indexUuid);
        return structured(RestStatus.BAD_REQUEST, cause, isPretty(request), Map.of());
    }

    /**
     * More than one write index for an alias.
     *
     * <p><b>Considered divergence.</b> OpenSearch answers {@code 500 illegal_state_exception}; this
     * answers {@code 400 illegal_argument_exception} with the same reason. A request that a client got
     * wrong is a client error, and reporting it as a server fault tells the caller to retry something
     * that will never succeed. The type follows the status rather than being left inconsistent with it.
     * Recorded under considered divergences in the README.
     */
    public static RestResponse multipleWriteIndices(RestRequest request, String alias, List<String> writeIndices) {
        return structured(
            RestStatus.BAD_REQUEST,
            "illegal_argument_exception",
            "alias [" + alias + "] has more than one write index [" + String.join(", ", writeIndices) + "]",
            request
        );
    }

    /** {@code 400} for an {@code _aliases} request with no actions. Verified wording. */
    public static RestResponse noActionSpecified(RestRequest request) {
        return structured(RestStatus.BAD_REQUEST, "illegal_argument_exception", "No action specified", request);
    }

    /**
     * {@code 404} for a template GET that matched nothing.
     *
     * @param noun {@code "index template"} or {@code "component template"} — the wording differs per kind
     */
    public static RestResponse templateNotFound(RestRequest request, String noun, String name) {
        return structured(
            RestStatus.NOT_FOUND,
            "resource_not_found_exception",
            noun + " matching [" + name + "] not found",
            request
        );
    }

    /**
     * {@code 404} for deleting a template that is not there.
     *
     * <p>Verified: all three template kinds report this identically, as
     * {@code index_template_missing_exception} with {@code index_template [x] missing} — including
     * component templates, which are neither index templates nor described that way anywhere else.
     */
    public static RestResponse templateMissing(RestRequest request, String name) {
        return structured(
            RestStatus.NOT_FOUND,
            "index_template_missing_exception",
            "index_template [" + name + "] missing",
            request
        );
    }

    /** {@code 400} for a template body missing a required property. Wording verified per kind. */
    public static RestResponse requiredProperty(RestRequest request, String property) {
        return structured(
            RestStatus.BAD_REQUEST,
            "illegal_argument_exception",
            "Required [" + property + "]",
            request
        );
    }

    /**
     * {@code 400} for a legacy template with no index patterns.
     *
     * <p>Legacy templates validate through a different path than the newer kinds and report it
     * differently: {@code action_request_validation_exception} with OpenSearch's accumulating
     * "Validation Failed: 1: ...;" format, rather than {@code Required [index_patterns]}. Verified.
     */
    public static RestResponse legacyTemplateValidationFailed(RestRequest request, String message) {
        return structured(
            RestStatus.BAD_REQUEST,
            "action_request_validation_exception",
            "Validation Failed: 1: " + message + ";",
            request
        );
    }

    /**
     * {@code 400} for a composable template overlapping another at the same priority.
     *
     * <p>Message format follows the server's, which spells out both sides so the operator can see which
     * template to change: patterns, the conflicting names, each of their patterns, and the priority.
     */
    public static RestResponse conflictingTemplatePriority(
        RestRequest request,
        String name,
        List<String> patterns,
        List<String> conflicts,
        int priority
    ) {
        return structured(
            RestStatus.BAD_REQUEST,
            "illegal_argument_exception",
            "index template [" + name + "] has index patterns " + patterns
                + " matching patterns from existing templates [" + String.join(",", conflicts)
                + "] with patterns (" + String.join(", ", conflicts) + " => " + patterns
                + ") that have the same priority [" + priority + "], multiple index templates may not "
                + "match during index creation, please use a different priority",
            request
        );
    }

    /**
     * Wraps a cause in the structured envelope.
     *
     * <p>{@code root_cause} comes first and repeats the cause verbatim, extra fields included, which
     * is what the server does for a failure with no nested cause. Field order matches the server's.
     */
    /**
     * No tenant could be resolved from the request.
     *
     * <p><b>Unverified:</b> the shape mirrors what the OpenSearch security plugin returns for an
     * unauthenticated request, but has not been diffed against a secured cluster. If clients are
     * expected to distinguish "no credentials" from "credentials not mapped to a tenant", that will
     * want splitting into 401 and 403.
     */
    /**
     * {@code 409} for a read-modify-write that lost its compare-and-set.
     *
     * <p><b>This shape is ours.</b> OpenSearch has no equivalent because it serialises metadata updates
     * through the cluster manager, so a concurrent mapping or settings update never reaches the client
     * as a conflict. Here each index is its own key and a writer can lose the race, and reporting that
     * honestly is better than the alternatives: retrying internally hides contention a client should
     * know about, and an unconditional overwrite would silently lose the other update.
     *
     * <p>A client should re-read and retry. The operation is idempotent, so a retry is safe.
     */
    public static RestResponse concurrentUpdate(RestRequest request, String index) {
        ObjectNode cause = Json.object();
        cause.put("type", "version_conflict_engine_exception");
        cause.put("reason", "index [" + index + "] was updated concurrently; re-read and retry");
        cause.put("index", index);
        return structured(RestStatus.CONFLICT, cause, isPretty(request), Map.of());
    }

    /**
     * {@code 400} for a setting the data plane does not have.
     *
     * <p>Verified against 3.9.0. The suffix has two branches, and which one appears depends on whether
     * any registered key scores above {@code 0.7} on Lucene's normalised Levenshtein similarity — see
     * {@code SettingsValidation.didYouMean}. A typo gets the suggestion; a genuinely invented key gets
     * the generic advice.
     *
     * <p>A setting the user may not supply does <b>not</b> report through here — it has its own message
     * on each path; see {@link #privateSettingOnUpdate} and {@link #privateSettingsRejected}. An earlier
     * version of this claimed otherwise on the strength of a probe using {@code index.provided_name},
     * which is absent from the registry rather than private, so it took this path for the ordinary
     * reason.
     *
     * @param suggestion the rendered {@code did you mean} target, or {@code null} for the generic advice
     */
    public static RestResponse unknownSetting(RestRequest request, String key, String suggestion) {
        String reason = "unknown setting [" + key + "]"
            + (suggestion == null
                ? " please check that any required plugins are installed, or check the breaking changes "
                    + "documentation for removed settings"
                : " did you mean " + suggestion + "?");
        return structured(RestStatus.BAD_REQUEST, "settings_exception", reason, request);
    }

    /**
     * {@code 400} for a static setting changed on an open index.
     *
     * <p>Byte-exact against 3.9.0, doubled brackets and all — and they double for two different
     * reasons, which is why this is easy to get wrong. The format is
     * {@code "Can't update non dynamic settings [%s] for open indices %s"}: the settings list gets its
     * brackets from the format string wrapping a {@code List.toString()}, while the index list gets
     * them from {@code Index.toString()} already rendering as {@code [name/uuid]} inside a list.
     *
     * @param indices each already rendered as {@code [name/uuid]}, the way {@code Index.toString} does
     */
    public static RestResponse nonDynamicSettings(RestRequest request, List<String> keys, List<String> indices) {
        return structured(
            RestStatus.BAD_REQUEST,
            "illegal_argument_exception",
            "Can't update non dynamic settings [" + keys + "] for open indices " + indices,
            request
        );
    }

    /**
     * {@code 409} for a weighted routing write whose {@code _version} is not the stored one.
     *
     * <p>{@code WeightedRoutingService.ensureNoVersionConflict}. <b>Note the trailing space</b>, which
     * comes from the format string being written as two concatenated literals — {@code "...is at a "} and
     * {@code "different version %s "} — and is part of the message a client sees.
     *
     * <p>Reached on {@code PUT} and {@code DELETE} alike, and on both the "no weights yet" and "weights
     * exist" paths: with nothing stored the version compared against is {@code -1}, so a {@code DELETE}
     * with no body at all conflicts {@code -2} against {@code -1} rather than reporting nothing to
     * delete. Verified.
     *
     * @param actual the stored version, or {@code -1} when nothing is stored
     */
    public static RestResponse weightedRoutingVersionConflict(RestRequest request, long requested, long actual) {
        return structured(
            RestStatus.CONFLICT,
            "unsupported_weighted_routing_state_exception",
            "requested version is " + requested
                + " but cluster weighted routing metadata is at a different version " + actual + " ",
            request
        );
    }

    /**
     * {@code 409} for weights a real cluster's state will not accept.
     *
     * <p>{@code UnsupportedWeightedRoutingStateException}, which is a 409 rather than the 400 its
     * contents suggest. The one instance this service can raise is a forced awareness value left without
     * a weight; the decommission-related instances belong to a plane that does not exist yet.
     */
    public static RestResponse unsupportedWeightedRoutingState(RestRequest request, String reason) {
        return structured(RestStatus.CONFLICT, "unsupported_weighted_routing_state_exception", reason, request);
    }

    /**
     * {@code 400} for a value that had to be a number and was not.
     *
     * <p>The reason is {@link NumberFormatException}'s own message, quotes included:
     * {@code For input string: "abc"}. It reaches a client unwrapped because OpenSearch parses weights
     * with {@code Double.parseDouble} inside the request parser and lets the exception escape, so this is
     * a raw JDK message rather than one the server composed. Verified against 3.9.0.
     */
    public static RestResponse numberFormat(RestRequest request, String input) {
        return structured(
            RestStatus.BAD_REQUEST,
            "number_format_exception",
            "For input string: \"" + input + "\"",
            request
        );
    }

    /**
     * {@code 400} for a cluster setting that is unknown or cannot be updated.
     *
     * <p>Different wording from the index path, verified: {@code persistent setting [cluster.x], not
     * recognized} and {@code persistent setting [cluster.name], not dynamically updateable}. The scope
     * word is part of the message, so a transient update says {@code transient setting [...]}.
     */
    public static RestResponse clusterSettingRejected(RestRequest request, String scope, String key, String why) {
        return structured(
            RestStatus.BAD_REQUEST,
            "settings_exception",
            scope + " setting [" + key + "], " + why,
            request
        );
    }

    /**
     * {@code 400} for a cluster settings update that sets nothing.
     *
     * <p>Verified: reached by an empty body object and, less obviously, by a body whose only keys are
     * neither {@code persistent} nor {@code transient} — {@code {"bogus":{...}}} gets this rather than
     * an unknown-scope error, because the server simply finds nothing to apply.
     */
    /**
     * {@code 400} for changing a private setting on an existing index.
     *
     * <p>{@code AbstractScopedSettings} line 658. Distinct from the create and template wording, which
     * comes from {@code MetadataCreateIndexService} instead — one condition, three messages, depending
     * on which service reached it first.
     */
    public static RestResponse privateSettingOnUpdate(RestRequest request, String key) {
        return structured(
            RestStatus.BAD_REQUEST,
            "settings_exception",
            "can not update private setting [" + key + "]; this setting is managed by OpenSearch",
            request
        );
    }

    /**
     * {@code 400} for supplying a private setting when creating an index or putting a template.
     *
     * <p>{@code MetadataCreateIndexService} line 1766 collects these into a {@code ValidationException},
     * which renders as {@code Validation Failed: 1: <error>;2: <error>;} — one-based, each terminated by a
     * semicolon and <b>no space between entries</b>. The missing space is easy to add by accident.
     *
     * <p>The template kinds wrap the same text in {@code InvalidIndexTemplateException}:
     * {@code index_template [name] invalid, cause [...]}. All three kinds say {@code index_template},
     * including component and legacy — the same quirk as the DELETE-missing message.
     *
     * @param templateName the template being put, or {@code null} when this is an index create
     */
    public static RestResponse privateSettingsRejected(RestRequest request, List<String> keys, String templateName) {
        // ValidationException.toString: "Validation Failed: " once, then "N: <error>;" per error with no
        // separator between them -- so the second onwards follow the previous semicolon directly, with no
        // space. Verified byte for byte against a two-error response.
        StringBuilder validation = new StringBuilder("Validation Failed: ");
        for (int i = 0; i < keys.size(); i++) {
            validation.append(i + 1).append(": private index setting [")
                .append(keys.get(i)).append("] can not be set explicitly;");
        }
        if (templateName == null) {
            return structured(RestStatus.BAD_REQUEST, "validation_exception", validation.toString(), request);
        }
        return structured(
            RestStatus.BAD_REQUEST,
            "invalid_index_template_exception",
            "index_template [" + templateName + "] invalid, cause [" + validation + "]",
            request
        );
    }

    /**
     * {@code 400} for a {@code Final} setting on a <b>closed</b> index — never updateable, not even then.
     *
     * <p>{@code AbstractScopedSettings} line 937: {@code "final " + type + " setting [" + key + "], not
     * updateable"}, where {@code type} is caller-supplied and is the index name on this path. The same
     * template produces the cluster path's {@code persistent setting [x], not recognized}.
     *
     * <p>Only reached once every target index is closed. While any is open, a real cluster pre-filters
     * everything non-dynamic and reports the "for open indices" message instead, final or not.
     */
    public static RestResponse finalSetting(RestRequest request, String index, String key) {
        return structured(
            RestStatus.BAD_REQUEST,
            "settings_exception",
            "final " + index + " setting [" + key + "], not updateable",
            request
        );
    }

    /** {@code 404} for a snapshot repository that is not configured. Verified on GET and DELETE. */
    public static RestResponse repositoryMissing(RestRequest request, String repository) {
        return structured(
            RestStatus.NOT_FOUND,
            "repository_missing_exception",
            "[" + repository + "] missing",
            request
        );
    }

    /**
     * {@code 400} for a request that failed {@code ActionRequestValidationException}.
     *
     * <p>Same rendering as {@link #noSettingsToUpdate}, which is one instance of it: one-based, each
     * error terminated by a semicolon, and <b>no space between entries</b>. See
     * {@link #privateSettingsRejected} on why that detail is worth stating.
     */
    public static RestResponse validationFailed(RestRequest request, String... errors) {
        StringBuilder validation = new StringBuilder("Validation Failed: ");
        for (int i = 0; i < errors.length; i++) {
            validation.append(i + 1).append(": ").append(errors[i]).append(';');
        }
        return structured(
            RestStatus.BAD_REQUEST,
            "action_request_validation_exception",
            validation.toString(),
            request
        );
    }

    /** {@code 400 illegal_argument_exception} — the general shape for a rejected argument. */
    public static RestResponse illegalArgument(RestRequest request, String reason) {
        return structured(RestStatus.BAD_REQUEST, "illegal_argument_exception", reason, request);
    }

    /**
     * {@code 400 parsing_exception} — a body whose structure is wrong rather than whose values are.
     *
     * <p>A real cluster adds {@code line} and {@code col} from its XContent parser's token location.
     * Those are <b>deliberately absent</b> here: this service parses with a different parser on a
     * different pass, so any numbers it produced would be plausible and wrong, which is worse than
     * absent. The {@code reason} already names the offending field.
     */
    public static RestResponse scriptParsingFailure(RestRequest request, String reason) {
        return structured(RestStatus.BAD_REQUEST, "parsing_exception", reason, request);
    }

    public static RestResponse noSettingsToUpdate(RestRequest request) {
        return structured(
            RestStatus.BAD_REQUEST,
            "action_request_validation_exception",
            "Validation Failed: 1: no settings to update;",
            request
        );
    }

    public static RestResponse tenantUnresolved(boolean pretty) {
        return structured(
            RestStatus.UNAUTHORIZED,
            "security_exception",
            "no tenant could be resolved from the request",
            pretty
        );
    }

    private static RestResponse structured(
        RestStatus status,
        ObjectNode cause,
        boolean pretty,
        Map<String, String> headers
    ) {
        ObjectNode error = Json.object();
        error.putArray("root_cause").add(cause.deepCopy());
        error.setAll(cause);

        ObjectNode root = Json.object();
        root.set("error", error);
        root.put("status", status.code());
        return RestResponse.json(status, Json.write(root, pretty), headers);
    }

    private static RestResponse bareString(
        RestStatus status,
        String message,
        boolean pretty,
        Map<String, String> headers
    ) {
        ObjectNode root = Json.object();
        root.put("error", message);
        root.put("status", status.code());
        return RestResponse.json(status, Json.write(root, pretty), headers);
    }

    /**
     * OpenSearch derives the {@code type} field from the exception class name:
     * {@code IllegalArgumentException} becomes {@code illegal_argument_exception}.
     */
    static String exceptionType(Throwable cause) {
        String simpleName = cause.getClass().getSimpleName();
        StringBuilder snake = new StringBuilder(simpleName.length() + 8);
        for (int i = 0; i < simpleName.length(); i++) {
            char c = simpleName.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    snake.append('_');
                }
                snake.append(Character.toLowerCase(c));
            } else {
                snake.append(c);
            }
        }
        return snake.toString();
    }

    static boolean isPretty(RestRequest request) {
        return request.paramAsBoolean("pretty", false);
    }
}
