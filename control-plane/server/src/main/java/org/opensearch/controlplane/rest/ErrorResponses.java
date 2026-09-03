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
