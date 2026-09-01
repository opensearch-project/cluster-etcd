/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

import com.fasterxml.jackson.databind.node.ObjectNode;

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
     */
    public static RestResponse noHandler(String method, String uri, boolean pretty) {
        ObjectNode root = Json.object();
        root.put("error", "no handler found for uri [" + uri + "] and method [" + method + "]");
        return RestResponse.json(RestStatus.BAD_REQUEST, Json.write(root, pretty));
    }

    /**
     * The path is known but not for this method.
     *
     * <p>Verified: 405 with a comma-separated {@code Allow} header, a capitalised message, and a
     * {@code status} field present.
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

    /** An unexpected exception escaped a handler. */
    public static RestResponse internalError(Throwable cause, boolean pretty) {
        ObjectNode node = Json.object();
        node.put("type", exceptionType(cause));
        node.put("reason", cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage());
        return structured(RestStatus.INTERNAL_SERVER_ERROR, node, pretty, Map.of());
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
