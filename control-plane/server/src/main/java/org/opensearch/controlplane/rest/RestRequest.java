/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * An inbound request, already routed: {@link #operationGroup()} and
 * {@link #pathParams()} are populated from the matched route.
 *
 * <p>Transport-agnostic on purpose, so the Netty layer can be swapped and so
 * handlers are testable without a socket.
 */
public final class RestRequest {

    private final String method;
    private final String rawPath;
    private final String uri;
    private final Map<String, List<String>> queryParams;
    private final Map<String, String> pathParams;
    private final Map<String, String> headers;
    private final byte[] body;
    private final String operationGroup;

    public RestRequest(
        String method,
        String rawPath,
        String uri,
        Map<String, List<String>> queryParams,
        Map<String, String> pathParams,
        Map<String, String> headers,
        byte[] body,
        String operationGroup
    ) {
        this.method = method.toUpperCase(Locale.ROOT);
        this.rawPath = rawPath;
        this.uri = uri;
        this.queryParams = Map.copyOf(queryParams);
        this.pathParams = Map.copyOf(pathParams);
        this.headers = Map.copyOf(headers);
        this.body = body == null ? new byte[0] : body;
        this.operationGroup = operationGroup;
    }

    public String method() {
        return method;
    }

    /** The path with query string stripped, still percent-encoded. Decoded values live in {@link #pathParams()}. */
    public String rawPath() {
        return rawPath;
    }

    /** The full request target including query string, for error messages. */
    public String uri() {
        return uri;
    }

    public Map<String, List<String>> queryParams() {
        return queryParams;
    }

    /** Placeholder values from the matched route template, e.g. {@code index -> my-index}. */
    public Map<String, String> pathParams() {
        return pathParams;
    }

    /** The {@code x-operation-group} of the matched route, e.g. {@code indices.create}. */
    public String operationGroup() {
        return operationGroup;
    }

    public byte[] body() {
        return body;
    }

    public boolean hasBody() {
        return body.length > 0;
    }

    public String bodyAsString() {
        return new String(body, StandardCharsets.UTF_8);
    }

    /** A path placeholder value. */
    public String pathParam(String name) {
        return pathParams.get(name);
    }

    /** First value of a query parameter, or {@code null}. */
    public String param(String name) {
        List<String> values = queryParams.get(name);
        return values == null || values.isEmpty() ? null : values.getFirst();
    }

    public String param(String name, String defaultValue) {
        return Optional.ofNullable(param(name)).orElse(defaultValue);
    }

    /**
     * OpenSearch's flag semantics: a bare {@code ?pretty} with no value counts as
     * true, which is why this is not just {@code Boolean.parseBoolean}.
     */
    public boolean paramAsBoolean(String name, boolean defaultValue) {
        List<String> values = queryParams.get(name);
        if (values == null || values.isEmpty()) {
            return defaultValue;
        }
        String value = values.getFirst();
        if (value == null || value.isEmpty()) {
            return true;
        }
        return Boolean.parseBoolean(value);
    }

    public String header(String name) {
        return headers.get(name.toLowerCase(Locale.ROOT));
    }

    public Map<String, String> headers() {
        return headers;
    }

    @Override
    public String toString() {
        return method + " " + uri + " -> " + operationGroup;
    }
}
