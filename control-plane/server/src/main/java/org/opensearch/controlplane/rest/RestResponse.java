/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/** An outbound response: status, content type, body bytes, plus any extra headers. */
public final class RestResponse {

    public static final String APPLICATION_JSON = "application/json; charset=UTF-8";
    public static final String TEXT_PLAIN = "text/plain; charset=UTF-8";

    private final RestStatus status;
    private final String contentType;
    private final byte[] body;
    private final Map<String, String> headers;

    public RestResponse(RestStatus status, String contentType, byte[] body, Map<String, String> headers) {
        this.status = status;
        this.contentType = contentType;
        this.body = body == null ? new byte[0] : body;
        this.headers = Map.copyOf(headers);
    }

    public static RestResponse json(RestStatus status, String json) {
        return new RestResponse(status, APPLICATION_JSON, json.getBytes(StandardCharsets.UTF_8), Map.of());
    }

    public static RestResponse json(RestStatus status, String json, Map<String, String> headers) {
        return new RestResponse(status, APPLICATION_JSON, json.getBytes(StandardCharsets.UTF_8), headers);
    }

    /** A body-less response, as OpenSearch returns for HEAD requests. */
    public static RestResponse empty(RestStatus status) {
        return new RestResponse(status, APPLICATION_JSON, new byte[0], Map.of());
    }

    public RestResponse withHeader(String name, String value) {
        Map<String, String> merged = new LinkedHashMap<>(headers);
        merged.put(name, value);
        return new RestResponse(status, contentType, body, merged);
    }

    public RestStatus status() {
        return status;
    }

    public String contentType() {
        return contentType;
    }

    public byte[] body() {
        return body;
    }

    public Map<String, String> headers() {
        return headers;
    }
}
