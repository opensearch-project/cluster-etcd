/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

/**
 * JSON construction and serialisation for responses.
 *
 * <p>Centralised so that {@code ?pretty} is handled identically everywhere. OpenSearch
 * pretty-prints through Jackson's {@code DefaultPrettyPrinter}, so the two-space indent and
 * {@code " : "} separator line up with the server.
 */
public final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ObjectWriter COMPACT = MAPPER.writer();
    private static final ObjectWriter PRETTY = MAPPER.writerWithDefaultPrettyPrinter();

    private Json() {}

    public static ObjectNode object() {
        return MAPPER.createObjectNode();
    }

    public static ArrayNode array() {
        return MAPPER.createArrayNode();
    }

    /** Parses JSON, throwing {@link IOException} on malformed input. */
    public static JsonNode parse(byte[] json) throws IOException {
        return MAPPER.readTree(json);
    }

    /** Compact canonical serialisation, for storing a payload. */
    public static byte[] toBytes(JsonNode node) {
        try {
            return COMPACT.writeValueAsBytes(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialise JSON", e);
        }
    }

    public static String write(JsonNode node, boolean pretty) {
        try {
            return (pretty ? PRETTY : COMPACT).writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialise JSON", e);
        }
    }

    public static RestResponse response(RestStatus status, JsonNode body, boolean pretty) {
        return RestResponse.json(status, write(body, pretty));
    }

    /**
     * Builds a response, applying the global parameters the request asked for.
     *
     * <p>Every success response goes through here, which is the point: {@code filter_path} and
     * {@code pretty} apply to all of them without any handler needing to know. Error responses
     * deliberately do not use this path — a real cluster ignores {@code filter_path} when rendering an
     * exception.
     */
    public static RestResponse response(RestStatus status, JsonNode body, RestRequest request) {
        JsonNode filtered = FilterPath.apply(body, request.param(GlobalParameters.FILTER_PATH));
        return response(status, filtered, request.paramAsBoolean(GlobalParameters.PRETTY, false));
    }

    /**
     * The {@code AcknowledgedResponseBase} shape — {@code {"acknowledged": true}} — returned by
     * most control plane mutations.
     */
    public static RestResponse acknowledged(RestRequest request) {
        ObjectNode body = object();
        body.put("acknowledged", true);
        return response(RestStatus.OK, body, request);
    }
}
