/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the Netty-to-router translation without opening a socket: URI
 * splitting, query decoding, header normalisation, body extraction, and response
 * writing.
 */
class ControlPlaneHttpHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static EmbeddedChannel channelFor(HttpRequestDispatcher dispatcher) {
        return new EmbeddedChannel(new ControlPlaneHttpHandler(dispatcher));
    }

    private static EmbeddedChannel stubbedChannel() {
        Router router = Router.fromGeneratedRoutes(HandlerRegistry.withAllStubbed(), TestContexts.factory());
        return channelFor(router::dispatch);
    }

    private static FullHttpRequest request(HttpMethod method, String uri, String body) {
        FullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            method,
            uri,
            Unpooled.wrappedBuffer(body.getBytes(StandardCharsets.UTF_8))
        );
        request.headers().set(HttpHeaderNames.HOST, "localhost");
        request.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, request.content().readableBytes());
        return request;
    }

    @Test
    void stubbedOperationReturns501OverHttp() throws Exception {
        EmbeddedChannel channel = stubbedChannel();
        channel.writeInbound(request(HttpMethod.PUT, "/my-index", "{}"));

        FullHttpResponse response = channel.readOutbound();
        assertNotNull(response, "handler should have written a response");
        assertEquals(501, response.status().code());
        assertEquals(RestResponse.APPLICATION_JSON, response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        assertEquals(
            response.content().readableBytes(),
            response.headers().getInt(HttpHeaderNames.CONTENT_LENGTH).intValue()
        );

        JsonNode json = MAPPER.readTree(response.content().toString(StandardCharsets.UTF_8));
        assertEquals("indices.create", json.get("error").get("operation_group").asText());
    }

    @Test
    void queryStringIsStrippedFromThePathAndDecoded() {
        AtomicReference<RestRequest> seen = new AtomicReference<>();
        EmbeddedChannel channel = channelFor((method, rawPath, uri, queryParams, headers, body) -> {
            seen.set(new RestRequest(method, rawPath, uri, queryParams, Map.of(), headers, body, "test"));
            return RestResponse.json(RestStatus.OK, "{}");
        });

        channel.writeInbound(request(HttpMethod.GET, "/_cluster/health?level=indices&pretty", ""));

        RestRequest captured = seen.get();
        assertEquals("/_cluster/health", captured.rawPath());
        assertEquals("/_cluster/health?level=indices&pretty", captured.uri());
        assertEquals("indices", captured.param("level"));
        assertTrue(captured.paramAsBoolean("pretty", false), "bare ?pretty means true");
    }

    @Test
    void percentEncodedPathIsNotDecodedBeforeSplitting() {
        AtomicReference<String> rawPath = new AtomicReference<>();
        EmbeddedChannel channel = channelFor((method, path, uri, queryParams, headers, body) -> {
            rawPath.set(path);
            return RestResponse.json(RestStatus.OK, "{}");
        });

        channel.writeInbound(request(HttpMethod.GET, "/weird%2Fname/_settings", ""));

        // Still encoded at this layer; PathTrie decodes per-segment after splitting.
        assertEquals("/weird%2Fname/_settings", rawPath.get());
    }

    @Test
    void bodyAndHeadersReachTheDispatcher() {
        AtomicReference<RestRequest> seen = new AtomicReference<>();
        EmbeddedChannel channel = channelFor((method, rawPath, uri, queryParams, headers, body) -> {
            seen.set(new RestRequest(method, rawPath, uri, queryParams, Map.of(), headers, body, "test"));
            return RestResponse.json(RestStatus.OK, "{}");
        });

        FullHttpRequest request = request(HttpMethod.PUT, "/my-index", "{\"settings\":{}}");
        request.headers().set("X-Opaque-Id", "abc123");
        channel.writeInbound(request);

        RestRequest captured = seen.get();
        assertEquals("{\"settings\":{}}", captured.bodyAsString());
        // Headers are lowercased so lookups are case-insensitive.
        assertEquals("abc123", captured.header("x-opaque-id"));
        assertEquals("abc123", captured.header("X-Opaque-Id"));
    }

    @Test
    void allowHeaderIsPropagatedOnMethodNotAllowed() {
        EmbeddedChannel channel = stubbedChannel();
        channel.writeInbound(request(HttpMethod.DELETE, "/_cluster/health", ""));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(405, response.status().code());
        assertEquals("GET", response.headers().get("Allow"));
    }

    @Test
    void headResponseHasNoBodyButKeepsContentLengthZero() {
        EmbeddedChannel channel = stubbedChannel();
        channel.writeInbound(request(HttpMethod.HEAD, "/my-index", ""));

        FullHttpResponse response = channel.readOutbound();
        assertEquals(501, response.status().code());
        assertEquals(0, response.content().readableBytes());
        assertEquals(0, response.headers().getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
    }
}
