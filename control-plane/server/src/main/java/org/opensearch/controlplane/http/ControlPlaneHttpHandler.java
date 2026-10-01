/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.opensearch.controlplane.rest.RestResponse;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Translates Netty's HTTP objects into the transport-agnostic dispatch call and back. */
final class ControlPlaneHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final HttpRequestDispatcher dispatcher;

    ControlPlaneHttpHandler(HttpRequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        String uri = request.uri();
        String method = request.method().name().toUpperCase(Locale.ROOT);

        // Split on '?' ourselves and keep the path percent-encoded. Netty's
        // QueryStringDecoder.path() would hand back a decoded path, which must not
        // be split on '/' afterwards.
        int queryStart = uri.indexOf('?');
        String rawPath = queryStart < 0 ? uri : uri.substring(0, queryStart);

        QueryStringDecoder queryDecoder = new QueryStringDecoder(uri);
        Map<String, List<String>> queryParams = queryDecoder.parameters();

        Map<String, String> headers = new LinkedHashMap<>();
        for (Map.Entry<String, String> header : request.headers()) {
            headers.put(header.getKey().toLowerCase(Locale.ROOT), header.getValue());
        }

        ByteBuf content = request.content();
        byte[] body = new byte[content.readableBytes()];
        content.getBytes(content.readerIndex(), body);

        RestResponse response;
        try {
            response = dispatcher.dispatch(method, rawPath, uri, queryParams, headers, body);
        } catch (RuntimeException e) {
            response = org.opensearch.controlplane.rest.ErrorResponses.internalError(e, false);
        }

        writeResponse(ctx, request, response);
    }

    private static void writeResponse(ChannelHandlerContext ctx, FullHttpRequest request, RestResponse response) {
        ByteBuf content = Unpooled.wrappedBuffer(response.body());
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.valueOf(response.status().code()),
            content
        );

        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, response.contentType());
        httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        response.headers().forEach((name, value) -> httpResponse.headers().set(name, value));

        boolean keepAlive = HttpUtil.isKeepAlive(request);
        HttpUtil.setKeepAlive(httpResponse, keepAlive);

        if (keepAlive) {
            ctx.writeAndFlush(httpResponse);
        } else {
            ctx.writeAndFlush(httpResponse).addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Malformed HTTP, a decoder failure, or anything else that never reached
        // dispatch. There is no usable request context, so answer plainly and close.
        byte[] body = ("{\"error\":\"" + escape(String.valueOf(cause.getMessage())) + "\",\"status\":400}")
            .getBytes(StandardCharsets.UTF_8);
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.BAD_REQUEST,
            Unpooled.wrappedBuffer(body)
        );
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, RestResponse.APPLICATION_JSON);
        httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
        ctx.writeAndFlush(httpResponse).addListener(ChannelFutureListener.CLOSE);
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
    }
}
