/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
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
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.RestResponse;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

/**
 * Translates Netty's HTTP objects into the transport-agnostic dispatch call and back.
 *
 * <h2>Dispatch runs off the event loop</h2>
 *
 * <p>A handler blocks: every store call is synchronous I/O against etcd or ZooKeeper, and
 * {@code cluster.health} with a {@code wait_for_*} parameter re-reads the store until its condition holds or
 * its timeout passes, 30 seconds by default. On an event-loop thread either would stall every connection
 * that thread serves. So each request is dispatched on a virtual thread, which a blocked handler costs
 * nothing to hold, and the response is written back through the channel, which is safe from any thread.
 *
 * <p>Bounded all the same, by {@code inFlight}: past it a request answers {@code 429}, as a real cluster
 * does when a thread pool's queue is full, rather than queueing without limit against the store.
 *
 * <p>Reading pauses on a connection while one of its requests is in flight and resumes once the response
 * is written, so pipelined requests on one connection are answered in order.
 */
final class ControlPlaneHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private final HttpRequestDispatcher dispatcher;
    private final Executor executor;
    private final Semaphore inFlight;

    ControlPlaneHttpHandler(HttpRequestDispatcher dispatcher, Executor executor, Semaphore inFlight) {
        this.dispatcher = dispatcher;
        this.executor = executor;
        this.inFlight = inFlight;
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
        // Copied out above; the request is released when this method returns, so the task may not touch it.
        boolean keepAlive = HttpUtil.isKeepAlive(request);

        if (inFlight.tryAcquire() == false) {
            writeResponse(ctx, keepAlive, ErrorResponses.rejectedExecution(
                "rejected execution: " + MAX_IN_FLIGHT + " requests already in flight"));
            return;
        }
        ctx.channel().config().setAutoRead(false);
        executor.execute(() -> {
            RestResponse response;
            try {
                response = dispatcher.dispatch(method, rawPath, uri, queryParams, headers, body);
            } catch (RuntimeException e) {
                response = ErrorResponses.internalError(e, false);
            } finally {
                inFlight.release();
            }
            writeResponse(ctx, keepAlive, response)
                .addListener(written -> ctx.channel().config().setAutoRead(true));
        });
    }

    /** How many requests may be dispatched at once, across every connection. */
    static final int MAX_IN_FLIGHT = 1024;

    private static ChannelFuture writeResponse(ChannelHandlerContext ctx, boolean keepAlive, RestResponse response) {
        ByteBuf content = Unpooled.wrappedBuffer(response.body());
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.valueOf(response.status().code()),
            content
        );

        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, response.contentType());
        httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        response.headers().forEach((name, value) -> httpResponse.headers().set(name, value));

        HttpUtil.setKeepAlive(httpResponse, keepAlive);

        ChannelFuture written = ctx.writeAndFlush(httpResponse);
        return keepAlive ? written : written.addListener(ChannelFutureListener.CLOSE);
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
