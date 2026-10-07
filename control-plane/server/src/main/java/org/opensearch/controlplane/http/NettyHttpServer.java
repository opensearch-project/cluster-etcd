/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerKeepAliveHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * A minimal Netty HTTP/1.1 front end.
 *
 * <p>Netty rather than a higher-level framework because the routing lives in
 * {@link org.opensearch.controlplane.router.Router} — a framework's own router
 * would have to be bypassed anyway, and Netty is what OpenSearch uses, so
 * anything ported from the server lands in familiar surroundings.
 */
public final class NettyHttpServer implements AutoCloseable {

    /** Matches OpenSearch's default http.max_content_length. */
    private static final int MAX_CONTENT_LENGTH = 100 * 1024 * 1024;

    private final String host;
    private final int port;
    private final HttpRequestDispatcher dispatcher;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ExecutorService dispatchExecutor;
    private Channel channel;

    public NettyHttpServer(String host, int port, HttpRequestDispatcher dispatcher) {
        this.host = host;
        this.port = port;
        this.dispatcher = dispatcher;
    }

    /** Binds the listen socket. Returns once the server is accepting connections. */
    public void start() throws InterruptedException {
        try {
            bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
            workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
            dispatchExecutor = Executors.newVirtualThreadPerTaskExecutor();
            Semaphore inFlight = new Semaphore(ControlPlaneHttpHandler.MAX_IN_FLIGHT);

            ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        // Aggregate into FullHttpRequest: control plane bodies are
                        // small and whole-document, so there is nothing to gain from
                        // streaming. Revisit if bulk-style ndjson is ever in scope.
                        pipeline.addLast(new HttpObjectAggregator(MAX_CONTENT_LENGTH));
                        pipeline.addLast(new HttpServerKeepAliveHandler());
                        pipeline.addLast(new ControlPlaneHttpHandler(dispatcher, dispatchExecutor, inFlight));
                    }
                });

            channel = bootstrap.bind(new InetSocketAddress(host, port)).sync().channel();
        } catch (Throwable e) {
            // The event loop threads are non-daemon. Without this the JVM hangs
            // forever after a failed bind (port in use, permission denied) instead
            // of reporting the error and exiting.
            //
            // Throwable rather than a narrower type on purpose: Netty's sync()
            // rethrows the original failure via an unchecked cast, so a bind error
            // arrives here as a bare java.net.SocketException despite this method
            // declaring only InterruptedException. Java's precise rethrow keeps the
            // declared signature honest.
            close();
            throw e;
        }
    }

    /** The actual bound port, which differs from the requested one when port 0 was requested. */
    public int boundPort() {
        if (channel == null) {
            throw new IllegalStateException("server is not started");
        }
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    /** Blocks until the listen socket closes. */
    public void awaitShutdown() throws InterruptedException {
        if (channel != null) {
            channel.closeFuture().sync();
        }
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.close().awaitUninterruptibly();
            channel = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
            workerGroup = null;
        }
        if (dispatchExecutor != null) {
            // Requests still in flight have nowhere to write their responses now; do not wait for them.
            dispatchExecutor.shutdownNow();
            dispatchExecutor = null;
        }
    }
}
