/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.http;

import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Over a real socket: a handler that blocks must not hold up anyone else's request. */
class NettyHttpServerTests {

    @Test
    void aBlockedHandlerDoesNotStallOtherRequests() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        HttpRequestDispatcher dispatcher = (method, rawPath, uri, queryParams, headers, body) -> {
            if (rawPath.equals("/slow")) {
                try {
                    // Stands in for a store call or a wait_for_status poll.
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return RestResponse.json(RestStatus.OK, "{\"path\":\"" + rawPath + "\"}");
        };

        try (NettyHttpServer server = new NettyHttpServer("127.0.0.1", 0, dispatcher)) {
            server.start();
            // One client per request, so they go over separate connections.
            String base = "http://127.0.0.1:" + server.boundPort();
            CompletableFuture<HttpResponse<String>> slow = HttpClient.newHttpClient().sendAsync(
                HttpRequest.newBuilder(URI.create(base + "/slow")).build(), HttpResponse.BodyHandlers.ofString());

            HttpResponse<String> fast = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(base + "/fast")).timeout(Duration.ofSeconds(5)).build(),
                HttpResponse.BodyHandlers.ofString());
            assertEquals(200, fast.statusCode());
            assertFalse(slow.isDone(), "the slow request is still blocked, and the fast one was answered anyway");

            release.countDown();
            assertEquals(200, slow.get(5, TimeUnit.SECONDS).statusCode());
            assertTrue(slow.get().body().contains("/slow"));
        }
    }
}
