/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.rest.TimeValues;

import java.time.Duration;
import java.util.Optional;

/**
 * What {@code cluster.health} and {@code cluster.state} share: both answer from the supervisor's projections,
 * both can wait on them, and both take upstream's cluster-manager timeouts.
 */
final class ClusterRequests {

    /** A request holds a dispatch slot while it waits, so no wait is unbounded, {@code -1} included. */
    static final Duration MAX_WAIT = Duration.ofMinutes(5);

    /** How often a waiting request re-reads. The supervisor passes every 2 s, so faster finds nothing new. */
    static final Duration POLL = Duration.ofSeconds(1);

    /** {@code BaseRestHandler}: the two spellings of one parameter may not both be sent. */
    static Optional<RestResponse> rejectBothManagerTimeouts(RestRequest request) {
        if (request.param("master_timeout") != null && request.param("cluster_manager_timeout") != null) {
            return Optional.of(ErrorResponses.structured(RestStatus.BAD_REQUEST, "parse_exception",
                "Please only use one of the request parameters [master_timeout, cluster_manager_timeout].", request));
        }
        return Optional.empty();
    }

    /**
     * A time-value parameter, in upstream's words when it is malformed.
     *
     * @return the parameter, {@code fallback} when absent, and {@link #MAX_WAIT} for {@code -1}
     * @throws IllegalArgumentException with upstream's message
     */
    static Duration timeValue(RestRequest request, String name, Duration fallback) {
        String value = request.param(name);
        if (value == null) {
            return fallback;
        }
        Duration parsed = TimeValues.parse(value, name);
        return parsed == null ? MAX_WAIT : parsed;
    }

    /** The parameter {@code cluster_manager_timeout} was sent as: the new name wins when it is present. */
    static String clusterManagerTimeoutParam(RestRequest request) {
        return request.param("cluster_manager_timeout") != null ? "cluster_manager_timeout" : "master_timeout";
    }

    /** Sleeps until {@code deadlineNanos} or one {@code poll}, whichever is sooner. */
    static void sleepUntil(long deadlineNanos, Duration poll) {
        long remaining = deadlineNanos - System.nanoTime();
        try {
            Thread.sleep(Duration.ofNanos(Math.max(0, Math.min(remaining, poll.toNanos()))));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting", e);
        }
    }

    private ClusterRequests() {}
}
