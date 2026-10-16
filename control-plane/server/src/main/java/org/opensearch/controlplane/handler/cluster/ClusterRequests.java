/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import org.opensearch.controlplane.handler.OperationContext;
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
public final class ClusterRequests {

    /** A request holds a dispatch slot while it waits, so no wait is unbounded, {@code -1} included. */
    public static final Duration MAX_WAIT = Duration.ofMinutes(5);

    /** How often a waiting request re-reads. The supervisor passes every 2 s, so faster finds nothing new. */
    public static final Duration POLL = Duration.ofSeconds(1);

    /** {@code BaseRestHandler}: the two spellings of one parameter may not both be sent. */
    public static Optional<RestResponse> rejectBothManagerTimeouts(RestRequest request) {
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
    public static Duration timeValue(RestRequest request, String name, Duration fallback) {
        String value = request.param(name);
        if (value == null) {
            return fallback;
        }
        Duration parsed = TimeValues.parse(value, name);
        return parsed == null ? MAX_WAIT : parsed;
    }

    /** The parameter {@code cluster_manager_timeout} was sent as: the new name wins when it is present. */
    public static String clusterManagerTimeoutParam(RestRequest request) {
        return request.param("cluster_manager_timeout") != null ? "cluster_manager_timeout" : "master_timeout";
    }

    /** What {@link #awaitFresh} read, and whether a supervisor was managing the tenant when it did. */
    public record Read(StatusProjections projections, boolean fresh) {}

    /** Upstream's default for {@code cluster_manager_timeout}. */
    public static final Duration DEFAULT_CLUSTER_MANAGER_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Reads the supervisor's projections, waiting up to {@code timeout} for a fresh {@code status/cluster}
     * unless {@code local}, which takes the last documents as they are.
     *
     * @return empty when none appeared in time, which is upstream's "no cluster manager discovered": the caller
     *     answers {@link ErrorResponses#clusterManagerNotDiscovered}
     */
    public static Optional<Read> awaitFresh(OperationContext context, Duration timeout, boolean local, Duration poll) {
        long deadline = System.nanoTime() + Math.min(timeout.toNanos(), MAX_WAIT.toNanos());
        while (true) {
            StatusProjections projections = StatusProjections.read(context.store());
            boolean fresh = projections.fresh(context.nowEpochMillis());
            if (fresh || local) {
                return Optional.of(new Read(projections, fresh));
            }
            if (System.nanoTime() - deadline >= 0) {
                return Optional.empty();
            }
            sleepUntil(deadline, poll);
        }
    }

    /** Sleeps until {@code deadlineNanos} or one {@code poll}, whichever is sooner. */
    public static void sleepUntil(long deadlineNanos, Duration poll) {
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
