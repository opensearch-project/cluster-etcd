/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterState;

/**
 * Hands a fabricated {@link ClusterState} to the server.
 *
 * <p>This is the seam the whole project exists to reach, and it is one method wide. Everything that happens
 * after it — creating an index, recovering a shard, opening it for search, honouring allocation awareness — is
 * code the server already has and already runs on every published state. The plugin's contribution is to be
 * the thing that publishes.
 *
 * <p>An interface because it is also the one part of the pipeline that cannot be exercised without a running
 * node. Everything upstream is a pure function or a store read; this is a call into a service that owns
 * threads, applies state on its own executor, and reports asynchronously. A seam here means the poller's
 * behaviour around it — what it acknowledges, what it retries, what it says when applying fails — is testable
 * on its own.
 */
public interface ClusterStateApplier {

    /**
     * Applies a state derived from the one currently applied.
     *
     * <p>The state is built from a function rather than passed in, because the server decides when the update
     * runs and against what. Between a poll observing the current state and the applier getting to the update,
     * the server may have applied something else — a shard finishing recovery, for instance — and building
     * against a stale snapshot would silently undo it. So the fabrication happens inside the update, against
     * the state the server hands over.
     *
     * @param source a short description, which appears in the server's own logs and in pending-task output
     * @param onOutcome called when the state has been applied, or when it could not be
     */
    void apply(String source, java.util.function.Function<ClusterState, ClusterState> update, Outcome onOutcome);

    /** What became of an application. Deliberately not a listener interface with a default method. */
    @FunctionalInterface
    interface Outcome {
        /**
         * @param failure {@code null} when the state was applied
         */
        void applied(String source, Exception failure);
    }
}
