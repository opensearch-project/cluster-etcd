/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterApplierService;

import java.util.Objects;
import java.util.function.Function;

/**
 * The voice in the server's head: {@code ClusterApplierService.updateClusterState}.
 *
 * <p>Forty lines around one call, and the call is the entire trick. A locally fabricated state goes to the same
 * applier the real publication path uses, so recovery, allocation awareness, search routing and shard lifecycle
 * all run completely unmodified. Lifted from {@code cluster-etcd}, where this is the part that unambiguously
 * worked — and the reason this project is weeks rather than months.
 *
 * <p>Two things about the shape are worth keeping. The state is built <b>inside</b> the update function, against
 * whatever the server hands over, so an update never overwrites something the server applied while the poll was
 * deciding what to ask for. And the outcome is asynchronous: the applier queues the update at {@code HIGH}
 * priority on its own thread and calls back later, so nothing here may assume the state is applied when this
 * method returns.
 */
public final class ClusterApplierServiceStateApplier implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(ClusterApplierServiceStateApplier.class);

    private final ClusterApplierService clusterApplierService;

    public ClusterApplierServiceStateApplier(ClusterApplierService clusterApplierService) {
        this.clusterApplierService = Objects.requireNonNull(clusterApplierService, "clusterApplierService");
    }

    @Override
    public void apply(String source, Function<ClusterState, ClusterState> update, Outcome onOutcome) {
        clusterApplierService.updateClusterState(source, update, new ClusterApplier.ClusterApplyListener() {
            @Override
            public void onSuccess(String appliedSource) {
                onOutcome.applied(appliedSource, null);
            }

            @Override
            public void onFailure(String failedSource, Exception e) {
                // Logged here as well as reported, because the reported version is capped at 512 characters for
                // the heartbeat and this is the only place the stack trace exists.
                logger.error("clusterless: failed to apply the cluster state from {}", failedSource, e);
                onOutcome.applied(failedSource, e);
            }
        });
    }
}
