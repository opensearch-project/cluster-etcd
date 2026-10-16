/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.handler.cluster.StatusProjections.Status;
import org.opensearch.controlplane.handler.cluster.StatusProjections.Totals;
import org.opensearch.controlplane.handler.indices.IndexManagementTasks;
import org.opensearch.controlplane.store.KeyValueStore;

import java.util.List;

/**
 * The cluster-level figures {@code cluster.health} and {@code _cat/health} both report, so that the two cannot
 * disagree about what a pending task is or how a percentage is rounded.
 */
public final class HealthFigures {

    private HealthFigures() {}

    /** This service's own index-management tasks that no pass has completed. */
    public record PendingTasks(int count, long longestWaitMillis) {}

    public static PendingTasks pendingTasks(KeyValueStore store, long nowMillis) {
        List<JsonNode> pending = IndexManagementTasks.pending(store);
        long longest = pending.stream()
            .mapToLong(task -> Math.max(0, nowMillis - task.path(IndexManagementTasks.SUBMITTED_AT).asLong(nowMillis)))
            .max()
            .orElse(0);
        return new PendingTasks(pending.size(), longest);
    }

    /**
     * {@code ClusterStateHealth}: exactly 100 when green; otherwise active over every copy of every index,
     * whatever the request selected. Upstream divides zero by zero there and renders NaN; this answers 100,
     * since with no copies there is nothing inactive.
     */
    public static double activeShardsPercent(Status status, StatusProjections projections) {
        if (status == Status.GREEN) {
            return 100.0;
        }
        Totals all = Totals.over(projections.indices().values());
        int total = all.activeShards() + all.initializingShards() + all.unassignedShards();
        return total == 0 ? 100.0 : ((double) all.activeShards() / total) * 100;
    }
}
