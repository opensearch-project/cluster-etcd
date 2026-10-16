/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cat;

import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.cluster.ClusterRequests;
import org.opensearch.controlplane.handler.cluster.HealthFigures;
import org.opensearch.controlplane.handler.cluster.StatusProjections;
import org.opensearch.controlplane.handler.cluster.StatusProjections.Status;
import org.opensearch.controlplane.handler.cluster.StatusProjections.Totals;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.RestRequest;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * {@code GET /_cat/health}: one row, the figures {@code cluster.health} reports, read the same way and by the
 * same code, so the two cannot disagree. See {@code ClusterHealthHandler} for what they are and where they come
 * from.
 *
 * <p>{@code shards} is the <em>active</em> shard count, as upstream's is, and {@code relo} is always 0.
 * {@code ts=false} drops {@code epoch} and {@code timestamp}.
 */
public final class CatHealthHandler extends CatHandler {

    private static final List<Column> COLUMNS = List.of(
        Column.left("epoch", "t,time", "seconds since 1970-01-01 00:00:00"),
        Column.left("timestamp", "ts,hms,hhmmss", "time in HH:MM:SS"),
        Column.left("cluster", "cl", "cluster name"),
        Column.left("status", "st", "health status"),
        Column.right("node.total", "nt,nodeTotal", "total number of nodes"),
        Column.right("node.data", "nd,nodeData", "number of nodes that can store data"),
        Column.right("discovered_cluster_manager", "dcm,dm,discovered_master", "cluster manager is discovered or not"),
        Column.right("shards", "t,sh,shards.total,shardsTotal", "total number of shards"),
        Column.right("pri", "p,shards.primary,shardsPrimary", "number of primary shards"),
        Column.right("relo", "r,shards.relocating,shardsRelocating", "number of relocating nodes"),
        Column.right("init", "i,shards.initializing,shardsInitializing", "number of initializing nodes"),
        Column.right("unassign", "u,shards.unassigned,shardsUnassigned", "number of unassigned shards"),
        Column.right("pending_tasks", "pt,pendingTasks", "number of pending tasks"),
        Column.right("max_task_wait_time", "mtwt,maxTaskWaitTime", "wait time of longest task pending"),
        Column.right("active_shards_percent", "asp,activeShardsPercent", "active number of shards in percent")
    );

    private static final DateTimeFormatter HOURS_MINUTES_SECONDS =
        DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT).withZone(ZoneOffset.UTC);

    private final ClusterIdentity identity;

    public CatHealthHandler(ClusterIdentity identity) {
        this(identity, ClusterRequests.POLL);
    }

    CatHealthHandler(ClusterIdentity identity, Duration poll) {
        super(poll);
        this.identity = identity;
    }

    @Override
    public String operationGroup() {
        return "cat.health";
    }

    @Override
    List<Column> columns() {
        return COLUMNS;
    }

    @Override
    Outcome table(RestRequest request, OperationContext context) {
        Optional<ClusterRequests.Read> read = awaitSupervisor(request, context);
        if (read.isEmpty()) {
            return Outcome.failure(ErrorResponses.clusterManagerNotDiscovered(request));
        }
        StatusProjections projections = read.get().projections();
        boolean fresh = read.get().fresh();

        Totals totals = Totals.over(projections.indices().values());
        Status status = fresh ? totals.status() : Status.RED;
        long now = context.nowEpochMillis();
        HealthFigures.PendingTasks pending = HealthFigures.pendingTasks(context.store(), now);

        CatTable table = new CatTable(COLUMNS);
        table.row(
            now / 1000,
            HOURS_MINUTES_SECONDS.format(java.time.Instant.ofEpochMilli(now)),
            identity.clusterName(),
            status.wire(),
            projections.numberOfNodes(),
            projections.numberOfDataNodes(),
            fresh,
            totals.activeShards(),
            totals.activePrimaryShards(),
            0,
            totals.initializingShards(),
            totals.unassignedShards(),
            pending.count(),
            pending.longestWaitMillis() == 0 ? "-" : Duration.ofMillis(pending.longestWaitMillis()),
            String.format(Locale.ROOT, "%1.1f%%", HealthFigures.activeShardsPercent(status, projections))
        );
        return Outcome.of(request.paramAsBoolean("ts", true) ? table : table.without(Set.of("epoch", "timestamp")));
    }
}
