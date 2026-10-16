/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cat;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.cluster.ClusterRequests;
import org.opensearch.controlplane.handler.cluster.ShardRoutings;
import org.opensearch.controlplane.handler.cluster.StatusProjections;
import org.opensearch.controlplane.handler.cluster.StatusProjections.IndexHealth;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.RestRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@code GET /_cat/shards[/{index}]}: one row per copy of each shard, from the same routing
 * {@code cluster.state} renders (see {@link ShardRoutings}).
 *
 * <p>A copy the index wants and nothing holds is a row too, {@code UNASSIGNED} with no node, so the table agrees
 * with {@code cluster.health}. For it the {@code ip}, {@code id}, {@code node}, {@code docs} and {@code store}
 * cells are blank. For a placed copy {@code docs} and {@code store} are {@code x}: see the data plane's
 * {@code FINDINGS_FROM_THE_CONTROL_PLANE.md}. A closed index has no rows, and the index expression is lenient,
 * as the cluster state request this is built on is: a missing name is skipped.
 *
 * <p>Rows are ordered by index, shard, and primary first. Not supported: the {@code unassigned.*} columns, which
 * record when and why and are not projected, {@code sync_id}, and the statistics columns.
 */
public final class CatShardsHandler extends CatHandler {

    private static final List<Column> COLUMNS = List.of(
        Column.left("index", "i,idx", "index name"),
        Column.left("shard", "s,sh", "shard name"),
        Column.left("prirep", "p,pr,primaryOrReplica", "primary or replica"),
        Column.left("state", "st", "shard state"),
        Column.right("docs", "d,dc", "number of docs in shard"),
        Column.right("store", "sto", "store size of shard (how much disk it uses)"),
        Column.left("ip", "", "ip of node where it lives"),
        Column.left("id", "", "unique id of node where it lives").optional(),
        Column.left("node", "n", "name of node where it lives"),
        Column.right("docs.deleted", "dd,docsDeleted", "number of deleted docs in shard").optional()
    );

    /** {@code lenientExpandOpen}: the cluster state request's default. */
    private static final IndicesOptions LENIENT_EXPAND_OPEN = new IndicesOptions(true, false, false, true, true);

    public CatShardsHandler() {
        this(ClusterRequests.POLL);
    }

    CatShardsHandler(Duration poll) {
        super(poll);
    }

    @Override
    public String operationGroup() {
        return "cat.shards";
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

        Selected selected = resolve(request, context, LENIENT_EXPAND_OPEN);
        if (selected.failure() != null) {
            return Outcome.failure(selected.failure());
        }
        List<IndexHealth> indices = new ArrayList<>();
        selected.names().forEach(name -> projections.index(name).ifPresent(indices::add));

        CatTable table = new CatTable(COLUMNS);
        for (ShardRoutings.Routing routing : ShardRoutings.of(indices)) {
            JsonNode node = routing.nodeId() == null ? null : projections.nodes().get(routing.nodeId());
            // A copy with no node holds no statistics: blank. One with a node has them, which nothing reports yet.
            String unknown = routing.unassigned() ? null : CatTable.UNKNOWN;
            table.row(
                routing.index(),
                routing.shard(),
                routing.prirep(),
                routing.state(),
                unknown,
                unknown,
                node == null ? null : node.path("address").asText(),
                routing.nodeId(),
                node == null ? null : node.path("name").asText(),
                unknown
            );
        }
        return Outcome.of(table);
    }
}
