/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cat;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.cluster.ClusterRequests;
import org.opensearch.controlplane.handler.cluster.StatusProjections;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.RestRequest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.TreeSet;

/**
 * {@code GET /_cat/nodes}: one row per node the tenant can see, which is the nodes holding or assigned something
 * of its own and no others, by the supervisor's design (the same list {@code cluster.state} gives).
 *
 * <p>Heap, memory, CPU and load are {@code x}: they are per-node runtime, which the tenant-visible projection
 * deliberately omits so that it does not change with every heartbeat. {@code cluster_manager} is {@code x} as
 * upstream writes it when no cluster manager is elected, and none is. {@code node.role} is {@code d} for a data
 * node and {@code -} for one that only coordinates; {@code node.roles} names every role.
 *
 * <p>Upstream's default columns are these eleven; they are listed, with {@code node.roles} and
 * {@code cluster_manager}, exactly as a node of this build prints them. Not supported: the disk, heap-size,
 * file-descriptor, jvm and statistics columns. Rows are sorted by name, where upstream's order is a hash map's.
 */
public final class CatNodesHandler extends CatHandler {

    private static final List<Column> COLUMNS = List.of(
        Column.left("ip", "i", "ip address"),
        Column.right("heap.percent", "hp,heapPercent", "used heap ratio"),
        Column.right("ram.percent", "rp,ramPercent", "used machine memory ratio"),
        Column.right("cpu", "cpu", "recent cpu usage"),
        Column.right("load_1m", "l", "1m load avg"),
        Column.right("load_5m", "l", "5m load avg"),
        Column.right("load_15m", "l", "15m load avg"),
        Column.left("node.role", "r,role,nodeRole", "m:master eligible node, d:data node, i:ingest node, -:coordinating node only"),
        Column.left("node.roles", "rs,all roles", "-:coordinating node only"),
        Column.left("cluster_manager", "cm,m,master", "*:current cluster manager"),
        Column.left("name", "n", "node name"),
        Column.left("id", "id,nodeId", "unique node id").optional(),
        Column.right("port", "po", "bound transport port").optional(),
        Column.left("version", "v", "os version").optional()
    );

    public CatNodesHandler() {
        this(ClusterRequests.POLL);
    }

    CatNodesHandler(Duration poll) {
        super(poll);
    }

    @Override
    public String operationGroup() {
        return "cat.nodes";
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
        boolean fullId = request.paramAsBoolean("full_id", false);

        List<JsonNode> nodes = new ArrayList<>(projections.nodes().values());
        nodes.sort(Comparator.comparing((JsonNode node) -> node.path("name").asText())
            .thenComparing(node -> node.path("node_id").asText()));

        CatTable table = new CatTable(COLUMNS);
        for (JsonNode node : nodes) {
            String id = node.path("node_id").asText();
            TreeSet<String> roles = new TreeSet<>();
            node.path("roles").forEach(role -> roles.add(role.asText().toLowerCase(Locale.ROOT)));
            table.row(
                node.path("address").asText(),
                CatTable.UNKNOWN,
                CatTable.UNKNOWN,
                CatTable.UNKNOWN,
                CatTable.UNKNOWN,
                CatTable.UNKNOWN,
                CatTable.UNKNOWN,
                roles.contains("data") ? "d" : "-",
                roles.isEmpty() ? "-" : String.join(",", roles),
                CatTable.UNKNOWN,
                node.path("name").asText(),
                fullId ? id : id.substring(0, Math.min(4, id.length())),
                node.path("port").asInt(),
                node.path("version").asText()
            );
        }
        return Outcome.of(table);
    }
}
