/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.cluster.StatusProjections.Copy;
import org.opensearch.controlplane.handler.cluster.StatusProjections.IndexHealth;
import org.opensearch.controlplane.rest.Json;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Every copy of every shard of the open indices handed in, as a cluster state's routing table would list
 * them: what {@code cluster.state}'s {@code routing_table} and {@code routing_nodes} render, and what
 * {@code _cat/shards} prints a row for.
 *
 * <p>The observed copies come as the supervisor saw them, then one unassigned entry for each copy the index asks
 * for that exists nowhere. The projection lists observed copies only, so the unplaced ones are derived from the
 * counts the index asked for. A copy a node reports failed is unassigned too, since it serves nothing and a real
 * cluster would show it so. Neither carries {@code unassigned_info}: why and when are not projected.
 *
 * <p>A closed index has no entry: it wants no copies, and the supervisor reports it with none.
 */
public final class ShardRoutings {

    private ShardRoutings() {}

    /** One copy of one shard, as {@code ShardRouting.toXContent} writes it. */
    public record Routing(String index, int shard, String state, boolean primary, boolean searchOnly,
                          String nodeId, String allocationId) {

        public boolean unassigned() {
            return "UNASSIGNED".equals(state);
        }

        /** {@code p}, {@code s} or {@code r}, as {@code _cat/shards} writes {@code prirep}. */
        public String prirep() {
            return primary ? "p" : searchOnly ? "s" : "r";
        }

        ObjectNode render() {
            ObjectNode node = Json.object();
            node.put("state", state);
            node.put("primary", primary);
            node.put("searchOnly", searchOnly);
            if (nodeId == null) {
                node.putNull("node");
            } else {
                node.put("node", nodeId);
            }
            // Nothing relocates here, so this is always null: a move is a copy removed and another created.
            node.putNull("relocating_node");
            node.put("shard", shard);
            node.put("index", index);
            if (allocationId != null) {
                node.putObject("allocation_id").put("id", allocationId);
            }
            return node;
        }
    }

    /** In index order, then shard, then primary first; the order {@code indices} iterates in. */
    public static List<Routing> of(Collection<IndexHealth> indices) {
        List<Routing> all = new ArrayList<>();
        for (IndexHealth index : indices) {
            if (isClosed(index.document()) == false) {
                all.addAll(of(index));
            }
        }
        return all;
    }

    private static List<Routing> of(IndexHealth index) {
        List<Routing> routings = new ArrayList<>();
        int searchReplicas = Math.max(0, index.copiesPerShard() - 1 - index.numberOfReplicas());
        for (int shard = 0; shard < index.numberOfShards(); shard++) {
            List<Copy> copies = new ArrayList<>(index.shards().getOrDefault(shard, List.of()));
            copies.sort(Comparator.comparingInt(copy -> "PRIMARY".equals(copy.role()) ? 0 : 1));

            int primaries = 0;
            int replicas = 0;
            int search = 0;
            for (Copy copy : copies) {
                switch (copy.role()) {
                    case "PRIMARY" -> primaries++;
                    case "SEARCH_REPLICA" -> search++;
                    default -> replicas++;
                }
                boolean placed = copy.started() || "INITIALIZING".equals(copy.state());
                routings.add(new Routing(index.name(), shard, placed ? copy.state() : "UNASSIGNED",
                    "PRIMARY".equals(copy.role()), "SEARCH_REPLICA".equals(copy.role()),
                    placed ? copy.nodeId() : null, placed ? copy.allocationId() : null));
            }
            for (int missing = primaries; missing < 1; missing++) {
                routings.add(new Routing(index.name(), shard, "UNASSIGNED", true, false, null, null));
            }
            for (int missing = replicas; missing < index.numberOfReplicas(); missing++) {
                routings.add(new Routing(index.name(), shard, "UNASSIGNED", false, false, null, null));
            }
            for (int missing = search; missing < searchReplicas; missing++) {
                routings.add(new Routing(index.name(), shard, "UNASSIGNED", false, true, null, null));
            }
        }
        return routings;
    }

    private static boolean isClosed(JsonNode document) {
        return "close".equals(document.path("state").asText("open"));
    }
}
