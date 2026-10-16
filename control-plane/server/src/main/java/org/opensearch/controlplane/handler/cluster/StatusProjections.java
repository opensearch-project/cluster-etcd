/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * What the supervisor has projected for a tenant, read once and composed into health figures.
 *
 * <p>Three sources, and the rule for each (supervisor {@code SUPERVISOR_KEYS.md} §1, control plane
 * {@code FINDINGS_FROM_THE_DATA_PLANE.md} item 1):
 *
 * <ul>
 *   <li><b>{@code status/indices/<uuid>}</b> gives each index's status and counts, computed by the supervisor
 *       against the copies the index asked for. They are served as they are. The documents list only observed
 *       copies, so recomputing health from them would miss every copy that is wanted and placed nowhere, and
 *       report a missing replica as green.</li>
 *   <li><b>{@code status/cluster}</b> gives the node counts, and is the freshness signal for the whole tenant:
 *       it is rewritten at least every {@code heartbeat_ms}, last in a pass, so a fresh one means everything
 *       else that pass computed has been written too.</li>
 *   <li><b>{@code indices/<name>}</b>, this service's own documents, decide which indices exist. An index the
 *       supervisor has not projected yet is reported red with every copy unassigned, which is what it is until
 *       a pass places it. A projection for an index that no longer exists here is ignored.</li>
 * </ul>
 *
 * <p>Cluster-level status and counts are always the sum over the selected indices' documents rather than
 * {@code status/cluster}'s own totals, so that filtered and unfiltered requests agree and an index created
 * since the last pass counts in both.
 */
public final class StatusProjections {

    public static final String CLUSTER_KEY = "status/cluster";
    public static final String INDICES_PREFIX = "status/indices/";

    /**
     * How much older than {@code heartbeat_ms} {@code status/cluster} may be before nothing is managing the
     * tenant: a lease TTL of 15 s, a pass of 2 s, and margin for clock skew between the two processes, which
     * compare wall clocks. The supervisor agreed this in its {@code FINDINGS_FROM_THE_CONTROL_PLANE.md}, item 1.
     */
    static final long FAILOVER_ALLOWANCE_MS = 35_000;

    /** What a supervisor that predates the heartbeat publishes as {@code heartbeat_ms}; read as the current default. */
    static final long DEFAULT_HEARTBEAT_MS = 10_000;

    public enum Status {
        GREEN, YELLOW, RED;

        public static Status of(String value) {
            return switch (value == null ? "" : value.toLowerCase(java.util.Locale.ROOT)) {
                case "green" -> GREEN;
                case "yellow" -> YELLOW;
                // An unrecognised value can only come from a newer supervisor; do not report it as healthy.
                default -> RED;
            };
        }

        public Status worst(Status other) {
            return compareTo(other) >= 0 ? this : other;
        }

        public String wire() {
            return name().toLowerCase(java.util.Locale.ROOT);
        }
    }

    /**
     * One copy of a shard as the supervisor saw it. {@code allocationId} and {@code inSync} are present on a
     * document-replication copy only, carried verbatim ({@code SUPERVISOR_KEYS.md}, finding 12); {@code inSync}
     * is set on a primary.
     */
    public record Copy(String nodeId, String role, String state, String allocationId, List<String> inSync) {
        public boolean started() {
            return "STARTED".equals(state);
        }
    }

    /**
     * One index's health: the supervisor's figures, or the unplaced figures for one it has not projected.
     * {@code document} is this service's own index document, which {@code cluster.state} renders.
     */
    public record IndexHealth(
        String name,
        JsonNode document,
        Status status,
        int numberOfShards,
        int numberOfReplicas,
        int copiesPerShard,
        int activePrimaryShards,
        int activeShards,
        int initializingShards,
        int unassignedShards,
        NavigableMap<Integer, List<Copy>> shards
    ) {}

    /** Status and counts over a set of indices. Relocating shards are always zero: no copy here relocates. */
    public record Totals(Status status, int activePrimaryShards, int activeShards, int initializingShards, int unassignedShards) {

        public static Totals over(Collection<IndexHealth> indices) {
            Status status = Status.GREEN;
            int primaries = 0;
            int active = 0;
            int initializing = 0;
            int unassigned = 0;
            for (IndexHealth index : indices) {
                status = status.worst(index.status());
                primaries += index.activePrimaryShards();
                active += index.activeShards();
                initializing += index.initializingShards();
                unassigned += index.unassignedShards();
            }
            return new Totals(status, primaries, active, initializing, unassigned);
        }
    }

    private final JsonNode cluster;
    private final NavigableMap<String, IndexHealth> indices;
    private final NavigableMap<String, Long> revisions;
    private final NavigableMap<String, JsonNode> nodes;

    private StatusProjections(
        JsonNode cluster,
        NavigableMap<String, IndexHealth> indices,
        NavigableMap<String, Long> revisions,
        NavigableMap<String, JsonNode> nodes
    ) {
        this.cluster = cluster;
        this.indices = indices;
        this.revisions = revisions;
        this.nodes = nodes;
    }

    public static final String NODES_PREFIX = "status/nodes/";

    public static StatusProjections read(KeyValueStore store) {
        JsonNode cluster = store.get(CLUSTER_KEY).map(versioned -> parse(versioned.value())).orElse(null);

        NavigableMap<String, Long> revisions = new TreeMap<>();
        Map<String, JsonNode> byUuid = new HashMap<>();
        store.scanPrefix(INDICES_PREFIX).forEach((key, versioned) -> {
            JsonNode status = parse(versioned.value());
            byUuid.put(status.path("index_uuid").asText(), status);
            revisions.put(key, versioned.revision());
        });

        NavigableMap<String, IndexHealth> indices = new TreeMap<>();
        store.scanPrefix(IndexAbstractions.INDEX_KEY_PREFIX).forEach((key, versioned) -> {
            String name = key.substring(IndexAbstractions.INDEX_KEY_PREFIX.length());
            JsonNode metadata = parse(versioned.value());
            JsonNode projected = byUuid.get(metadata.path("uuid").asText());
            indices.put(name, projected != null ? projected(name, metadata, projected) : unprojected(name, metadata));
            revisions.put(key, versioned.revision());
        });

        // The tenant's own nodes, by node id. A lost node is not here: the supervisor leaves it out, and its
        // copies out of the index projections, so the three agree.
        NavigableMap<String, JsonNode> nodes = new TreeMap<>();
        store.scanPrefix(NODES_PREFIX).forEach((key, versioned) -> {
            JsonNode node = parse(versioned.value());
            nodes.put(node.path("node_id").asText(key.substring(NODES_PREFIX.length())), node);
            revisions.put(key, versioned.revision());
        });
        return new StatusProjections(cluster, indices, revisions, nodes);
    }

    /** The tenant's nodes as the supervisor projects them, by node id: {@code status/nodes/<id>} documents. */
    public NavigableMap<String, JsonNode> nodes() {
        return nodes;
    }

    /**
     * The store revision of every index document, index projection and node projection read, by key. What
     * {@code cluster.state} derives its {@code version} and {@code state_uuid} from, alongside the documents it
     * reads itself.
     * {@code status/cluster} is deliberately absent: it is rewritten every heartbeat whether or not anything
     * changed, which would make the version move every ten seconds.
     */
    public NavigableMap<String, Long> revisions() {
        return revisions;
    }

    /**
     * Whether a supervisor is managing the tenant: {@code status/cluster} exists and is no older than its
     * heartbeat plus a failover. Absent means no supervisor has projected the tenant yet.
     */
    public boolean fresh(long nowMillis) {
        if (cluster == null) {
            return false;
        }
        long heartbeat = cluster.path("heartbeat_ms").asLong(0);
        long allowed = (heartbeat > 0 ? heartbeat : DEFAULT_HEARTBEAT_MS) + FAILOVER_ALLOWANCE_MS;
        return nowMillis - cluster.path("observed_at").asLong(0) <= allowed;
    }

    public int numberOfNodes() {
        return cluster == null ? 0 : cluster.path("number_of_nodes").asInt(0);
    }

    public int numberOfDataNodes() {
        return cluster == null ? 0 : cluster.path("number_of_data_nodes").asInt(0);
    }

    /** Every index this service knows of, by the tenant's own name. */
    public NavigableMap<String, IndexHealth> indices() {
        return indices;
    }

    public Optional<IndexHealth> index(String name) {
        return Optional.ofNullable(indices.get(name));
    }

    private static IndexHealth projected(String name, JsonNode document, JsonNode status) {
        NavigableMap<Integer, List<Copy>> shards = new TreeMap<>();
        status.path("shards").properties().forEach(shard -> {
            List<Copy> copies = new ArrayList<>();
            shard.getValue().forEach(copy -> {
                List<String> inSync = null;
                if (copy.has("in_sync")) {
                    inSync = new ArrayList<>();
                    for (JsonNode id : copy.get("in_sync")) {
                        inSync.add(id.asText());
                    }
                }
                copies.add(new Copy(copy.path("node_id").asText(null), copy.path("role").asText(), copy.path("state").asText(),
                    copy.path("allocation_id").asText(null), inSync));
            });
            shards.put(Integer.parseInt(shard.getKey()), copies);
        });
        int replicas = status.path("number_of_replicas").asInt(0);
        return new IndexHealth(
            // The tenant's name, not the projection's: the projection's is the same today, and this one is the
            // name every other operation answers with.
            name,
            document,
            Status.of(status.path("status").asText(null)),
            status.path("number_of_shards").asInt(0),
            replicas,
            1 + replicas + status.path("number_of_search_replicas").asInt(0),
            status.path("active_primary_shards").asInt(0),
            status.path("active_shards").asInt(0),
            status.path("initializing_shards").asInt(0),
            status.path("unassigned_shards").asInt(0),
            shards
        );
    }

    /**
     * An index no pass has projected yet. Open, nothing is placed, so it is red with every copy unassigned, as
     * the supervisor would report it. Closed, it wants no copies, which the supervisor reports as green.
     */
    private static IndexHealth unprojected(String name, JsonNode metadata) {
        Settings settings = Settings.forIndex(metadata.path("settings"));
        int shards = intSetting(settings, "index.number_of_shards", 1);
        int replicas = intSetting(settings, "index.number_of_replicas", 0);
        int copies = 1 + replicas + intSetting(settings, "index.number_of_search_replicas", 0);
        boolean open = "close".equals(metadata.path("state").asText("open")) == false;
        NavigableMap<Integer, List<Copy>> placed = new TreeMap<>();
        if (open) {
            for (int shard = 0; shard < shards; shard++) {
                placed.put(shard, List.of());
            }
        }
        return new IndexHealth(
            name, metadata, open ? Status.RED : Status.GREEN, shards, replicas, copies, 0, 0, 0, open ? shards * copies : 0, placed);
    }

    private static int intSetting(Settings settings, String key, int fallback) {
        try {
            return settings.get(key).map(Integer::parseInt).orElse(fallback);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            throw new IllegalStateException("corrupt status projection", e);
        }
    }
}
