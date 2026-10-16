/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.cluster.StatusProjections.IndexHealth;
import org.opensearch.controlplane.handler.indices.IndexStore;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.rest.Json;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * The sections of a {@code cluster.state} response, each rendered in OpenSearch's shape and field order
 * (checked against a node's serialisation of {@code ClusterState}, 3.10.0-SNAPSHOT).
 *
 * <p>What is left out is left out because this service has no honest value for it, not because it is hard:
 * the per-index {@code version}, {@code mapping_version}, {@code settings_version}, {@code aliases_version} and
 * {@code routing_num_shards} counters, {@code primary_terms} (the supervisor holds those, outside the tenant's
 * key space) and {@code split_shards_metadata}; {@code unassigned_info} and {@code recovery_source} on a shard,
 * which record why and when and from where, none of which is projected; and every metadata custom
 * ({@code index-graveyard}, composable and component templates, data streams, stored scripts, pipelines,
 * repositories). See {@link ClusterStateHandler}.
 */
final class ClusterStateSections {

    private ClusterStateSections() {}

    // ------------------------------------------------------------------ blocks

    /** {@code ClusterBlockLevel}, in enum order, which is the order upstream writes them in. */
    private static final List<String> LEVELS = List.of("read", "write", "metadata_read", "metadata_write", "create_index");

    private record Block(int id, String description, boolean retryable, List<String> levels) {

        ObjectNode render() {
            ObjectNode node = Json.object();
            node.put("description", description);
            node.put("retryable", retryable);
            ArrayNode levelNodes = node.putArray("levels");
            LEVELS.stream().filter(levels::contains).forEach(levelNodes::add);
            return node;
        }
    }

    /** Upstream's no-cluster-manager block under the default {@code cluster.no_master_block=write}. */
    private static final Block NO_CLUSTER_MANAGER = new Block(2, "no cluster-manager", true, List.of("write", "metadata_write"));

    /** Index blocks by the setting that sets them, with {@code IndexMetadata}'s ids, descriptions and levels. */
    private static final Map<String, Block> SETTING_BLOCKS = new LinkedHashMap<>();

    static {
        SETTING_BLOCKS.put("index.blocks.read_only",
            new Block(5, "index read-only (api)", false, List.of("write", "metadata_write")));
        SETTING_BLOCKS.put("index.blocks.read", new Block(7, "index read (api)", false, List.of("read")));
        SETTING_BLOCKS.put("index.blocks.write", new Block(8, "index write (api)", false, List.of("write")));
        SETTING_BLOCKS.put("index.blocks.metadata",
            new Block(9, "index metadata (api)", false, List.of("metadata_read", "metadata_write")));
        SETTING_BLOCKS.put("index.blocks.read_only_allow_delete", new Block(12,
            "disk usage exceeded flood-stage watermark, index has read-only-allow-delete block", false,
            List.of("write", "metadata_write")));
        SETTING_BLOCKS.put("index.blocks.search_only", new Block(20, "index scaled down", false, List.of("write")));
    }

    /** {@code MetadataIndexStateService.INDEX_CLOSED_BLOCK}. */
    private static final Block INDEX_CLOSED = new Block(4, "index closed", false, List.of("read", "write"));

    /**
     * {@code global} only when nothing is managing the tenant; {@code indices} only for indices with a block;
     * the whole object empty when there are none, which upstream writes as {@code {}}.
     */
    static ObjectNode blocks(Collection<IndexHealth> indices, boolean noClusterManager) {
        ObjectNode blocks = Json.object();
        if (noClusterManager) {
            blocks.putObject("global").set(Integer.toString(NO_CLUSTER_MANAGER.id()), NO_CLUSTER_MANAGER.render());
        }
        ObjectNode byIndex = Json.object();
        for (IndexHealth index : indices) {
            NavigableMap<Integer, Block> blocked = new TreeMap<>();
            if (isClosed(index.document())) {
                blocked.put(INDEX_CLOSED.id(), INDEX_CLOSED);
            }
            Settings settings = Settings.forIndex(index.document().path("settings"));
            SETTING_BLOCKS.forEach((setting, block) -> {
                if (settings.getAsBoolean(setting, false)) {
                    blocked.put(block.id(), block);
                }
            });
            if (blocked.isEmpty() == false) {
                ObjectNode entry = byIndex.putObject(index.name());
                blocked.forEach((id, block) -> entry.set(Integer.toString(id), block.render()));
            }
        }
        if (byIndex.isEmpty() == false) {
            blocks.set("indices", byIndex);
        }
        return blocks;
    }

    // ------------------------------------------------------------------ nodes

    /**
     * {@code DiscoveryNode.toXContent}: name, ephemeral id, transport address and attributes, keyed by node id.
     * No roles, version or host: a cluster state does not carry them.
     */
    static ObjectNode nodes(NavigableMap<String, JsonNode> nodeDocuments) {
        ObjectNode nodes = Json.object();
        nodeDocuments.forEach((id, node) -> {
            ObjectNode entry = nodes.putObject(id);
            entry.put("name", node.path("name").asText());
            entry.put("ephemeral_id", node.path("ephemeral_id").asText());
            entry.put("transport_address", node.path("address").asText() + ":" + node.path("port").asInt());
            ObjectNode attributes = entry.putObject("attributes");
            new TreeMap<String, JsonNode>(toMap(node.path("attributes"))).forEach(attributes::set);
        });
        return nodes;
    }

    // ------------------------------------------------------------------ metadata

    /**
     * {@code Metadata.toXContent} in the API context.
     *
     * <p>Persistent and transient settings are not part of it: upstream writes them only in the gateway
     * context, so {@code GET _cluster/state} never shows them and {@code GET _cluster/settings} is how they
     * are read.
     *
     * @param filtered whether an index expression was given. Upstream then builds a fresh {@code Metadata}
     *     holding only the matching indices, so {@code cluster_uuid_committed} reads false and there are no
     *     templates, which is reproduced rather than papered over
     */
    static ObjectNode metadata(
        String clusterUuid,
        boolean filtered,
        NavigableMap<String, JsonNode> legacyTemplates,
        Collection<IndexHealth> indices,
        Map<String, List<String>> aliasesByIndex,
        boolean flatSettings
    ) {
        ObjectNode metadata = Json.object();
        metadata.put("cluster_uuid", clusterUuid);
        metadata.put("cluster_uuid_committed", filtered == false);

        // No voting configuration exists: consensus belongs to the store, not to a quorum of nodes of this
        // service. See the README's note on voting config exclusions.
        ObjectNode coordination = metadata.putObject("cluster_coordination");
        coordination.put("term", 0);
        coordination.putArray("last_committed_config");
        coordination.putArray("last_accepted_config");
        coordination.putArray("voting_config_exclusions");

        ObjectNode templates = metadata.putObject("templates");
        if (filtered == false) {
            legacyTemplates.forEach((name, definition) -> templates.set(name, legacyTemplate(definition, flatSettings)));
        }

        ObjectNode rendered = metadata.putObject("indices");
        for (IndexHealth index : indices) {
            rendered.set(index.name(), index(index, aliasesByIndex.getOrDefault(index.name(), List.of()), flatSettings));
        }
        return metadata;
    }

    /** {@code IndexTemplateMetadata}, typed: mappings keyed by type, aliases keyed by name. */
    private static ObjectNode legacyTemplate(JsonNode definition, boolean flatSettings) {
        ObjectNode template = Json.object();
        template.put("order", definition.path("order").asInt(0));
        if (definition.hasNonNull("version")) {
            template.set("version", definition.get("version").deepCopy());
        }
        template.set("index_patterns", definition.path("index_patterns").deepCopy());
        template.set("settings", Settings.forIndex(definition.path("settings")).toJson(flatSettings));
        template.set("mappings", typedMappings(definition.path("mappings")));
        JsonNode aliases = definition.path("aliases");
        template.set("aliases", aliases.isObject() ? aliases.deepCopy() : Json.object());
        return template;
    }

    /** {@code IndexMetadata.Builder.toXContent}, for the fields this service can stand behind. */
    private static ObjectNode index(IndexHealth index, List<String> aliases, boolean flatSettings) {
        JsonNode document = index.document();
        ObjectNode entry = Json.object();
        entry.put("state", isClosed(document) ? "close" : "open");
        entry.set("settings", IndexStore.reportedSettings(document).toJson(flatSettings));
        entry.set("mappings", typedMappings(document.path("mappings")));
        ArrayNode aliasNames = entry.putArray("aliases");
        aliases.forEach(aliasNames::add);

        // Carried from the primary copy's in-sync set, which the supervisor publishes for a document-replication
        // index only. Absent rather than empty elsewhere: an empty set would claim no copy is in sync.
        ObjectNode inSync = Json.object();
        index.shards().forEach((shard, copies) -> copies.stream()
            .filter(copy -> copy.inSync() != null)
            .findFirst()
            .ifPresent(primary -> {
                ArrayNode ids = inSync.putArray(Integer.toString(shard));
                primary.inSync().forEach(ids::add);
            }));
        if (inSync.isEmpty() == false) {
            entry.set("in_sync_allocations", inSync);
        }

        entry.putObject("rollover_info");
        entry.put("system", false);
        if (document.has("ingestion_status")) {
            entry.set("ingestion_status", document.get("ingestion_status").deepCopy());
        }
        return entry;
    }

    /**
     * Mappings the way the API writes them: keyed by type, always {@code _doc}. An index with none writes
     * {@code {}}, and so does one whose only mapping is an empty {@code _doc}.
     */
    private static ObjectNode typedMappings(JsonNode mappings) {
        ObjectNode typed = Json.object();
        if (mappings.isObject() == false || mappings.isEmpty()) {
            return typed;
        }
        boolean alreadyTyped = mappings.size() == 1 && mappings.has("_doc");
        JsonNode body = alreadyTyped ? mappings.get("_doc") : mappings;
        if (body.isObject() && body.isEmpty() == false) {
            typed.set("_doc", body.deepCopy());
        }
        return typed;
    }

    // ------------------------------------------------------------------ routing

    /** {@code RoutingTable.toXContent}: {@code indices} -> index -> {@code shards} -> shard id -> copies. */
    static ObjectNode routingTable(Collection<IndexHealth> indices) {
        ObjectNode byIndex = Json.object();
        Map<String, Map<Integer, ArrayNode>> grouped = new TreeMap<>();
        for (ShardRoutings.Routing routing : ShardRoutings.of(indices)) {
            grouped.computeIfAbsent(routing.index(), name -> new TreeMap<>())
                .computeIfAbsent(routing.shard(), shard -> Json.array())
                .add(routing.render());
        }
        grouped.forEach((index, shards) -> {
            ObjectNode rendered = byIndex.putObject(index).putObject("shards");
            shards.forEach((shard, copies) -> rendered.set(Integer.toString(shard), copies));
        });
        ObjectNode table = Json.object();
        table.set("indices", byIndex);
        return table;
    }

    /**
     * {@code RoutingNodes.toXContent}: the unassigned copies, then every node's copies.
     *
     * @param dataNodeIds listed with an empty array when they hold nothing, as upstream does when the
     *     {@code nodes} metric is also on; empty when it is not, which leaves only nodes that hold copies
     */
    static ObjectNode routingNodes(Collection<IndexHealth> indices, Collection<String> dataNodeIds) {
        ArrayNode unassigned = Json.array();
        NavigableMap<String, ArrayNode> nodes = new TreeMap<>();
        dataNodeIds.forEach(id -> nodes.put(id, Json.array()));
        for (ShardRoutings.Routing routing : ShardRoutings.of(indices)) {
            if (routing.unassigned()) {
                unassigned.add(routing.render());
            } else {
                nodes.computeIfAbsent(String.valueOf(routing.nodeId()), id -> Json.array()).add(routing.render());
            }
        }
        ObjectNode routingNodes = Json.object();
        routingNodes.set("unassigned", unassigned);
        ObjectNode rendered = routingNodes.putObject("nodes");
        nodes.forEach(rendered::set);
        return routingNodes;
    }

    // ------------------------------------------------------------------ helpers

    private static boolean isClosed(JsonNode document) {
        return "close".equals(document.path("state").asText("open"));
    }

    private static Map<String, JsonNode> toMap(JsonNode object) {
        Map<String, JsonNode> map = new LinkedHashMap<>();
        object.properties().forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        return map;
    }

    /** Lower-cased, for comparing roles that a newer supervisor might spell differently. */
    static boolean hasRole(JsonNode node, String role) {
        for (JsonNode each : node.path("roles")) {
            if (each.asText().toLowerCase(Locale.ROOT).equals(role)) {
                return true;
            }
        }
        return false;
    }
}
