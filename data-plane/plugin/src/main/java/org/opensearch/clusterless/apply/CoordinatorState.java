/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.clusterless.ClusterlessIndexSettings;
import org.opensearch.clusterless.contract.IndexStatus;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.NodeStatus;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.ShardState;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Adds the shards of coordinated tenants — on <em>other</em> nodes — to whatever this node built for itself.
 *
 * <p>A decorator rather than a second fabricator, because a node has one cluster state. A node that both holds
 * copies of an index and coordinates for its tenant needs a routing table containing its own copies <b>and</b> the
 * others', which is what a real cluster state looks like; two states would be neither. Wrapping means the data path
 * is exactly the one already tested, and everything here is additive.
 *
 * <p>It is also the shape to keep if coordinators later run on dedicated capacity, which is where they belong —
 * their traffic and hardware are not a data node's. A pure coordinator is then this decorating a
 * {@link DataNodeState} that was given nothing to hold, and the two roles stay uncoupled either way.
 *
 * <h2>The three things that have to be exactly right</h2>
 *
 * <ul>
 *   <li><b>This node's own routing entries are never touched.</b> They carry allocation ids and states that came
 *       from the server, and rebuilding one restarts a serving shard — D2.2, the most expensive mistake available
 *       here. Copies the projection reports <em>on this node</em> are therefore skipped outright: the assignment is
 *       authoritative for what this node holds, and the projection is the supervisor's older observation of it.
 *   <li><b>Other nodes' entries are carried forward when nothing about them changed.</b> Not for efficiency:
 *       {@code initialize()} generates a fresh allocation id, so rebuilding them every pass would make the routing
 *       table differ from the last one for ever, and this node would publish a new cluster state every poll
 *       interval — exactly the bug M5 found, in a new place.
 *   <li><b>Every node named in the routing table is in {@link DiscoveryNodes}.</b> {@code RoutingNodes} looks up
 *       each shard's node and builds a {@code RoutingNode} around whatever it finds, so a routing entry naming a
 *       node the state does not contain fails an invariant. A copy whose node cannot be built is dropped with a
 *       reason instead.
 * </ul>
 *
 * <p><b>And the count that must match the table.</b> {@code index.number_of_search_replicas} has to describe the
 * routing table in the same state, because {@code cluster.routing.search_replica.strict} makes
 * {@code OperationRouting} inject a search-replica preference for any index whose metadata claims one. On a data
 * node that means the local view, which is what {@link DataNodeState} sets; here the table holds every copy, so it
 * means the fleet's. The same rule, and the opposite number.
 */
public final class CoordinatorState implements NodeState {

    private static final Logger logger = LogManager.getLogger(CoordinatorState.class);

    private final NodeState delegate;
    private final DiscoveryNode localNode;
    private final CoordinatorView view;
    private final MappingSource mappings;

    public CoordinatorState(NodeState delegate, DiscoveryNode localNode, CoordinatorView view, MappingSource mappings) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.view = Objects.requireNonNull(view, "view");
        this.mappings = Objects.requireNonNull(mappings, "mappings");
    }

    @Override
    public ClusterState buildClusterState(ClusterState previous) {
        ClusterState held = delegate.buildClusterState(previous);
        if (view.isEmpty()) {
            return held;
        }

        Metadata.Builder metadata = Metadata.builder(held.metadata());
        RoutingTable.Builder routing = RoutingTable.builder(held.routingTable());
        Set<NodeId> reached = new LinkedHashSet<>();

        for (CoordinatorView.RoutedIndex index : view.indices()) {
            try {
                route(index, held, previous, metadata, routing, reached);
            } catch (Exception e) {
                // One index's routing is one index's problem, for the same reason a fabrication failure is
                // contained per index: an exception here would otherwise cost the node every other index too.
                logger.error("clusterless: could not build coordinator routing for [{}]", index.uuid(), e);
            }
        }

        return ClusterState.builder(held).nodes(nodes(held, reached)).metadata(metadata).routingTable(routing.build()).build();
    }

    private void route(
        CoordinatorView.RoutedIndex routed,
        ClusterState held,
        ClusterState previous,
        Metadata.Builder metadata,
        RoutingTable.Builder routing,
        Set<NodeId> reached
    ) {
        String indexName = IndexNaming.indexName(routed.tenant(), routed.uuid(), routed.config().name());
        Index index = new Index(indexName, routed.uuid());

        IndexRoutingTable existing = held.routingTable().index(index);
        IndexRoutingTable.Builder indexRouting = IndexRoutingTable.builder(index);

        // This node's own copies, verbatim from what the data-node fabrication decided. Rebuilding one of these is
        // the mistake D2.2 is about, so they are copied across untouched.
        boolean heldHere = false;
        if (existing != null) {
            for (IndexShardRoutingTable shard : existing) {
                for (ShardRouting copy : shard) {
                    indexRouting.addShard(copy);
                    heldHere |= localNode.getId().equals(copy.currentNodeId());
                }
            }
        }

        int searchReplicas = 0;
        int replicas = 0;
        for (Map.Entry<Integer, List<IndexStatus.Copy>> shard : routed.placement().shards().entrySet()) {
            for (IndexStatus.Copy copy : shard.getValue()) {
                if (localNode.getId().equals(copy.node().value())) {
                    // Mine. Already in the table above, from the assignment, which is the authority for this node
                    // -- the projection is the supervisor's observation of it and may be a pass behind.
                    continue;
                }
                ShardRouting elsewhere = routingFor(new ShardId(index, shard.getKey()), copy, previous, index);
                if (elsewhere == null) {
                    continue;
                }
                indexRouting.addShard(elsewhere);
                reached.add(copy.node());
                if (copy.role() == ShardRole.SEARCH_REPLICA) {
                    searchReplicas++;
                } else if (copy.role() == ShardRole.REPLICA) {
                    replicas++;
                }
            }
        }

        // Whose metadata this is. If this node holds a copy, the data path built it -- from the assignment, the
        // local view of the copy counts, and whatever the remote store contributed -- and a coordinator has no
        // business overwriting that. If it holds none, the metadata is the coordinator's own, and it has to be
        // rebuilt each pass rather than left alone: DataNodeState *retains* the metadata of an index it holds no
        // copy of, so a settings change would otherwise never reach the routing view.
        if (heldHere == false) {
            metadata.put(metadataFor(routed, index, replicas, searchReplicas, held.metadata().index(index)), false);
        }
        routing.add(indexRouting);
    }

    /**
     * A routing entry for a copy on another node: the previous one where it still describes the same thing, a
     * fresh one otherwise.
     *
     * <p>Reused on (node, role, state) rather than rebuilt, because {@code initialize} mints an allocation id and a
     * table that differs every pass makes this node publish a state every pass. Nothing reads these allocation ids
     * — a coordinator tracks no in-sync set — so the only thing they have to be is stable.
     */
    private ShardRouting routingFor(ShardId shardId, IndexStatus.Copy copy, ClusterState previous, Index index) {
        boolean primary = copy.role() == ShardRole.PRIMARY;
        boolean searchOnly = copy.role() == ShardRole.SEARCH_REPLICA;
        if (copy.role() == ShardRole.UNKNOWN) {
            logger.debug("clusterless: not routing to {} on [{}], whose role this build does not understand", shardId, copy.node());
            return null;
        }
        if (copy.state() != ShardState.STARTED && copy.state() != ShardState.INITIALIZING) {
            // FAILED occupies disk and cannot serve, and UNKNOWN cannot be mapped to a routing state at all.
            return null;
        }
        if (view.peers().containsKey(copy.node()) == false) {
            return null;
        }

        IndexRoutingTable previousRouting = previous.routingTable().index(index);
        if (previousRouting != null) {
            IndexShardRoutingTable previousShard = previousRouting.shard(shardId.id());
            if (previousShard != null) {
                for (ShardRouting candidate : previousShard) {
                    boolean sameNode = copy.node().value().equals(candidate.currentNodeId());
                    boolean sameRole = candidate.primary() == primary && candidate.isSearchOnly() == searchOnly;
                    boolean sameState = candidate.started() == (copy.state() == ShardState.STARTED);
                    if (sameNode && sameRole && sameState) {
                        return candidate;
                    }
                }
            }
        }

        ShardRouting fresh = ShardRouting.newUnassigned(
            shardId,
            primary,
            searchOnly,
            // Inert: this node is not recovering somebody else's copy. A started entry drops it entirely below.
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "observed by the supervisor on another node")
        ).initialize(copy.node().value(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        return copy.state() == ShardState.STARTED ? fresh.moveToStarted() : fresh;
    }

    /**
     * Metadata for an index this node holds no part of.
     *
     * <p>It still needs a shard count to route by and a mapping to answer a {@code _mapping} request with, and it
     * needs the identity settings for a different reason: {@code TenantIndexNameResolver} maps a tenant's own index
     * name onto the fabricated one by reading them, so without them a coordinator would only answer to the
     * fabricated name — which is the thing the mapping exists to hide.
     */
    private IndexMetadata metadataFor(
        CoordinatorView.RoutedIndex routed,
        Index index,
        int replicas,
        int searchReplicas,
        IndexMetadata previous
    ) {
        Settings.Builder settings = Settings.builder();
        settings.loadFromMap(Map.copyOf(routed.config().settings()));
        settings.put(IndexMetadata.SETTING_INDEX_UUID, routed.uuid());
        settings.put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        settings.put(IndexMetadata.SETTING_CREATION_DATE, DataNodeState.creationDate(routed.config()));
        settings.put(ClusterlessIndexSettings.TENANT.getKey(), routed.tenant().value());
        settings.put(ClusterlessIndexSettings.TENANT_INDEX_NAME.getKey(), routed.config().name());
        settings.put(ClusterlessIndexSettings.CONFIG_REVISION.getKey(), routed.config().revision());

        // Describing this state's own routing table, which here holds every copy in the fleet. The same rule
        // DataNodeState follows and the opposite number, because its table holds only what it is.
        settings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas);
        settings.put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, searchReplicas);

        IndexMetadata.Builder builder = IndexMetadata.builder(index.getName()).settings(settings);
        var mapping = mappings.mappingFor(index, routed.config().mappings());
        if (mapping != null) {
            builder.putMapping(mapping);
        }
        for (Integer shard : routed.placement().shards().keySet()) {
            builder.primaryTerm(shard, 1L);
        }
        if (previous != null) {
            // Carried forward for the reason DataNodeState carries them: these are how the server decides what to
            // reload, and a version that reset every pass would say that everything about the index had changed.
            builder.version(previous.getVersion())
                .settingsVersion(previous.getSettingsVersion())
                .mappingVersion(previous.getMappingVersion());
            IndexMetadata built = builder.build();
            if (previous.equals(built) == false) {
                builder.version(previous.getVersion() + 1);
                if (previous.getSettings().equals(built.getSettings()) == false) {
                    builder.settingsVersion(previous.getSettingsVersion() + 1);
                }
                if (Objects.equals(previous.mapping(), built.mapping()) == false) {
                    builder.mappingVersion(previous.getMappingVersion() + 1);
                }
            }
        }
        return builder.build();
    }

    /**
     * This node plus every peer a routing entry names.
     *
     * <p>The first fabrication here that contains a node other than the local one, and the reason it can is the
     * ephemeral id in the node projection: {@code DiscoveryNode.equals} compares only that field, and the
     * transport refuses a connection whose handshake returns a node unequal to the one it was given. A peer built
     * without it is unequal to the real one by construction.
     */
    private DiscoveryNodes nodes(ClusterState held, Set<NodeId> reached) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(held.nodes());
        for (NodeId nodeId : reached) {
            NodeStatus peer = view.peers().get(nodeId);
            if (peer == null || nodes.get(nodeId.value()) != null) {
                continue;
            }
            DiscoveryNode built = peerFor(peer);
            if (built == null) {
                continue;
            }
            try {
                nodes.add(built);
            } catch (RuntimeException e) {
                // DiscoveryNodes.Builder refuses two nodes sharing an address, among other things. One peer this
                // node cannot represent must cost that peer's shards rather than every index in the state.
                logger.warn("clusterless: could not add peer [{}] to the fabricated state", peer.nodeId(), e);
            }
        }
        return nodes.build();
    }

    private DiscoveryNode peerFor(NodeStatus peer) {
        try {
            Set<DiscoveryNodeRole> roles = peer.holdsData() ? Set.of(DiscoveryNodeRole.DATA_ROLE) : Set.of();
            return new DiscoveryNode(
                peer.name() == null ? peer.nodeId().value() : peer.name(),
                peer.nodeId().value(),
                peer.ephemeralId(),
                peer.address(),
                peer.address(),
                new TransportAddress(InetAddress.getByName(peer.address()), peer.port()),
                peer.attributes(),
                roles,
                versionOf(peer)
            );
        } catch (UnknownHostException | RuntimeException e) {
            logger.warn("clusterless: could not build a peer for [{}] from its projection", peer.nodeId(), e);
            return null;
        }
    }

    /**
     * The peer's version, or this node's.
     *
     * <p>Falling back rather than failing, because a version string this build cannot parse comes from a node
     * newer than it, and refusing to route to it would make an upgrade look like an outage. Nothing about
     * connecting depends on this being right — node equality is the ephemeral id alone — and the transport
     * negotiates what it actually needs.
     */
    private static Version versionOf(NodeStatus peer) {
        try {
            return peer.version() == null ? Version.CURRENT : Version.fromString(peer.version());
        } catch (RuntimeException e) {
            return Version.CURRENT;
        }
    }

    /** Copies this node routes to, for the report and the local status endpoint. */
    public List<String> describe() {
        List<String> lines = new ArrayList<>();
        for (CoordinatorView.RoutedIndex index : view.indices()) {
            lines.add(
                index.tenant() + "/" + index.config().name() + ": " + index.placement().allCopies().size()
                    + " copy(ies) across " + index.placement().shards().size() + " shard(s)"
            );
        }
        return lines;
    }
}
