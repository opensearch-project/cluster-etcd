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
import org.opensearch.cluster.metadata.IngestionStatus;
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
import org.opensearch.clusterless.contract.IndexConfig;
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
    private final TrackedInSync trackedInSync;

    public CoordinatorState(NodeState delegate, DiscoveryNode localNode, CoordinatorView view, MappingSource mappings) {
        this(delegate, localNode, view, mappings, TrackedInSync.UNKNOWN);
    }

    public CoordinatorState(
        NodeState delegate,
        DiscoveryNode localNode,
        CoordinatorView view,
        MappingSource mappings,
        TrackedInSync trackedInSync
    ) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.view = Objects.requireNonNull(view, "view");
        this.mappings = Objects.requireNonNull(mappings, "mappings");
        this.trackedInSync = Objects.requireNonNull(trackedInSync, "trackedInSync");
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
        Set<Integer> shards = new java.util.HashSet<>();
        Set<Integer> shardsWithAPrimary = new java.util.HashSet<>();
        Map<Integer, Integer> searchOnlyPerShard = new java.util.HashMap<>();
        if (existing != null) {
            for (IndexShardRoutingTable shard : existing) {
                for (ShardRouting copy : shard) {
                    indexRouting.addShard(copy);
                    heldHere |= localNode.getId().equals(copy.currentNodeId());
                    count(shards, shardsWithAPrimary, searchOnlyPerShard, copy.id(), copy.primary(), copy.isSearchOnly());
                }
            }
        }

        int searchReplicas = 0;
        int replicas = 0;
        for (Integer shardNumber : routed.shards()) {
            for (IndexStatus.Copy copy : routed.placement().copiesOf(shardNumber)) {
                if (localNode.getId().equals(copy.node().value())) {
                    // Mine. Already in the table above, from the assignment, which is the authority for this node
                    // -- the projection is the supervisor's observation of it and may be a pass behind.
                    continue;
                }
                if (routed.needs(shardNumber, copy.role()) == false) {
                    // A copy this node has no relationship with: another search replica of a shard it also holds a
                    // search replica of, or any peer at all of a shard it holds the primary of. Leaving it out is
                    // what keeps a five-copy shard from putting five entries in five nodes' states -- and the count
                    // below follows the table rather than the projection, so the two cannot disagree.
                    continue;
                }
                ShardRouting elsewhere = routingFor(new ShardId(index, shardNumber), copy, previous, index, documentReplicated(routed));
                if (elsewhere == null) {
                    continue;
                }
                indexRouting.addShard(elsewhere);
                reached.add(copy.node());
                count(shards, shardsWithAPrimary, searchOnlyPerShard, shardNumber, elsewhere.primary(), elsewhere.isSearchOnly());
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
        IndexRoutingTable table = indexRouting.build();
        if (heldHere == false) {
            metadata.put(metadataFor(routed, index, replicas, searchReplicas, held.metadata().index(index)), false);
        } else {
            boolean everyShardHasAPrimary = shards.isEmpty() == false && shardsWithAPrimary.containsAll(shards);
            int searchOnly = searchOnlyPerShard.values().stream().mapToInt(Integer::intValue).max().orElse(0);
            // Whether the marker is this node's or the tenant's. The control plane can set
            // index.blocks.search_only deliberately -- a rolled-over daily index has no writer and its search
            // replicas should stop polling for updates that are not coming -- and that is the flag's real meaning.
            // Removing it because a primary is in the routing table would overrule the operator and restart the
            // polling they asked to stop.
            boolean markerIsTheTenants = Boolean.parseBoolean(
                routed.config().settings().getOrDefault(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), "false")
            );
            metadata.put(
                describingTheWholeTable(
                    held.metadata().index(index),
                    previous.metadata().index(index),
                    searchOnly,
                    everyShardHasAPrimary && markerIsTheTenants == false,
                    documentReplicated(routed) ? inSyncOfPrimariesHere(table) : Map.of()
                ),
                false
            );
        }
        routing.add(table);
    }

    /**
     * For each shard whose document-replication primary is on this node, the copies to call in sync: the primary, and
     * the copies in this routing table that the primary's own tracker holds in sync. Absent for a shard whose tracker
     * cannot say, which leaves the data path's set of the primary alone.
     *
     * <p>Built here rather than in {@link DataNodeState} because this is where the whole table is, and the intersection
     * with it is the part that must not be skipped. The tracker keeps a copy for as long as the cluster state calls it
     * in sync, so a set fed back from the tracker alone would keep a copy that has left the table in it for ever, and
     * with it the global checkpoint. That is the shape of the in-sync set that grew without bound in M4.
     *
     * <p>Initializing copies count: the primary marks a copy in sync as its recovery finishes, before the copy's own
     * node has started it, and {@code ReplicationTracker.updateFromClusterManager} takes such an id as initializing
     * and in sync.
     */
    private Map<Integer, Set<String>> inSyncOfPrimariesHere(IndexRoutingTable table) {
        Map<Integer, Set<String>> inSync = new java.util.TreeMap<>();
        for (IndexShardRoutingTable shard : table) {
            ShardRouting primary = shard.primaryShard();
            if (primary == null || localNode.getId().equals(primary.currentNodeId()) == false) {
                continue;
            }
            Set<String> tracked = trackedInSync.of(shard.shardId());
            if (tracked == null) {
                continue;
            }
            Set<String> ids = new java.util.TreeSet<>();
            ids.add(primary.allocationId().getId());
            for (ShardRouting copy : shard.assignedShards()) {
                if (tracked.contains(copy.allocationId().getId())) {
                    ids.add(copy.allocationId().getId());
                }
            }
            inSync.put(shard.shardId().id(), ids);
        }
        return inSync;
    }

    static boolean documentReplicated(CoordinatorView.RoutedIndex routed) {
        return "DOCUMENT".equals(routed.config().replication());
    }

    /**
     * Whether a copy on another node gets a routing entry in this node's state.
     *
     * <p>The one test, and {@link PeerRecovery} uses it too: a replica built against a primary that this class then
     * leaves out is one applied state away from the {@code RoutingNodes} assertion that every shard has a primary.
     */
    public static boolean routable(IndexStatus.Copy copy, boolean documentReplicated, Map<NodeId, ?> peers) {
        if (copy.role() == ShardRole.UNKNOWN) {
            return false;
        }
        if (copy.state() != ShardState.STARTED && copy.state() != ShardState.INITIALIZING) {
            // FAILED occupies disk and cannot serve, and UNKNOWN cannot be mapped to a routing state at all.
            return false;
        }
        if (peers.containsKey(copy.node()) == false) {
            return false;
        }
        // Under document replication, the allocation id is what peer recovery and the primary's tracker key on, and only
        // the copy's own node knows it. A copy whose node published none gets no entry rather than a minted id, which
        // nothing would ever match.
        return documentReplicated == false || copy.allocationId() != null;
    }

    /**
     * The allocation id a copy on another node gets here: the one its own node published, under document replication.
     * Null otherwise, which means mint one. Nothing reads those -- a coordinator tracks no in-sync set, and a search
     * replica recovers from the remote store -- so they only have to be stable, which reusing the entry makes them.
     */
    public static String allocationIdFor(IndexStatus.Copy copy, boolean documentReplicated) {
        return documentReplicated ? copy.allocationId() : null;
    }

    /**
     * A routing entry for a copy on another node: the previous one where it still describes the same thing, a
     * fresh one otherwise.
     *
     * <p>Reused on (node, role, state) rather than rebuilt, because a table that differs every pass makes this node
     * publish a state every pass -- and under document replication on the allocation id as well, since a copy its node
     * built again is a different copy with a different id.
     */
    private ShardRouting routingFor(ShardId shardId, IndexStatus.Copy copy, ClusterState previous, Index index, boolean documentReplicated) {
        boolean primary = copy.role() == ShardRole.PRIMARY;
        boolean searchOnly = copy.role() == ShardRole.SEARCH_REPLICA;
        if (routable(copy, documentReplicated, view.peers()) == false) {
            logger.debug("clusterless: not routing to {} on [{}]: {}", shardId, copy.node(), copy);
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
                    boolean sameCopy = documentReplicated == false || candidate.allocationId().getId().equals(copy.allocationId());
                    if (sameNode && sameRole && sameState && sameCopy) {
                        return candidate;
                    }
                }
            }
        }

        ShardRouting fresh = ShardRouting.newUnassigned(
            shardId,
            primary,
            searchOnly,
            // Which source is required rather than chosen. ShardRouting asserts that a copy which is neither a
            // primary nor search-only recovers from a peer -- "replica shards always recover from primary" -- so a
            // document-replication replica must say so even in another node's entry, where it is inert: this node is
            // not the one recovering it, and a started entry drops the source entirely below. The other two roles
            // are exempt from that assertion, and an empty store is the honest "not my business" for them.
            primary || searchOnly
                ? RecoverySource.EmptyStoreRecoverySource.INSTANCE
                : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "observed by the supervisor on another node")
        ).initialize(copy.node().value(), allocationIdFor(copy, documentReplicated), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        return copy.state() == ShardState.STARTED ? fresh.moveToStarted() : fresh;
    }

    /**
     * The data path's metadata, with the one count that has to describe the table it ships in.
     *
     * <p>{@link DataNodeState} sets {@code index.number_of_search_replicas} from what <em>it</em> holds, which is
     * right for the state it builds and wrong for this one: a moment later this class adds every other node's copies
     * to the same routing table. Leaving it alone was a real defect rather than a tidiness question, and it was
     * visible in a running deployment — a node holding a primary and coordinating showed
     * {@code number_of_search_replicas: 0} beside a routing table containing a started search replica, so
     * {@code cluster.routing.search_replica.strict} had nothing to act on and searches were spread across primaries
     * and search replicas instead of preferring the replicas. Which is the thing search replicas exist for.
     *
     * <h2>And the marker, which turns out to cost freshness</h2>
     *
     * <p>{@link DataNodeState} sets {@code index.blocks.search_only} when this node holds nothing but search
     * replicas of an index, to satisfy {@code RoutingNodes}' assertion that a shard has a primary. That flag means
     * something else upstream — an index scaled down to read-only — and the rest of its meaning comes along:
     * {@code IndexService.AsyncReplicationTask.shouldRun()} is {@code false == INDEX_BLOCKS_SEARCH_ONLY}, and that
     * task is what calls {@code maybeSyncSegments} for active search-only shards on the refresh interval. So the
     * marker <b>disables the timer that keeps a search replica current</b>, which upstream is right about (a
     * scaled-down index has no writer to follow) and is wrong for a copy whose primary is merely on another node.
     *
     * <p>The marker exists only because the primary was missing from the table, so once the table has one it is
     * removed — and the periodic sync resumes. <b>Unless the tenant asked for it</b>: the control plane may set
     * {@code index.blocks.search_only} deliberately, which is the flag's real meaning — a rolled-over daily index
     * has no writer, and its search replicas should stop polling for updates that are not coming. Removing that
     * because a primary happens to be in the table would overrule an operator and restart the polling they asked to
     * stop, so only a marker this plugin added is taken away. Which makes coordinating on a search-replica node worth more than
     * routing: it is what lets that node's copies stay fresh without depending on a primary that has never heard
     * of them.
     *
     * <p>Nothing else the data path decided is touched — the remote-store settings, the mappings it canonicalised,
     * the identity — and this has no business restating any of it.
     */
    private static void count(
        Set<Integer> shards,
        Set<Integer> shardsWithAPrimary,
        Map<Integer, Integer> searchOnlyPerShard,
        int shard,
        boolean primary,
        boolean searchOnly
    ) {
        shards.add(shard);
        if (primary) {
            shardsWithAPrimary.add(shard);
        }
        if (searchOnly) {
            searchOnlyPerShard.merge(shard, 1, Integer::sum);
        }
    }

    private static IndexMetadata describingTheWholeTable(
        IndexMetadata held,
        IndexMetadata applied,
        int searchOnlyInTable,
        boolean everyShardHasAPrimary,
        Map<Integer, Set<String>> inSync
    ) {
        Settings.Builder settings = Settings.builder().put(held.getSettings());
        settings.put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, searchOnlyInTable);
        if (everyShardHasAPrimary) {
            settings.remove(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey());
        }
        Settings shipped = settings.build();

        if (applied == null) {
            return withInSync(IndexMetadata.builder(held).settings(shipped), inSync).build();
        }
        if (org.opensearch.index.IndexSettings.same(shipped, applied.getSettings())) {
            // The settings this state ships are the ones already applied, so the version must not move. Bumping it
            // anyway is fatal rather than untidy: IndexService.updateMetadata asserts that a moved settings version
            // means the settings differ, and an AssertionError inside the applier takes the node down.
            //
            // Reached because the two stages disagree by design and then agree: the data path rebuilds this index's
            // settings from the assignment each pass, without the search-replica count or the dropped marker that
            // this class adds -- so it sees a difference from the previous state, bumps for it, and this class then
            // restores exactly what was applied. The net change is nothing and the version had moved twice.
            //
            // Compared with the server's own IndexSettings.same rather than Settings.equals, because that is the
            // predicate deciding whether it will treat this as an update -- approximating it is how the version and
            // the settings come to disagree again.
            //
            // The metadata version, though, moves by one when anything else differs -- the in-sync set, or a term -- as it
            // does in OpenSearch, and stays put only when nothing does. It used to stay put regardless.
            IndexMetadata.Builder unchanged = withInSync(IndexMetadata.builder(held).settings(shipped), inSync)
                .settingsVersion(applied.getSettingsVersion())
                .version(applied.getVersion());
            IndexMetadata candidate = unchanged.build();
            return candidate.equals(applied) ? candidate : unchanged.version(applied.getVersion() + 1).build();
        }
        return withInSync(IndexMetadata.builder(held).settings(shipped), inSync)
            .settingsVersion(applied.getSettingsVersion() + 1)
            .version(Math.max(held.getVersion(), applied.getVersion() + 1))
            .build();
    }

    private static IndexMetadata.Builder withInSync(IndexMetadata.Builder builder, Map<Integer, Set<String>> inSync) {
        inSync.forEach(builder::putInSyncAllocationIds);
        return builder;
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
        // As DataNodeState states it, so the two never describe one index differently.
        if (routed.config().settings().containsKey(IndexConfig.REPLICATION_TYPE) == false) {
            settings.put(IndexConfig.REPLICATION_TYPE, IndexConfig.DEFAULT_REPLICATION);
        }
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
        // Nothing on this node ingests for an index it holds no copy of, so this changes no behaviour here. It is
        // set because the alternative is a coordinator reporting a paused index as running -- the same class of
        // inconsistency as number_of_search_replicas being 0 beside a started search replica, which is the M8
        // defect that was found by reading the output rather than by anything failing.
        builder.ingestionStatus(new IngestionStatus(routed.config().ingestionPaused()));
        var mapping = mappings.mappingFor(index, routed.config().mappings());
        if (mapping != null) {
            builder.putMapping(mapping);
        }
        for (Integer shard : routed.shards()) {
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
                // Belt and braces. The cases that used to reach here -- a lost node, a projection with no ephemeral
                // id, two ids claiming one endpoint -- are all decided in the pass now, with a reason recorded, so
                // anything arriving here is something neither this class nor CoordinatorViewSource anticipated.
                //
                // At debug rather than warn deliberately: this runs on every pass, so a per-peer warn is a line per
                // second for as long as the condition lasts, which is exactly what it produced. What an operator
                // needs to read is in the pass's deferred list, which reaches the status endpoint and the
                // heartbeat and is logged only when the intended state changes.
                logger.debug("clusterless: could not add peer [{}] to the fabricated state", peer.nodeId(), e);
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
