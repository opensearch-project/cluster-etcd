/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionStatus;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.clusterless.ClusterlessIndexSettings;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The cluster state a data node would have, if it were told by a cluster manager.
 *
 * <p>This is the voice in the server's head. Everything downstream of a published cluster state — recovery,
 * allocation awareness, search routing, shard lifecycle — is code that already exists and already works; the
 * only thing missing in a clusterless deployment is something to hand it a state. So the value here is entirely
 * in <b>not</b> reimplementing any of that, and the risk is entirely in fabricating a state that differs from a
 * real one in some way the server notices.
 *
 * <p>Four of those ways are subtle enough to be worth naming, because each produces damage rather than an
 * error:
 *
 * <ul>
 *   <li><b>A routing table built from scratch restarts every shard.</b> The routing entry for a copy that is
 *       already serving has to be carried across from the previous state, allocation id and all. Rebuild it and
 *       the server sees a shard it has never heard of where a started one used to be, and recovers it — on
 *       every pass, for ever. This is what D2.2 is about, and it is the single most expensive thing to get
 *       wrong.
 *   <li><b>Metadata versions must move forward, and only on change.</b> {@code version},
 *       {@code settingsVersion} and {@code mappingVersion} are how the server decides what to reload. Resetting
 *       them makes everything look changed; never bumping them makes a real change look like nothing.
 *   <li><b>The creation date must not come from the clock.</b> {@code IndexMetadata} demands one, and a date
 *       generated per node would differ on every node and every restart — which is cluster-state churn that
 *       looks like a metadata change. It comes from the index document, which every node reads identically.
 *   <li><b>An empty-store recovery discards data.</b> Which recovery source a copy gets is the difference
 *       between resuming a shard and silently rebuilding it from nothing. See {@link LocalShardStore}.
 * </ul>
 *
 * <p>The state carries <b>only the local node</b> in its {@link DiscoveryNodes}. There is no cluster to
 * enumerate, and a data node has no business knowing the fleet's routing table — under document replication it
 * will need the immediate peers of its own shards, and nothing more, which is milestone 9.
 */
public final class DataNodeState implements NodeState {

    private static final Logger logger = LogManager.getLogger(DataNodeState.class);

    /**
     * Every fabricated primary term is 1, and that is a known gap rather than a simplification.
     *
     * <p>A primary term is what lets a superseded primary be fenced: a replica or a remote store can refuse a
     * write from an older term. Nothing in this contract carries one — there is no per-primary generation
     * anywhere in the key layout — so there is nothing to fabricate from, and D8.2 records the consequence
     * plainly: two nodes briefly believing they are primary for a remote-store-backed shard could write to the
     * same remote path. The supervisor avoids the situation by never moving or failing over a primary, which is
     * a scheduling constraint standing in for a correctness mechanism.
     *
     * <p>Carried forward from the previous state when the index already existed, so that a term never goes
     * backwards within one process even though it also never advances.
     */
    private static final long INITIAL_PRIMARY_TERM = 1L;

    private final ClusterName clusterName;
    private final DiscoveryNode localNode;
    private final DesiredState desired;
    private final MappingSource mappings;
    private final LocalShardStore shardStore;
    private final RemoteStore remoteStore;
    private final Set<Index> stopRetaining;
    private final Map<ShardId, RecoverySource> recoverySources;

    /**
     * @param clusterName this node's own, from its settings. Passed in rather than inherited from the previous
     *     state, which was a bug: the first fabrication of a process picks up whatever name the state it was
     *     handed happened to carry, and every state after that copies it. In practice that meant
     *     {@code ClusterName.DEFAULT} — "opensearch" — so {@code GET /_cluster/state} reported a different cluster
     *     from {@code GET /_cluster/health}, which reads its name from the settings. Two of this plugin's own
     *     answers disagreeing about which cluster they describe is worse than either being wrong.
     */
    public DataNodeState(
        ClusterName clusterName,
        DiscoveryNode localNode,
        DesiredState desired,
        MappingSource mappings,
        LocalShardStore shardStore
    ) {
        this(clusterName, localNode, desired, mappings, shardStore, RemoteStore.NONE, Set.of());
    }

    /**
     * @param stopRetaining indices whose metadata to leave out even though the previous state had it, which is what
     *     lets the server delete their files. Decided by {@link RetainedIndexReaper} once per pass and passed in
     *     already settled: this class is called twice on a pass that applies, so it must not be the thing counting.
     */
    public DataNodeState(
        ClusterName clusterName,
        DiscoveryNode localNode,
        DesiredState desired,
        MappingSource mappings,
        LocalShardStore shardStore,
        Set<Index> stopRetaining
    ) {
        this(clusterName, localNode, desired, mappings, shardStore, RemoteStore.NONE, stopRetaining, Map.of());
    }

    /**
     * @param remoteStore where this node's segments live, if not only on its own disk. Contributes the repository to
     *     register and the index settings that point a shard at it; {@link RemoteStore#NONE} for a node with none,
     *     which changes nothing about the state that is built.
     */
    public DataNodeState(
        ClusterName clusterName,
        DiscoveryNode localNode,
        DesiredState desired,
        MappingSource mappings,
        LocalShardStore shardStore,
        RemoteStore remoteStore,
        Set<Index> stopRetaining
    ) {
        this(clusterName, localNode, desired, mappings, shardStore, remoteStore, stopRetaining, Map.of());
    }

    /**
     * @param recoverySources recovery sources already looked up for copies about to be created, by
     *     {@link RemoteRecovery} once per pass. Consulted only where this class would otherwise choose an empty
     *     store, so a disk that has gained bytes since the lookup still wins: the two cannot disagree in the
     *     direction that discards data.
     */
    public DataNodeState(
        ClusterName clusterName,
        DiscoveryNode localNode,
        DesiredState desired,
        MappingSource mappings,
        LocalShardStore shardStore,
        RemoteStore remoteStore,
        Set<Index> stopRetaining,
        Map<ShardId, RecoverySource> recoverySources
    ) {
        this.clusterName = Objects.requireNonNull(clusterName, "clusterName");
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.desired = Objects.requireNonNull(desired, "desired");
        this.mappings = Objects.requireNonNull(mappings, "mappings");
        this.shardStore = Objects.requireNonNull(shardStore, "shardStore");
        this.remoteStore = Objects.requireNonNull(remoteStore, "remoteStore");
        this.stopRetaining = Set.copyOf(Objects.requireNonNull(stopRetaining, "stopRetaining"));
        this.recoverySources = recoverySources == null ? Map.of() : Map.copyOf(recoverySources);
    }

    @Override
    public ClusterState buildClusterState(ClusterState previous) {
        ClusterState.Builder state = ClusterState.builder(clusterName);
        // Each application has to look like a newer publication than the last, or the applier ignores it.
        state.version(previous.version() + 1);
        state.nodes(DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build());

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routing = RoutingTable.builder();

        if (remoteStore.repositories() != null) {
            // What makes the repository exist on this node: RepositoriesService applies cluster state and builds its
            // repositories from this custom. A real remote-store cluster has its cluster manager write the same
            // thing when a node joins; the node's own attributes are the same statement, made locally.
            metadata.putCustom(RepositoriesMetadata.TYPE, remoteStore.repositories());
        }

        Set<Index> stillWanted = new HashSet<>();
        for (DesiredState.DesiredIndex index : desired.indices().values()) {
            Index built = buildIndexOrKeepWhatIsThere(previous, index, metadata, routing);
            if (built != null) {
                stillWanted.add(built);
            }
        }
        retainIndicesNoLongerWanted(previous, stillWanted, metadata, routing);

        state.metadata(metadata);
        state.routingTable(routing.build());
        return state.build();
    }

    /**
     * Builds one index, or leaves it exactly as the previous state had it.
     *
     * <p>D2.4 in the place it is easiest to violate: <em>"One unrecoverable shard must never stop a node applying
     * the rest of its assignment."</em> That requirement is usually read as being about a shard that will not
     * recover, but it applies just as much to a shard this code cannot describe — and the second case is worse,
     * because a bug here is not confined to one index. Fabricating the whole state is a single operation, so an
     * exception anywhere in it aborts all of it, and the node then holds nothing new, applies nothing, and retries
     * the same failing fabrication for ever.
     *
     * <p>Which is exactly what happened: a mapping update on an index that had no mapping threw a
     * {@code NullPointerException} inside the applier, and the node spent the rest of its life re-initialising a
     * shard once per second. One index's problem became every index's problem, and the node's.
     *
     * <p>So a failure here costs that index a pass and nothing more. The previous state's version of it is carried
     * across untouched, which is the safe fallback in both directions: a copy already serving keeps serving, and —
     * more importantly — the index does not vanish from the metadata, because vanishing is what tells the server
     * to delete the files.
     */
    private Index buildIndexOrKeepWhatIsThere(
        ClusterState previous,
        DesiredState.DesiredIndex desiredIndex,
        Metadata.Builder metadata,
        RoutingTable.Builder routing
    ) {
        try {
            return buildIndex(previous, desiredIndex, metadata, routing);
        } catch (Exception e) {
            String indexName = IndexNaming.indexName(desiredIndex.tenant(), desiredIndex.uuid(), desiredIndex.config().name());
            Index index = new Index(indexName, desiredIndex.uuid());
            IndexMetadata previousMetadata = previous.metadata().index(index);
            logger.error(
                "clusterless: could not build the cluster state for [{}]; keeping {} and carrying on with the rest "
                    + "of the assignment",
                desiredIndex.key(),
                previousMetadata == null ? "it out of the state" : "what was already applied",
                e
            );
            if (previousMetadata == null) {
                return null;
            }
            metadata.put(previousMetadata, false);
            IndexRoutingTable previousRouting = previous.routingTable().index(index);
            if (previousRouting == null) {
                routing.add(IndexRoutingTable.builder(index));
            } else {
                // Added as-is rather than rebuilt, so every copy keeps its allocation id and its state. Rebuilding
                // is what restarts a serving shard, and this is the path taken when something is already wrong.
                routing.add(previousRouting);
            }
            return index;
        }
    }

    /**
     * Keeps the metadata of an index this node no longer holds a copy of, with no routing entry for it.
     *
     * <p><b>This is what stops "stop serving this" from meaning "destroy this", and the difference is the whole
     * of D5.4.</b> The server decides what to do with an index's files from why it was removed, and the two
     * reasons are chosen for it by whether the metadata is still there:
     *
     * <ul>
     *   <li>metadata gone → {@code ClusterChangedEvent.indicesDeleted()} reports a deletion, and
     *       {@code IndicesClusterStateService} removes the index with {@code DELETED}, whose javadoc says the
     *       shard files, state and translog "are removed";
     *   <li>metadata present, no shards here → removed with {@code NO_LONGER_ASSIGNED}, whose javadoc says they
     *       "are kept around in the case of a disaster recovery".
     * </ul>
     *
     * <p>So an assignment that stops naming a copy must leave the metadata in place. The supervisor's statement is
     * only ever "stop serving this" — it moves a shard by adding a copy elsewhere and then dropping this one, and
     * a node that deleted its bytes on hearing the second half would throw away the cheapest recovery path there
     * is, at the exact moment the new copy might still fail.
     *
     * <p>Carried forward from the previous state rather than remembered separately, which keeps this a pure
     * function of (previous, desired) and means it survives nothing that the previous state does not. A restart
     * therefore forgets: the index is absent from the first state a new process builds, and absence at that point
     * is not a transition, so nothing is deleted. Reaping the metadata deliberately — once the index really is
     * gone from the store and the copy has been reported absent for long enough — is the supervisor's
     * {@code ORPHANED_COPY} discipline turned inward, and is not implemented here yet.
     */
    private void retainIndicesNoLongerWanted(
        ClusterState previous,
        Set<Index> stillWanted,
        Metadata.Builder metadata,
        RoutingTable.Builder routing
    ) {
        for (IndexMetadata previousIndex : previous.metadata()) {
            Index index = previousIndex.getIndex();
            if (stillWanted.contains(index)) {
                continue;
            }
            if (stopRetaining.contains(index)) {
                // Left out on purpose. Absence from the metadata is what tells the server to remove the index with
                // DELETED rather than NO_LONGER_ASSIGNED, and therefore to delete the files -- which is the point:
                // the index is gone from the store and the reaper has watched it stay gone.
                logger.info("clusterless: releasing [{}] and the data on disk with it", index.getName());
                continue;
            }
            // Not logged here. This method runs on every pass -- twice on a pass that applies, since the state is
            // fabricated once to compare and once inside the applier -- so an INFO line here is a line per poll
            // interval for as long as the index is retained, which is exactly what it did. The fact belongs in
            // the report, which is printed only when something changes.
            logger.debug("clusterless: keeping the metadata of [{}], which this node no longer holds", index.getName());
            metadata.put(previousIndex, false);
            // An index routing table with no shards, which is what says "this node holds none of it". Removing
            // the index from the routing table altogether would say the same thing, and this way the index is
            // visibly present-but-empty rather than half absent.
            routing.add(IndexRoutingTable.builder(index));
        }
    }

    private Index buildIndex(
        ClusterState previous,
        DesiredState.DesiredIndex desiredIndex,
        Metadata.Builder metadata,
        RoutingTable.Builder routing
    ) {
        IndexConfig config = desiredIndex.config();
        String indexName = IndexNaming.indexName(desiredIndex.tenant(), desiredIndex.uuid(), config.name());
        Index index = new Index(indexName, desiredIndex.uuid());

        IndexMetadata previousMetadata = previous.metadata().index(index);
        IndexRoutingTable previousRouting = previous.routingTable().index(index);

        IndexMetadata.Builder builder = IndexMetadata.builder(indexName).settings(settingsFor(desiredIndex));

        MappingMetadata mapping = mappingFor(index, config, previousMetadata);
        if (mapping != null) {
            builder.putMapping(mapping);
        }

        // Desired state, applied level-triggered like everything else here. Set unconditionally rather than only
        // for an index with a source: IngestionStatus(false) is what the builder would have defaulted to, so an
        // ordinary index fabricates identically and StateEquivalence sees no change. Nothing latches, so a
        // resume is as ordinary as a pause -- and on a shard that is already running, the flip reaches the
        // poller through the published state, because IndicesClusterStateService.updateShard calls
        // updateShardIngestionState for every shard on this node on every applied state.
        builder.ingestionStatus(new IngestionStatus(config.ingestionPaused()));

        if (previousMetadata != null) {
            // Carried forward rather than reset. The server uses these to decide what to reload, so starting
            // them from zero every pass would say that everything about every index had just changed.
            builder.version(previousMetadata.getVersion())
                .settingsVersion(previousMetadata.getSettingsVersion())
                .mappingVersion(previousMetadata.getMappingVersion());
        }

        IndexRoutingTable.Builder indexRouting = IndexRoutingTable.builder(index);
        desiredIndex.shards().forEach((shardNumber, role) -> {
            ShardId shardId = new ShardId(index, shardNumber);
            long primaryTerm = previousMetadata == null ? INITIAL_PRIMARY_TERM : previousMetadata.primaryTerm(shardNumber);
            builder.primaryTerm(shardNumber, Math.max(primaryTerm, INITIAL_PRIMARY_TERM));

            ShardRouting shardRouting = routingFor(shardId, role, previousRouting);
            indexRouting.addShard(shardRouting);

            // Exactly the copies this pass described, and nothing carried forward. The set says which copies are
            // in sync *now*, so an allocation id whose routing entry is gone does not belong in it -- and this
            // used to carry the previous pass's set forward wholesale, which made it grow without bound.
            //
            // It killed a node, so the mechanism is worth stating. routingFor mints a fresh allocation id whenever
            // the previous entry cannot be reused -- a role change, or a copy that was not started or initializing
            // -- and the old id stayed in the set for ever. ReplicationTracker.updateFromClusterManager then
            // tracks a CheckpointState for every in-sync id, marking it replicated only when the id is the
            // primary's, because assignedToRemoteStoreNode is true for a remote-store-backed index
            // (ReplicationTracker.isReplicated). With no remote translog -- which is what segments_only is -- and
            // with DiscoveryNode.isRemoteStoreNode() false because a cluster-state repository is not configured
            // either, every escape in calculateReplicationGroup's assertion fails and the node exits with
            // "In absence of remote translog store, all tracked shards must have replication mode as
            // LOGICAL_REPLICATION". One stale id is enough; it just needs a second replication group to be
            // calculated afterwards.
            builder.putInSyncAllocationIds(shardNumber, Set.of(shardRouting.allocationId().getId()));
        });

        IndexMetadata built = builder.build();
        if (previousMetadata != null && previousMetadata.equals(built) == false) {
            // Something really did change, so say which part. A settings change and a mapping change are
            // reloaded by different machinery, and bumping only the outer version would leave one of them
            // holding stale state.
            builder.version(previousMetadata.getVersion() + 1);
            if (previousMetadata.getSettings().equals(built.getSettings()) == false) {
                builder.settingsVersion(previousMetadata.getSettingsVersion() + 1);
            }
            if (Objects.equals(previousMetadata.mapping(), built.mapping()) == false) {
                builder.mappingVersion(previousMetadata.getMappingVersion() + 1);
            }
            built = builder.build();
            logger.debug("clusterless: index [{}] metadata changed, now version {}", indexName, built.getVersion());
        }

        metadata.put(built, false);
        routing.add(indexRouting);
        return index;
    }

    /**
     * The mapping, canonicalised only when the index document it came from has actually changed.
     *
     * <p>Canonicalising means asking {@code IndicesService.createIndexMapperService}, and that turns out to be an
     * expensive and <em>side-effecting</em> thing to do: it builds an {@code IndexSettings} and an
     * {@code IndexModule}, calls {@code pluginsService.onIndexModule} on every installed plugin, and does it all
     * inside a {@code synchronized} method that real index operations also queue behind. Doing it on every pass
     * produced two log lines per second per index from unrelated components — which read exactly like a shard
     * restarting in a loop, and sent two people looking for a bug in the applier.
     *
     * <p>So it is skipped when it cannot matter. The store revision of the index document is carried in the
     * fabricated settings, so if the previously applied metadata was built from the same revision then the
     * document has not changed, and neither can its mapping: the applied {@code MappingMetadata} is reused
     * verbatim. Revisions are opaque and this compares them for equality only, which is the single operation the
     * contract permits.
     *
     * <p>That leaves canonicalisation running once per revision, which is the frequency it was always for. Its
     * purpose is that byte-identical intent produces byte-identical metadata — otherwise every pass looks like a
     * mapping change and {@code mappingVersion} climbs for ever — and comparing revisions achieves the same
     * property more cheaply for the case that is not a change at all.
     */
    private MappingMetadata mappingFor(Index index, IndexConfig config, IndexMetadata previousMetadata) {
        if (previousMetadata != null && ClusterlessIndexSettings.CONFIG_REVISION.get(previousMetadata.getSettings()) == config.revision()) {
            return previousMetadata.mapping();
        }
        return mappings.mappingFor(index, config.mappings());
    }

    /**
     * The routing entry for one copy: the previous one where it still applies, a fresh one where it does not.
     *
     * <p>Reused only when the previous entry is on <em>this</em> node, in a state that is going somewhere, and
     * for the same role. A role change is a different copy as far as the server is concerned — a promotion has
     * to re-open the shard as a primary — so reusing across one would leave a started routing entry claiming to
     * be something it is not. That cannot happen today, because the supervisor never fails a primary over
     * (D8.1), which is exactly why it is worth handling before it can.
     */
    private ShardRouting routingFor(ShardId shardId, ShardRole role, IndexRoutingTable previousRouting) {
        boolean primary = role == ShardRole.PRIMARY;
        boolean searchOnly = role == ShardRole.SEARCH_REPLICA;

        if (previousRouting != null) {
            IndexShardRoutingTable previousShard = previousRouting.shard(shardId.id());
            if (previousShard != null) {
                for (ShardRouting candidate : previousShard) {
                    boolean mine = localNode.getId().equals(candidate.currentNodeId());
                    boolean live = candidate.started() || candidate.initializing();
                    boolean sameRole = candidate.primary() == primary && candidate.isSearchOnly() == searchOnly;
                    if (mine && live && sameRole) {
                        return candidate;
                    }
                }
            }
        }

        RecoverySource recoverySource;
        if (shardStore.hasShardData(shardId)) {
            // There are bytes on disk for this shard. Recovering from an empty store would throw them away and
            // rebuild from the remote store or from nothing, which is slow at best and lossy at worst. Checked
            // first, and deliberately ahead of anything looked up in the repository: local bytes are the cheapest
            // correct answer, and if the disk has gained them since the lookup then the lookup is the stale one.
            recoverySource = RecoverySource.ExistingStoreRecoverySource.INSTANCE;
        } else {
            // No local bytes, which stops meaning "no data" once the segments live in a repository. What that
            // repository actually holds was looked up in the pass; see RemoteRecovery for why guessing either way
            // is unsafe. Nothing looked up means nothing remote-backed, so an empty store it is.
            recoverySource = recoverySources.getOrDefault(shardId, RecoverySource.EmptyStoreRecoverySource.INSTANCE);
        }

        if (role == ShardRole.REPLICA) {
            // A document-replication replica always recovers from its primary, whatever is on this disk. The local
            // bytes are not wasted -- peer recovery negotiates which segments and operations are actually needed,
            // so an existing copy still resumes cheaply -- but the *source* is not a choice: ShardRouting asserts
            // that a copy which is neither a primary nor search-only recovers from a peer, and the existing-store
            // source above is what a primary uses.
            recoverySource = RecoverySource.PeerRecoverySource.INSTANCE;
        }

        return ShardRouting.newUnassigned(
            shardId,
            primary,
            searchOnly,
            recoverySource,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "assigned by the clusterless data plane")
        ).initialize(localNode.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    private Settings settingsFor(DesiredState.DesiredIndex desiredIndex) {
        boolean holdsAnySearchReplica = desiredIndex.shards().containsValue(ShardRole.SEARCH_REPLICA);
        boolean holdsOnlySearchReplicas = holdsOnlySearchReplicas(desiredIndex.shards());
        IndexConfig config = desiredIndex.config();
        Settings.Builder settings = Settings.builder();

        // The control plane's settings first, so that nothing below can be overridden by them. They are flat
        // and entirely string-valued, which is the shape loadFromMap wants.
        settings.loadFromMap(Map.copyOf(config.settings()));

        settings.put(IndexMetadata.SETTING_INDEX_UUID, desiredIndex.uuid());
        settings.put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        settings.put(IndexMetadata.SETTING_CREATION_DATE, creationDate(config));

        // The contract's identity, which the name deliberately does not carry. See ClusterlessIndexSettings.
        settings.put(ClusterlessIndexSettings.TENANT.getKey(), desiredIndex.tenant().value());
        settings.put(ClusterlessIndexSettings.TENANT_INDEX_NAME.getKey(), config.name());
        settings.put(ClusterlessIndexSettings.CONFIG_REVISION.getKey(), config.revision());

        if (holdsAnySearchReplica) {
            // The marker that lets a shard with no primary exist in this node's routing table. RoutingNodes
            // asserts "Primary shard routing can't be null for non-search-only indices" for any shard whose
            // routing table has no primary, and IndicesClusterStateService builds RoutingNodes on every applied
            // state -- so without this, applying a search replica would trip an assertion on a server that runs
            // with them enabled. This node has no primary to name: it is on another node, and a segment
            // replication copy is never told where. cluster-etcd reached the same conclusion by the same assertion.
            //
            // It reads like it should cost writes, and here it does not. The setting's cluster block
            // (INDEX_SEARCH_ONLY_BLOCK, "index scaled down", ClusterBlockLevel.WRITE) is only ever derived from
            // the setting by cluster-manager code -- MetadataCreateIndexService, RestoreService, the gateway's
            // ClusterStateUpdaters -- and none of that runs in a clusterless node. This state is fabricated
            // directly and sets no blocks at all, so the setting is a marker and nothing more. That is exactly the
            // kind of dependence on an absent code path that stops being true quietly, so
            // DataNodeStateTests pins it: a fabricated state must carry no write block.
            //
            // Which is what makes a node holding a primary of shard 0 and a search replica of shard 1 of the same
            // index workable, and the alternative was declining half the natural placement: two nodes and a
            // two-shard index with one replica each is exactly that mix.
            settings.put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true);
        }

        // Independent of the marker above, and for a different reason. cluster.routing.search_replica.strict
        // defaults to true, and with it on, OperationRouting injects Preference.SEARCH_REPLICA into every search
        // that named no preference -- for any index whose metadata says it has search replicas. On a node that
        // holds the primary of shard 0, that preference selects from the search-only copies of shard 0, of which
        // there are none here, so a healthy primary would quietly answer nothing.
        //
        // So the count is one only when every copy this node holds is a search replica, and zero otherwise --
        // including the mixed case, where zero is right for both shards: a started search-only shard is in
        // IndexShardRoutingTable.activeShards regardless, so the default iterator still reaches it.
        settings.put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, holdsOnlySearchReplicas ? 1 : 0);

        if (remoteStore.backs(config)) {
            remoteStore.addIndexSettings(settings);
        }
        return settings.build();
    }

    /**
     * The index's creation date, from the document rather than from the clock.
     *
     * <p>Every node reads the same document, so every node derives the same date. Where the document has none,
     * the fallback is derived from the uuid — which is stable, shared, and meaningless as a date, in that order
     * of importance. {@code cluster-etcd} derives its from the index name for the same reason.
     */
    static long creationDate(IndexConfig config) {
        if (config.creationDate() > 0) {
            return config.creationDate();
        }
        long baseEpoch = 1704067200000L; // 2024-01-01T00:00:00Z
        return baseEpoch + Math.floorMod(config.uuid().hashCode(), 24 * 60 * 60 * 1000);
    }

    private static boolean holdsOnlySearchReplicas(Map<Integer, ShardRole> shards) {
        Set<ShardRole> roles = new LinkedHashSet<>(shards.values());
        return roles.equals(Set.of(ShardRole.SEARCH_REPLICA));
    }
}
