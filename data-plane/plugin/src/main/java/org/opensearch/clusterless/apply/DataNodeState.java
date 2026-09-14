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
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
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

    private final DiscoveryNode localNode;
    private final DesiredState desired;
    private final MappingSource mappings;
    private final LocalShardStore shardStore;

    public DataNodeState(DiscoveryNode localNode, DesiredState desired, MappingSource mappings, LocalShardStore shardStore) {
        this.localNode = Objects.requireNonNull(localNode, "localNode");
        this.desired = Objects.requireNonNull(desired, "desired");
        this.mappings = Objects.requireNonNull(mappings, "mappings");
        this.shardStore = Objects.requireNonNull(shardStore, "shardStore");
    }

    @Override
    public ClusterState buildClusterState(ClusterState previous) {
        ClusterState.Builder state = ClusterState.builder(previous.getClusterName());
        // Each application has to look like a newer publication than the last, or the applier ignores it.
        state.version(previous.version() + 1);
        state.nodes(DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build());

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routing = RoutingTable.builder();

        for (DesiredState.DesiredIndex index : desired.indices().values()) {
            buildIndex(previous, index, metadata, routing);
        }

        state.metadata(metadata);
        state.routingTable(routing.build());
        return state.build();
    }

    private void buildIndex(
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

        boolean searchOnly = holdsOnlySearchReplicas(desiredIndex.shards());
        IndexMetadata.Builder builder = IndexMetadata.builder(indexName).settings(settingsFor(desiredIndex, searchOnly));

        MappingMetadata mapping = mappings.mappingFor(index, config.mappings());
        if (mapping != null) {
            builder.putMapping(mapping);
        }

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

            // The in-sync set has to contain the copy we just described, or the server has a routing entry
            // whose allocation id it does not believe in.
            Set<String> inSync = new HashSet<>();
            if (previousMetadata != null) {
                inSync.addAll(previousMetadata.inSyncAllocationIds(shardNumber));
            }
            inSync.add(shardRouting.allocationId().getId());
            builder.putInSyncAllocationIds(shardNumber, inSync);
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

        RecoverySource recoverySource = shardStore.hasShardData(shardId)
            // There are bytes on disk for this shard. Recovering from an empty store would throw them away and
            // rebuild from the remote store or from nothing, which is slow at best and lossy at worst.
            ? RecoverySource.ExistingStoreRecoverySource.INSTANCE
            : RecoverySource.EmptyStoreRecoverySource.INSTANCE;

        return ShardRouting.newUnassigned(
            shardId,
            primary,
            searchOnly,
            recoverySource,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "assigned by the clusterless data plane")
        ).initialize(localNode.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    private Settings settingsFor(DesiredState.DesiredIndex desiredIndex, boolean searchOnly) {
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

        if (searchOnly) {
            // A search replica with no primary in the state is not something RoutingNodes will accept:
            // "Primary shard routing can't be null for non-search-only indices". This node has no primary to
            // name -- the primary is on some other node and the contract does not tell a segrep copy where --
            // so the index has to declare that serving reads is all it does. cluster-etcd reached the same
            // conclusion by the same assertion.
            settings.put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true);
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
    private static long creationDate(IndexConfig config) {
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
