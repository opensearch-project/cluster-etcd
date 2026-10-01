/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;

import java.util.Objects;

/**
 * Where this node's shards keep their segments, if anywhere other than its own disk.
 *
 * <p>A search replica is the reason this class exists, and the reason is not obvious from the milestone. A
 * segment-replication copy has to get its segments from somewhere, and the server picks the source in
 * {@code SegmentReplicationSourceFactory}:
 *
 * <pre>
 *   if (shard.indexSettings().isAssignedOnRemoteNode()) return new RemoteStoreReplicationSource(shard);
 *   else return new PrimaryShardReplicationSource(..., getPrimaryNode(shard.shardId()));
 * </pre>
 *
 * <p>and {@code getPrimaryNode} reads {@code clusterService.state().routingTable().shardRoutingTable(shardId)
 * .primaryShard()} and then looks that node up in {@code state().nodes()}. A clusterless node's fabricated state
 * contains only its own copies and only itself, so on the {@code else} branch both lookups come back empty and the
 * copy cannot replicate at all. <b>Remote-store-backed segment replication is therefore not one option of two for
 * a replica in this architecture; it is the only one that works without the peer resolution of M8/M9</b> — which is
 * exactly the scope this data plane was asked to support first, arrived at from the other direction.
 *
 * <h2>Configured on the node, not in the desired state</h2>
 *
 * <p>{@code isAssignedOnRemoteNode()} is computed from {@code node.attr.remote_store.*} — <em>node</em> settings,
 * not index settings and not cluster state. So the repository is local operator configuration, like the store URI:
 * the control plane says "this index uses segment replication" and the node says where its segments live. Nothing
 * new is needed in the key layout, and a tenant never names an operator's bucket.
 *
 * <p>Two things follow from the repository being node-local, and both are load-bearing:
 *
 * <ul>
 *   <li><b>Every node holding a copy of a shard must be configured with the same repository</b>, because the copies
 *       share one remote path. Nothing here can check that — a node cannot see another node's attributes — so it is
 *       a deployment invariant, and the failure if it is broken is a replica that reads an empty store rather than
 *       an error.
 *   <li><b>The path within the repository must be derived identically by every node.</b> It comes from
 *       {@code RemoteStoreUtils.determineRemoteStorePathStrategy}, which reads the index metadata's
 *       {@code REMOTE_STORE_CUSTOM_KEY} custom data and falls back to {@code PathType.FIXED} when there is none.
 *       In a real cluster one cluster manager stamps that custom data once and every node reads the same copy;
 *       here each node fabricates its own metadata independently, so <em>anything</em> stamped per node could
 *       differ between the primary and its replica and send them to different paths. So this sets no remote-store
 *       custom data at all, and every node lands on {@code FIXED} by the same route. That is a deliberate choice of
 *       the older, flatter layout over one that might disagree with itself.
 * </ul>
 *
 * <h2>Only segment-replication indices</h2>
 *
 * <p>{@code INDEX_REMOTE_STORE_ENABLED_SETTING} carries a validator that rejects {@code true} unless
 * {@code index.replication.type} is {@code SEGMENT}, and that validator runs when the index service is created —
 * so enabling remote store for a document-replication index would fail the index on the apply path. Rather than
 * force the replication type and quietly overrule the control plane, an index is backed only if it asked for
 * segment replication. A document-replication index on a remote-store node is left alone, which is also why
 * {@link DesiredStateResolver} declines a search replica of one: there would be no remote store for it to read.
 */
public final class RemoteStore {

    private static final Logger logger = LogManager.getLogger(RemoteStore.class);

    /** A node with no remote store configured: nothing is backed, and no repository is registered. */
    public static final RemoteStore NONE = new RemoteStore(null, null, null);

    private final String segmentRepository;
    private final String translogRepository;
    private final RepositoriesMetadata repositories;

    private RemoteStore(String segmentRepository, String translogRepository, RepositoriesMetadata repositories) {
        this.segmentRepository = segmentRepository;
        this.translogRepository = translogRepository;
        this.repositories = repositories;
    }

    /**
     * Reads this node's own remote-store attributes, or {@link #NONE} if it has none.
     *
     * <p>Done once at startup rather than per pass: node attributes cannot change while the process runs, and the
     * derivation throws {@link IllegalStateException} on a half-configured node. Failing there is right, and
     * failing there <em>once</em>, at startup, with the reason, beats failing inside every fabrication for ever.
     *
     * <p><b>Keyed on the segment repository, deliberately not on {@code DiscoveryNode.isRemoteStoreNode()}.</b>
     * That predicate requires a <em>cluster-state</em> repository as well, which a node running
     * {@code node.attr.remote_store.mode=segments_only} does not have — and that mode exists precisely for this
     * architecture. Meanwhile {@code IndexSettings.assignedOnRemoteNode} is computed from
     * {@code isRemoteDataAttributePresent}, which is true for such a node. So gating on
     * {@code isRemoteStoreNode()} would produce the one genuinely broken state available here: the server routing
     * replicas to {@code RemoteStoreReplicationSource} while this plugin had registered no repository and backed no
     * index. Match the predicate the server actually uses.
     */
    public static RemoteStore of(DiscoveryNode localNode) {
        Objects.requireNonNull(localNode, "localNode");
        String segment = RemoteStoreNodeAttribute.getSegmentRepoName(localNode.getAttributes());
        if (segment == null || segment.isEmpty()) {
            return NONE;
        }
        String translog = RemoteStoreNodeAttribute.getTranslogRepoName(localNode.getAttributes());
        RepositoriesMetadata repositories = new RemoteStoreNodeAttribute(localNode).getRepositoriesMetadata();
        logger.info(
            "clusterless: remote store configured on this node -- segments in [{}], translog {}. Indices using "
                + "segment replication will be backed by it, and search replicas will read their segments from it.",
            segment,
            translog == null ? "kept locally, so a primary cannot recover from the remote store" : "in [" + translog + "]"
        );
        return new RemoteStore(segment, translog, repositories);
    }

    /** Whether this node has a remote store at all. */
    public boolean configured() {
        return segmentRepository != null;
    }

    /** The repository segments go to, or {@code null} when there is none. */
    public String segmentRepository() {
        return segmentRepository;
    }

    /**
     * Whether a translog repository is configured as well as a segment one.
     *
     * <p>Worth asking separately, because it decides whether a primary can recover from the remote store at all:
     * {@code IndexShard.syncTranslogFilesFromRemoteTranslog} asserts its translog factory is the remote one, and
     * that factory is chosen only when {@code index.remote_store.translog.repository} is set.
     *
     * <p>Segments without a translog is a supported configuration rather than a misconfiguration, and it has its
     * own upstream switch: {@code node.attr.remote_store.mode=segments_only}, which makes
     * {@code RemoteStoreNodeAttribute} require the segment repository alone. It was added for this architecture.
     * The reasoning is that with pull-based ingestion the ingestion source holds every operation and the shard
     * tracks an offset into it, so a transaction log is a second copy of something already durable — which is why
     * {@code cluster-etcd} turned it off. What it costs is this recovery path, so {@link RemoteRecovery} declines
     * a primary that would need it rather than starting one empty over its own data.
     */
    public boolean translogConfigured() {
        return translogRepository != null && translogRepository.isEmpty() == false;
    }

    /**
     * Whether an index would be remote-store-backed on this node.
     *
     * <p>Asked by the fabricator, to decide what to put in the index settings, and by the resolver, to decide
     * whether a search replica of it can work at all. One question, one answer, so the two cannot disagree about
     * an index and leave a copy built on the assumption of a store it does not have.
     */
    public boolean backs(IndexConfig config) {
        return configured() && segmentReplication(config);
    }

    /**
     * The repositories to register, or {@code null} when there are none.
     *
     * <p>{@code RepositoriesService} is a {@code ClusterStateApplier} that reads
     * {@code Metadata.custom(RepositoriesMetadata.TYPE)}, so putting this in the fabricated metadata is all it
     * takes to make the repository exist on this node. In a real remote-store cluster the cluster manager writes
     * the same metadata on node join; there is no cluster manager here, and the node's own attributes say the
     * same thing.
     */
    public RepositoriesMetadata repositories() {
        return repositories;
    }

    /**
     * Adds the index settings that make a shard use the remote store.
     *
     * <p>The same three {@code MetadataCreateIndexService.updateRemoteStoreSettings} writes at index creation,
     * which is the moment this stands in for. Deliberately <em>not</em> accompanied by remote-store custom data:
     * see the note on path derivation above.
     */
    public void addIndexSettings(Settings.Builder settings) {
        if (configured() == false) {
            return;
        }
        settings.put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true);
        settings.put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, segmentRepository);
        if (translogRepository != null) {
            settings.put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, translogRepository);
        }
    }

    /**
     * Whether the index asked for segment replication.
     *
     * <p>Read from the index document's own settings rather than from this node's cluster default, because the
     * replication type has to be a property of the index that every node agrees on, and a node-local default is
     * the one thing that would let two nodes disagree.
     */
    static boolean segmentReplication(IndexConfig config) {
        String replication = config.settings().get(IndexMetadata.SETTING_REPLICATION_TYPE);
        return replication != null && ReplicationType.SEGMENT.name().equalsIgnoreCase(replication.trim());
    }
}
