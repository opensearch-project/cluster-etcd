/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Where a remote-store-backed primary should recover from, when this node has no local copy of it.
 *
 * <p>Once the segments live in a repository, "there are no bytes on disk for this shard" stops meaning "this
 * shard has no data". {@link LocalShardStore} answers the first question and the second one is the one that
 * matters, so a primary that comes back without its disk would otherwise start empty on top of a complete copy
 * of itself — and then serve as the authoritative primary of an index whose data it does not have, having sealed
 * the remote store's fence on the way so that the copy which still has the data cannot contest it.
 *
 * <h2>Neither answer is safe to guess</h2>
 *
 * <table>
 *   <caption>What each recovery source does</caption>
 *   <tr><th></th><th>repository has data</th><th>repository is empty</th></tr>
 *   <tr><td>{@code EMPTY_STORE}</td><td><b>silent data loss</b></td><td>correct</td></tr>
 *   <tr><td>{@code REMOTE_STORE}</td><td>correct</td><td>fails for ever</td></tr>
 * </table>
 *
 * <p>The bottom-right cell is the one that is easy to miss, and it is why "prefer {@code REMOTE_STORE} when
 * unsure" — the first answer written down for this — is wrong. {@code RemoteFsTranslog.download} takes its
 * {@code else} branch when the repository holds no translog metadata and creates no {@code translog.ckp}; back
 * in {@code StoreRecovery.recoverFromRemoteStore} the local store is then empty, {@code listAll().length <= 1}
 * holds, and {@code Checkpoint.read} throws. Since a failed copy is given a fresh routing entry on the next
 * pass, a new index whose primary defaulted to {@code REMOTE_STORE} would fail its first recovery for ever.
 *
 * <p>So the answer is looked up, and where it cannot be looked up <b>the copy is declined for the pass with a
 * reason</b> rather than built on a guess. That is the same patience {@link RetainedIndexReaper} applies to
 * deleting data and the same reporting {@link DesiredStateResolver} uses for a copy it will not build: a copy
 * declined for a pass costs a pass, and one built on a guess costs the shard or never starts.
 *
 * <h2>Remote translog, and why its absence is also a decline</h2>
 *
 * <p>{@code REMOTE_STORE} recovery is not available to an index that keeps its segments in a repository but not
 * its translog. {@code IndexShard.syncTranslogFilesFromRemoteTranslog} asserts its translog factory is the
 * remote one and casts to it, and that factory is only chosen when
 * {@code IndexSettings.isRemoteTranslogStoreEnabled()} — which is true exactly when
 * {@code index.remote_store.translog.repository} is set. There is no separate flag.
 *
 * <p>That configuration is a real one rather than a mistake: with pull-based ingestion the ingestion source holds
 * every operation, so a transaction log is redundant and {@code cluster-etcd} disabled it deliberately. It just
 * cannot use this recovery path, so a shard with data in the repository and no remote translog is declined with
 * a reason saying so. Recovering <em>that</em> shard means restoring segments and resuming from the ingestion
 * checkpoint, which is a different mechanism and not this one.
 *
 * <h2>Called once per pass, from the poller</h2>
 *
 * <p>This reads a repository over a network, and the fabricator is a pure function called twice on a pass that
 * applies — the mistake this repository has made four times. So it runs in the pass, before anything is
 * fabricated, and only for copies that are <em>about to be created fresh</em>: a copy already in the live state
 * keeps its routing entry and is never asked about. In a converged pool that set is empty and nothing is probed.
 */
public final class RemoteRecovery {

    private static final Logger logger = LogManager.getLogger(RemoteRecovery.class);

    private final RemoteStore remoteStore;
    private final LocalShardStore localData;
    private final RemoteShardData remoteData;
    private final String localNodeId;

    public RemoteRecovery(RemoteStore remoteStore, LocalShardStore localData, RemoteShardData remoteData, String localNodeId) {
        this.remoteStore = Objects.requireNonNull(remoteStore, "remoteStore");
        this.localData = Objects.requireNonNull(localData, "localData");
        this.remoteData = Objects.requireNonNull(remoteData, "remoteData");
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
    }

    /**
     * The desired state to fabricate from, and the recovery sources that were looked up for it.
     *
     * @param desired the same desired state, or one with the undecidable copies removed and the reasons recorded
     *     in {@code deferred} — which is what puts them in the report, the local status endpoint and the
     *     heartbeat's failure detail, all of which already read that list
     * @param sources recovery sources for copies where the repository was consulted. Absent means "no opinion",
     *     and the fabricator keeps its own answer.
     */
    public record Plan(DesiredState desired, Map<ShardId, RecoverySource> sources) {

        public Plan {
            sources = sources == null ? Map.of() : Map.copyOf(sources);
        }
    }

    /** Nothing to decide, for a node with no remote store. */
    public static Plan noOpinion(DesiredState desired) {
        return new Plan(desired, Map.of());
    }

    public Plan planFor(DesiredState desired, ClusterState live) {
        if (remoteStore.configured() == false) {
            return noOpinion(desired);
        }

        Map<ShardId, RecoverySource> sources = new LinkedHashMap<>();
        List<String> declined = new ArrayList<>();
        Map<DesiredState.IndexKey, DesiredState.DesiredIndex> kept = new TreeMap<>();

        for (DesiredState.DesiredIndex index : desired.indices().values()) {
            if (remoteStore.backs(index.config()) == false) {
                // Not remote-backed, so the disk is still the only place its data could be.
                kept.put(index.key(), index);
                continue;
            }
            Index built = new Index(IndexNaming.indexName(index.tenant(), index.uuid(), index.config().name()), index.uuid());

            Map<Integer, ShardRole> buildable = new TreeMap<>(index.shards());
            index.shards().forEach((shardNumber, role) -> {
                ShardId shardId = new ShardId(built, shardNumber);
                if (role != ShardRole.PRIMARY) {
                    // Only a primary recovers this way. A search replica reads the repository as part of opening
                    // its engine, from an ordinary store recovery source, and needs nothing decided here.
                    return;
                }
                if (alreadyHere(live, shardId) || localData.hasShardData(shardId)) {
                    // Serving already, or there are bytes to resume from. Either way the fabricator's own answer
                    // is right and better informed than this one -- and for a pull-based index the local commit is
                    // also where the ingestion pointer is, so resuming from it resumes from exactly where it left.
                    return;
                }
                if (pullsFromAnIngestionSource(index.config())) {
                    // Nothing to look up, and nothing to decline. An ingestion source is a durable log that this
                    // shard's position within is recorded in its own Lucene commit (StreamPoller.BATCH_START), so a
                    // shard with no commit has a well-defined thing to do: start empty and let the poller replay
                    // from index.ingestion_source.pointer.init.reset. Which is *why* such an index needs no
                    // translog, local or remote -- IngestionEngine uses a NoOpTranslogManager -- and therefore why
                    // remote-store recovery has nothing to offer it: that path restores a translog that does not
                    // exist.
                    replaying(shardId, index, sources);
                    return;
                }
                decide(shardId, index, sources, declined, buildable, shardNumber);
            });

            if (buildable.isEmpty() == false) {
                kept.put(index.key(), new DesiredState.DesiredIndex(index.key(), index.config(), buildable));
            }
        }

        if (declined.isEmpty()) {
            return new Plan(desired, sources);
        }
        List<String> deferred = new ArrayList<>(desired.deferred());
        deferred.addAll(declined);
        return new Plan(
            new DesiredState(desired.assignmentVersion(), kept, desired.coordinatorTenants(), deferred),
            sources
        );
    }

    /**
     * Whether this index is fed by an ingestion source rather than by writes to a primary.
     *
     * <p>Read from the index document's settings, like the replication type and for the same reason: it has to be a
     * property of the index that every node agrees on, and a node-local default is the one thing that could let two
     * nodes disagree.
     */
    static boolean pullsFromAnIngestionSource(IndexConfig config) {
        String type = config.settings().get(org.opensearch.cluster.metadata.IndexMetadata.SETTING_INGESTION_SOURCE_TYPE);
        return type != null
            && type.isBlank() == false
            && org.opensearch.cluster.metadata.IndexMetadata.NONE_INGESTION_SOURCE_TYPE.equalsIgnoreCase(type.trim()) == false;
    }

    /**
     * Starts the copy empty and says what the poller will do with it, which is the part worth reading in a log.
     *
     * <p>Both outcomes are the tenant's declared policy rather than a failure, and they are very different: from
     * {@code earliest} the shard replays the whole stream and ends up correct at the cost of doing all that work
     * again, while from {@code latest} it starts at the head and whatever arrived while the shard was gone is not
     * indexed by this copy. Neither is this node's decision to make, and both deserve to be visible when somebody
     * is working out why a shard is either busy or short of documents.
     */
    private void replaying(ShardId shardId, DesiredState.DesiredIndex index, Map<ShardId, RecoverySource> sources) {
        String reset = index.config()
            .settings()
            .getOrDefault(org.opensearch.cluster.metadata.IndexMetadata.SETTING_INGESTION_SOURCE_POINTER_INIT_RESET, "");
        logger.info(
            "clusterless: {} is fed by an ingestion source and has no local commit to resume from, so it starts empty "
                + "and the poller replays from [{}] -- no remote-store recovery is involved, because an ingestion "
                + "engine keeps no translog to restore",
            shardId,
            reset.isBlank() ? "the source's default reset point" : reset
        );
        sources.put(shardId, RecoverySource.EmptyStoreRecoverySource.INSTANCE);
    }

    private void decide(
        ShardId shardId,
        DesiredState.DesiredIndex index,
        Map<ShardId, RecoverySource> sources,
        List<String> declined,
        Map<Integer, ShardRole> buildable,
        int shardNumber
    ) {
        String repository = remoteStore.segmentRepository();
        RemoteShardData.Answer answer;
        try {
            answer = remoteData.probe(shardId, repository);
        } catch (Exception e) {
            logger.warn("clusterless: could not ask [{}] about {}; not creating the copy this pass", repository, shardId, e);
            answer = RemoteShardData.Answer.UNKNOWN;
        }

        switch (answer) {
            case EMPTY -> {
                // Nothing has ever been uploaded for this shard, so this is a new one and an empty store is
                // exactly right. Logged at INFO because it is the branch that would destroy data if the
                // repository were merely being unhelpful, and a person reading the log after a bad restart
                // should be able to see that the node asked and was told nothing was there.
                logger.info(
                    "clusterless: [{}] holds nothing for {}, so it is a new shard and starts from an empty store",
                    repository,
                    shardId
                );
                sources.put(shardId, RecoverySource.EmptyStoreRecoverySource.INSTANCE);
            }
            case HAS_DATA -> {
                if (remoteStore.translogConfigured() == false) {
                    // Declining rather than starting empty on top of it. See the class comment: this recovery
                    // path needs the remote translog factory, and the alternative is discarding the shard.
                    buildable.remove(shardNumber);
                    declined.add(
                        index.key() + "/" + shardNumber + ": [" + repository + "] holds data for this shard and this "
                            + "node has none, but recovering from a remote store needs "
                            + "index.remote_store.translog.repository, which this index does not set"
                    );
                }
                else {
                    logger.info(
                        "clusterless: [{}] holds data for {} and this node has none; recovering from the remote store "
                            + "rather than starting empty",
                        repository,
                        shardId
                    );
                    sources.put(shardId, new RecoverySource.RemoteStoreRecoverySource(
                        index.uuid(),
                        org.opensearch.Version.CURRENT,
                        new org.opensearch.repositories.IndexId(index.config().name(), index.uuid())
                    ));
                }
            }
            case UNKNOWN -> {
                buildable.remove(shardNumber);
                declined.add(
                    index.key() + "/" + shardNumber + ": could not tell whether [" + repository + "] holds data for "
                        + "this shard, and creating a primary either way risks discarding it or failing for ever; "
                        + "not creating it this pass"
                );
            }
        }
    }

    /** Whether this node already has a routing entry for the shard, in which case nothing is being created. */
    private boolean alreadyHere(ClusterState live, ShardId shardId) {
        var routing = live.routingTable().index(shardId.getIndex());
        if (routing == null) {
            return false;
        }
        IndexShardRoutingTable shard = routing.shard(shardId.id());
        if (shard == null) {
            return false;
        }
        for (ShardRouting candidate : shard) {
            if (localNodeId.equals(candidate.currentNodeId()) && (candidate.started() || candidate.initializing())) {
                return true;
            }
        }
        return false;
    }
}
