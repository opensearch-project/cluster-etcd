/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.core.index.Index;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Declines a copy whose assigned primary term is lower than the one it is already running at.
 *
 * <p>The third step that reduces the desired state once per pass, after {@link RemoteRecovery} and
 * {@link PeerRecovery}, and the same shape: it needs the live state, so it cannot live in the resolver.
 *
 * <h2>Why a lower term is refused rather than clamped</h2>
 *
 * <p>The contract says the supervisor never lowers a term in place, and that a node told a lower term than a started
 * copy is running defers it with a reason. Both halves matter, and clamping quietly to the higher value would
 * satisfy the server while breaking the contract's purpose.
 *
 * <p>The server would not accept the lower term anyway — {@code IndexShard.updateShardState} asserts
 * {@code newPrimaryTerm > pendingPrimaryTerm} on the promotion path, and a started primary may not change its term
 * at all. So the choice is not between working and failing; it is between **saying so** and papering over it. A
 * term that went backwards means the supervisor's counter did, and that is the one thing its compare-and-set is
 * built to prevent: it points at a lost or rolled-back counter, which is a condition somebody has to know about
 * before it issues a term twice. A node that clamped and carried on would leave the only evidence in a log line.
 *
 * <p>What it costs to decline: that copy is not rebuilt this pass, and the reason reaches the supervisor through
 * {@code applied}. The copy already running is untouched — declining reduces what is *fabricated*, and a started
 * shard whose index stays in the state keeps serving.
 */
public final class PrimaryTerms {

    private static final Logger logger = LogManager.getLogger(PrimaryTerms.class);

    private PrimaryTerms() {}

    public static DesiredState reduce(DesiredState desired, ClusterState live) {
        List<String> declined = new ArrayList<>();
        Map<DesiredState.IndexKey, DesiredState.DesiredIndex> kept = new TreeMap<>();

        for (DesiredState.DesiredIndex index : desired.indices().values()) {
            if (index.primaryTerms().isEmpty()) {
                // No term stated, so nothing to disagree with. An un-upgraded supervisor takes this path for
                // everything, and every shard stays at 1 exactly as it did before terms existed.
                kept.put(index.key(), index);
                continue;
            }

            Index built = new Index(IndexNaming.indexName(index.tenant(), index.uuid(), index.config().name()), index.uuid());
            IndexMetadata applied = live.metadata().index(built);
            if (applied == null) {
                // Nothing running, so no term to go backwards from.
                kept.put(index.key(), index);
                continue;
            }

            Map<Integer, ShardRole> buildable = new TreeMap<>(index.shards());
            Map<Integer, Long> terms = new TreeMap<>(index.primaryTerms());
            index.shards().forEach((shard, role) -> {
                long assigned = index.primaryTermOf(shard);
                long running = applied.primaryTerm(shard);
                if (assigned >= running) {
                    return;
                }
                if (started(live, built, shard) == false) {
                    // Not serving, so there is nothing in flight at the higher term and the lower one is merely a
                    // stale read of a document the supervisor is about to rewrite. Level-triggering covers it.
                    return;
                }
                buildable.remove(shard);
                terms.remove(shard);
                declined.add(
                    index.key() + "/" + shard + ": the assigned primary term is " + assigned + " and this node is "
                        + "already running that shard at " + running + "; a term must not go backwards, so the copy "
                        + "was not rebuilt"
                );
                logger.warn(
                    "clusterless: assigned primary term {} for {}/{} is below the running term {}; not rebuilding it",
                    assigned,
                    index.key(),
                    shard,
                    running
                );
            });

            if (buildable.isEmpty() == false) {
                kept.put(index.key(), new DesiredState.DesiredIndex(index.key(), index.config(), buildable, terms));
            }
        }

        if (declined.isEmpty()) {
            return desired;
        }
        List<String> deferred = new ArrayList<>(desired.deferred());
        deferred.addAll(declined);
        return new DesiredState(desired.assignmentVersion(), kept, desired.coordinatorTenants(), deferred);
    }

    private static boolean started(ClusterState live, Index index, int shard) {
        var routing = live.routingTable().index(index);
        if (routing == null || routing.shard(shard) == null) {
            return false;
        }
        for (var copy : routing.shard(shard)) {
            if (copy.started()) {
                return true;
            }
        }
        return false;
    }
}
