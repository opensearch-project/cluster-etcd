/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.clusterless.contract.IndexStatus;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.ShardState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Whether a document-replication replica can be built at all, which comes down to whether its primary is visible.
 *
 * <p>The counterpart to {@link RemoteRecovery}, and the same shape: a step that runs once per pass, reduces the
 * desired state rather than guessing, and leaves a reason behind for every copy it declines. Where that one asks
 * what a repository holds, this one asks what the peer view found.
 *
 * <h2>Why a replica without a visible primary must not be built</h2>
 *
 * <p>Not a preference — {@code RoutingNodes} asserts it:
 *
 * <pre>
 * assert indexShard.primary != null : "Primary shard routing can't be null for non-search-only indices"
 * </pre>
 *
 * <p>and the only escape is {@code index.blocks.search_only}, which is exactly the assertion that makes a search
 * replica need its primary in the table. So it is not a search-replica rule at all: <b>any</b> shard in a
 * fabricated routing table needs a primary entry, and a document-replication replica has no marker it could
 * honestly set to get out of it — the block would claim the index takes no writes, which for a docrep index is a
 * lie, and it stops the replica doing its job besides.
 *
 * <p>An assertion is not a soft failure either. It runs on the cluster applier thread, so tripping it does not
 * decline a shard, it exits the node — which is how the in-sync set growing without bound was found.
 *
 * <p>So the honest answer when the primary is not visible is the one the rest of this fabrication gives: decline
 * the copy, say why, and try again next pass. A replica whose primary has not been observed yet is the ordinary
 * first-pass case and resolves itself; one whose primary is on a node this node cannot reach is a real problem
 * that the reason names.
 *
 * <h2>Visible means what the fabricator will accept</h2>
 *
 * <p>Deliberately the same test as {@link CoordinatorState}'s: a copy in the projection, in a state that maps to a
 * routing entry, on a node the peer view resolved an address for. Approximating it here would produce a copy this
 * step believed buildable and the fabricator then left out — which is the shape of every bug this plugin has had
 * that took a day to find.
 */
public final class PeerRecovery {

    private static final Logger logger = LogManager.getLogger(PeerRecovery.class);

    private PeerRecovery() {}

    /**
     * @param coordinated the peer view read this pass, which is where a primary becomes visible
     * @return the desired state with any unbuildable docrep replica removed and recorded in {@code deferred}
     */
    public static DesiredState reduce(DesiredState desired, CoordinatorView coordinated) {
        List<String> declined = new ArrayList<>();
        Map<DesiredState.IndexKey, DesiredState.DesiredIndex> kept = new TreeMap<>();

        for (DesiredState.DesiredIndex index : desired.indices().values()) {
            if ("DOCUMENT".equals(index.config().replication()) == false) {
                // Segment replication: a search replica's primary is wanted for freshness rather than for
                // existence, and the marker covers its absence. Nothing to decide.
                kept.put(index.key(), index);
                continue;
            }

            Map<Integer, ShardRole> buildable = new TreeMap<>(index.shards());
            index.shards().forEach((shardNumber, role) -> {
                if (role != ShardRole.REPLICA) {
                    // A primary needs no peer to exist. It may end up replicating to fewer copies than the tenant
                    // asked for, which is a health question rather than a buildability one.
                    return;
                }
                if (primaryVisible(coordinated, index.uuid(), shardNumber)) {
                    return;
                }
                buildable.remove(shardNumber);
                declined.add(
                    index.key() + "/" + shardNumber + ": a document-replication replica recovers from its primary, "
                        + "and no started or initializing primary for that shard is reachable in this node's peer view"
                );
                logger.debug(
                    "clusterless: not building the replica of {}/{} -- its primary is not visible in the peer view",
                    index.key(),
                    shardNumber
                );
            });

            if (buildable.isEmpty() == false) {
                kept.put(index.key(), index.withShards(buildable));
            }
        }

        if (declined.isEmpty()) {
            return desired;
        }
        List<String> deferred = new ArrayList<>(desired.deferred());
        deferred.addAll(declined);
        return new DesiredState(desired.assignmentVersion(), kept, desired.coordinatorTenants(), deferred);
    }

    private static boolean primaryVisible(CoordinatorView coordinated, String uuid, int shard) {
        for (CoordinatorView.RoutedIndex routed : coordinated.indices()) {
            if (routed.uuid().equals(uuid) == false) {
                continue;
            }
            for (IndexStatus.Copy copy : routed.placement().copiesOf(shard)) {
                boolean usable = copy.state() == ShardState.STARTED || copy.state() == ShardState.INITIALIZING;
                if (copy.role() == ShardRole.PRIMARY && usable && coordinated.peers().containsKey(copy.node())) {
                    return true;
                }
            }
        }
        return false;
    }
}
