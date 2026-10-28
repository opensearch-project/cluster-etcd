/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.contract.IndexStatus;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.ShardState;
import org.opensearch.core.index.Index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Which of this node's started document-replication replicas to rebuild, because their primary no longer holds them in
 * sync.
 *
 * <p>A primary routes a started copy only if its own tracker holds it in sync ({@link CoordinatorState}); the server
 * rejects anything else. So a replica that falls out of the set -- because its primary restarted or was promoted and
 * knows nothing of it, or because a write to it failed and it was dropped -- is no longer written to. Nothing on the
 * replica's node notices: the copy is started and healthy from where it stands, and goes on answering reads from what it
 * had. Rebuilding it as initializing, under a new allocation id, makes the server close it and recover it from the
 * primary, which puts it back in the set by the ordinary route. It is the one place where reusing a serving entry (D2.2)
 * is deliberately not done.
 *
 * <h2>Not on absence alone</h2>
 *
 * <p>The primary marks a copy in sync as its recovery finishes, and the copy starts a moment later. The primary
 * publishes its new set on its next pass and the supervisor carries it on its own, so a replica that has just started
 * can read its primary's set from before it was in it. Rebuilding then would restart a healthy copy, and could do so
 * repeatedly. So absence counts only with something else that says the set is current:
 *
 * <ul>
 *   <li><b>the primary changed</b>: its allocation id is not the one this copy recovered from. A primary that restarts
 *       comes back under a new id, and a promoted one has the id it had as a replica;
 *   <li><b>the copy was in the set and is not now</b>: it was dropped, which is what a failed write to it does;
 *   <li><b>or it never joined</b> within {@link #PASSES_TO_JOIN} passes of starting, which covers a primary that changed
 *       while this copy was still recovering, so that the one it recovered from is not on record.
 * </ul>
 *
 * <p>A primary that publishes no set at all says nothing, and nothing is rebuilt. The memory is this process's, and
 * that is enough: a replica whose node restarts recovers from its primary anyway.
 *
 * <p>Run once per pass, like {@link RetainedIndexReaper}, because it counts passes and the fabrication runs twice on a
 * pass that applies.
 */
public final class StaleReplicas {

    private static final Logger logger = LogManager.getLogger(StaleReplicas.class);

    /** How long a started copy may wait to appear in its primary's set before that is taken as the answer. */
    static final int PASSES_TO_JOIN = 10;

    private final String localNodeId;

    /** By the allocation id of this node's copy, which a rebuild changes, so a rebuilt copy starts afresh. */
    private final Map<String, Seen> seen = new HashMap<>();

    private static final class Seen {
        String primary;
        boolean inSync;
        int startedPasses;
    }

    public StaleReplicas(String localNodeId) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
    }

    /** The allocation ids of this node's copies to rebuild this pass. */
    public Set<String> toRebuild(DesiredState desired, ClusterState live, CoordinatorView view) {
        Set<String> rebuild = new HashSet<>();
        Set<String> present = new HashSet<>();

        for (DesiredState.DesiredIndex index : desired.indices().values()) {
            if ("DOCUMENT".equals(index.config().replication()) == false) {
                continue;
            }
            Index built = new Index(IndexNaming.indexName(index.tenant(), index.uuid(), index.config().name()), index.uuid());
            IndexRoutingTable routing = live.routingTable().index(built);
            IndexStatus placement = placementOf(view, index.uuid());
            if (routing == null) {
                continue;
            }
            index.shards().forEach((shard, role) -> {
                if (role != ShardRole.REPLICA) {
                    return;
                }
                ShardRouting mine = localCopy(routing.shard(shard));
                if (mine == null || mine.primary() || mine.isSearchOnly()) {
                    return;
                }
                String id = mine.allocationId().getId();
                present.add(id);
                IndexStatus.Copy primary = placement == null ? null : primaryOf(placement.copiesOf(shard));
                if (decide(id, mine, primary, placement == null ? null : placement.inSyncOf(shard))) {
                    logger.info(
                        "clusterless: rebuilding this node's replica of {}/{} [{}]: its primary [{}] does not hold it in sync",
                        index.key(),
                        shard,
                        id,
                        primary.allocationId()
                    );
                    rebuild.add(id);
                }
            });
        }

        seen.keySet().retainAll(present);
        return rebuild;
    }

    private boolean decide(String id, ShardRouting mine, IndexStatus.Copy primary, List<String> inSync) {
        if (mine.initializing()) {
            // Recovering, from whichever primary the projection names now. That is the one to judge it against once started.
            Seen record = new Seen();
            record.primary = primary == null ? null : primary.allocationId();
            seen.put(id, record);
            return false;
        }
        if (mine.started() == false) {
            return false;
        }
        Seen record = seen.computeIfAbsent(id, key -> {
            Seen first = new Seen();
            first.primary = primary == null ? null : primary.allocationId();
            return first;
        });
        record.startedPasses++;
        if (inSync != null && inSync.contains(id)) {
            record.inSync = true;
            return false;
        }
        if (primary == null || primary.state() != ShardState.STARTED || primary.allocationId() == null || inSync == null) {
            // No started primary saying anything about its set, so nothing to act on.
            return false;
        }
        // Unknown is not a change: a copy whose primary was not on record is left to the bounded wait below.
        boolean primaryChanged = record.primary != null && primary.allocationId().equals(record.primary) == false;
        boolean dropped = record.inSync;
        boolean neverJoined = record.startedPasses > PASSES_TO_JOIN;
        return primaryChanged || dropped || neverJoined;
    }

    private ShardRouting localCopy(IndexShardRoutingTable shard) {
        if (shard == null) {
            return null;
        }
        for (ShardRouting copy : shard) {
            if (localNodeId.equals(copy.currentNodeId())) {
                return copy;
            }
        }
        return null;
    }

    private static IndexStatus placementOf(CoordinatorView view, String uuid) {
        for (CoordinatorView.RoutedIndex routed : view.indices()) {
            if (routed.uuid().equals(uuid)) {
                return routed.placement();
            }
        }
        return null;
    }

    private static IndexStatus.Copy primaryOf(List<IndexStatus.Copy> copies) {
        for (IndexStatus.Copy copy : copies) {
            if (copy.role() == ShardRole.PRIMARY) {
                return copy;
            }
        }
        return null;
    }
}
