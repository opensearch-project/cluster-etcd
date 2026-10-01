/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.core.index.Index;

import java.util.ArrayList;
import java.util.List;

/**
 * How far the node has actually got, measured against what it was asked for.
 *
 * <p>Measured from the <b>live cluster state</b> rather than from what was applied, and that distinction is the
 * point. Applying a state is instantaneous and means almost nothing: every new copy in it is
 * {@code INITIALIZING}, and whether it becomes {@code STARTED} depends on a recovery that happens afterwards
 * and may not succeed. So "have I done what I was told" cannot be answered at the moment of telling. It is
 * re-derived every pass from the routing table the server is actually running, which makes it level-triggered
 * like everything else here.
 *
 * <p>This is what the heartbeat's {@code applied.outcome} is computed from. The contract is explicit that the
 * field is a hint and the inventory is the truth — a supervisor that ignored it entirely would still converge —
 * but a hint that lied would still be worth removing, and "applied" is the easiest lie to tell: the state went
 * in without error, so it is tempting to say so.
 */
public record Convergence(int desired, int serving, List<String> waitingFor) {

    /** How many copies to name before summarising, for the same reason the deferred reasons are bounded. */
    static final int MAX_NAMED = 10;

    public boolean isComplete() {
        return desired == serving;
    }

    /**
     * A description short enough for the heartbeat's 512-character detail.
     *
     * <p>Empty when everything is serving, because a detail on a successful ack is noise the supervisor would
     * have to decide to ignore.
     */
    public String detail() {
        if (isComplete()) {
            return null;
        }
        StringBuilder detail = new StringBuilder(serving + " of " + desired + " copies serving");
        if (waitingFor.isEmpty() == false) {
            detail.append("; waiting for ").append(String.join(", ", waitingFor));
        }
        return detail.toString();
    }

    public static Convergence assess(DesiredState desired, ClusterState live, String localNodeId) {
        int wanted = 0;
        int serving = 0;
        List<String> waiting = new ArrayList<>();

        for (DesiredState.DesiredIndex index : desired.indices().values()) {
            String indexName = IndexNaming.indexName(index.tenant(), index.uuid(), index.config().name());
            Index resolved = new Index(indexName, index.uuid());
            IndexMetadata metadata = live.metadata().index(resolved);
            IndexRoutingTable routing = metadata == null ? null : live.routingTable().index(resolved);

            for (var entry : index.shards().entrySet()) {
                wanted++;
                int shard = entry.getKey();
                ShardRole role = entry.getValue();
                String state = stateOf(routing, shard, role, localNodeId);
                if ("STARTED".equals(state)) {
                    serving++;
                } else if (waiting.size() < MAX_NAMED) {
                    waiting.add(index.key() + "/" + shard + " (" + state + ")");
                }
            }
        }
        return new Convergence(wanted, serving, List.copyOf(waiting));
    }

    /**
     * What the live state says about one copy on this node.
     *
     * <p>{@code ABSENT} covers two cases that cannot be told apart from here, and the second one matters: a
     * copy the state does not describe yet, and a copy whose shard <b>failed</b> and was removed from the
     * routing table. OpenSearch has no {@code FAILED} routing state — a failed shard is taken out rather than
     * marked — so a node that wants to report {@code FAILED} has to keep its own record of the failure. Until it
     * does, a shard that cannot recover looks exactly like one that has not started recovering, and the node
     * reports {@code applying} for ever rather than saying what is wrong. That is the visible edge of the same
     * gap recorded against the inventory's {@code FAILED} state.
     */
    private static String stateOf(IndexRoutingTable routing, int shard, ShardRole role, String localNodeId) {
        if (routing == null) {
            return "ABSENT";
        }
        IndexShardRoutingTable shardTable = routing.shard(shard);
        if (shardTable == null) {
            return "ABSENT";
        }
        for (ShardRouting copy : shardTable) {
            if (localNodeId.equals(copy.currentNodeId()) == false) {
                continue;
            }
            if (ShardRole.of(copy) != role) {
                // Present, but not as what was asked for. Reporting it as serving would tell the supervisor a
                // primary exists where a search replica does.
                return "ROLE_IS_" + ShardRole.of(copy);
            }
            return copy.started() ? "STARTED" : copy.state().name();
        }
        return "ABSENT";
    }
}
