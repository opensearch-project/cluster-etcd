/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.IndexStatus;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.NodeStatus;
import org.opensearch.clusterless.contract.TenantId;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Where every copy of a coordinated tenant's shards is, and how to reach the nodes holding them.
 *
 * <p>Read once per pass and handed to the fabrication already settled, for the reason everything else here is:
 * {@link CoordinatorState} is a pure function called twice on a pass that applies, and this is several reads of
 * the store.
 *
 * <p>Two projections joined with one index document each. The placement comes from
 * {@code tenant/<t>/status/indices/<uuid>}, the addresses from {@code tenant/<t>/status/nodes/<id>}, and the
 * settings and mappings from the control plane's {@code tenant/<t>/indices/<name>} — because a coordinator has to
 * put an index in its cluster state to route to it, and an index in a cluster state needs a shard count and a
 * mapping whether or not this node holds any of it.
 *
 * @param deferred indices and copies left out, with a reason each — the same discipline as
 *     {@link DesiredState#deferred()}, and for the same reason: a coordinator that silently cannot reach half a
 *     tenant's shards is indistinguishable from one whose tenant has half as many shards.
 */
public record CoordinatorView(List<RoutedIndex> indices, Map<NodeId, NodeStatus> peers, List<String> deferred) {

    public CoordinatorView {
        indices = indices == null ? List.of() : List.copyOf(indices);
        peers = peers == null ? Map.of() : Collections.unmodifiableMap(new java.util.LinkedHashMap<>(peers));
        deferred = deferred == null ? List.of() : List.copyOf(deferred);
    }

    /** Nothing coordinated, which is every node that was not given the role. */
    public static CoordinatorView nothing() {
        return new CoordinatorView(List.of(), Map.of(), List.of());
    }

    public boolean isEmpty() {
        return indices.isEmpty();
    }

    /**
     * Copies this node could actually answer a query from: serving, on a peer it kept.
     *
     * <p>Deliberately stricter than what reaches the routing table, which also carries {@code INITIALIZING} copies
     * because the state should say what the fleet is doing. A query cannot be answered from one of those, so the
     * number worth reporting is this one.
     */
    public int servingCopies() {
        int serving = 0;
        for (RoutedIndex index : indices) {
            serving += servingCopiesOf(index);
        }
        return serving;
    }

    /**
     * Indices this node coordinates and can answer nothing for.
     *
     * <p>The state that otherwise looks like health: the assignment was applied, the routing table was built, no
     * copy was declined, and a search returns an empty result because every copy is somewhere this node cannot
     * reach or has not started. Worth a line on the heartbeat rather than only in a local endpoint.
     */
    public List<String> indicesWithNothingServing() {
        List<String> nothing = new java.util.ArrayList<>();
        for (RoutedIndex index : indices) {
            if (servingCopiesOf(index) == 0) {
                nothing.add(index.tenant() + "/" + index.config().name());
            }
        }
        return List.copyOf(nothing);
    }

    private int servingCopiesOf(RoutedIndex index) {
        int serving = 0;
        for (Integer shard : index.shards()) {
            for (org.opensearch.clusterless.contract.IndexStatus.Copy copy : index.placement().copiesOf(shard)) {
                if (peers.containsKey(copy.node()) && copy.state() == org.opensearch.clusterless.contract.ShardState.STARTED) {
                    serving++;
                }
            }
        }
        return serving;
    }

    /**
     * One index this node needs other nodes' copies of, which of its shards, and which copies of those shards.
     *
     * <p><b>Two reasons to be here, and they want very different amounts.</b> A tenant this node
     * <em>coordinates</em> contributes every shard of every one of its indices, because a coordinator answers
     * queries about all of them. An index this node merely <em>holds copies of</em> contributes only the shards it
     * holds, and of those only the copies it has a <em>relationship</em> with — see {@link #needs}.
     *
     * @param config the control plane's document, for the settings and mappings the cluster state needs
     * @param placement the supervisor's observation of where the copies are, including ones not yet started
     * @param shards which of the placement's shards to put in the routing table
     * @param localRoles what this node holds of each of those shards, or empty when this index is here because the
     *     tenant is coordinated — which is what {@link #needs} dispatches on
     */
    public record RoutedIndex(
        TenantId tenant,
        IndexConfig config,
        IndexStatus placement,
        Set<Integer> shards,
        Map<Integer, ShardRole> localRoles
    ) {

        public RoutedIndex {
            Objects.requireNonNull(tenant, "tenant");
            Objects.requireNonNull(config, "config");
            Objects.requireNonNull(placement, "placement");
            shards = shards == null ? Set.of() : Set.copyOf(shards);
            localRoles = localRoles == null ? Map.of() : Map.copyOf(localRoles);
        }

        /** Every shard the supervisor observed, which is what a coordinated tenant's index contributes. */
        public static RoutedIndex wholeIndex(TenantId tenant, IndexConfig config, IndexStatus placement) {
            return new RoutedIndex(tenant, config, placement, placement.shards().keySet(), Map.of());
        }

        /**
         * Whether this node needs another node's copy of one of its own shards in its routing table.
         *
         * <p><b>The rule is a relationship, not a shard.</b> Asking the projection for "shard 0" yields every copy
         * of shard 0, and on a five-node index with one primary and four search replicas that put all five copies
         * into all five nodes' states. Most of that is surplus, and the cost is not the size — it is that every
         * copy republishes its cluster state whenever any copy changes state, which is quadratic in copies per
         * shard on the feature whose whole purpose is scaling reads out.
         *
         * <p>So, per shard, by what this node holds of it:
         *
         * <ul>
         *   <li><b>A primary needs nothing.</b> Under remote-store replication a primary uploads segments and the
         *       replicas poll; it does not need to know who they are. This used to include them, because a primary
         *       with no replicas in its table logs {@code UnavailableShardsException: Not enough active copies}
         *       once per checkpoint from {@code PublishCheckpointAction} — but that is a primary trying to push
         *       checkpoints in a deployment where nothing reads them, which is an upstream defect to fix upstream
         *       rather than a reason to inflate every primary's state.
         *   <li><b>A search replica needs the primary, and only the primary.</b> Not to replicate — it polls the
         *       remote store — but because {@code RoutingNodes} requires {@code index.blocks.search_only} on an
         *       index whose routing table has no primary, and that block disables
         *       {@code IndexService.AsyncReplicationTask}, the timer doing the polling. So the primary's presence is
         *       what keeps a search replica current, and a fabricated node id would satisfy the assertion just as
         *       well — which is the clearest sign the assertion is the thing that should go.
         *   <li><b>A document-replication replica gets everything, for now.</b> Its replication group is a real one
         *       and M9 owns the question; narrowing it before then would be guessing.
         * </ul>
         *
         * <p><b>Document replication is checked first, and inverts the primary's answer.</b> A docrep primary's
         * replication group is exactly the copies in its own routing table, so one that knows no replicas does not
         * merely fail to push — it acknowledges writes that only it holds. That is the one failure this narrowing
         * could cause that nothing would report, which is why the check is on the index rather than on the role.
         */
        public boolean needs(int shard, ShardRole copyRole) {
            if (localRoles.isEmpty()) {
                // Here because the tenant is coordinated: this node answers queries about the whole index.
                return true;
            }
            ShardRole mine = localRoles.get(shard);
            if (mine == null) {
                // A shard of this index that this node does not hold, which the shard set should already have
                // excluded. Nothing needs it.
                return false;
            }
            // Document replication is the case where every copy genuinely is a peer, and it is checked before the
            // role because the reasoning is the opposite way round: a docrep primary's replication group *is* the
            // copies in its own routing table -- ReplicationOperation sends to what ReplicationTracker found there
            // -- so a primary that knows no replicas does not publish to nobody, it acknowledges writes that only
            // it has. That is the one failure this narrowing must not cause, and it is silent.
            if ("DOCUMENT".equals(config.replication())) {
                return true;
            }
            return switch (mine) {
                case PRIMARY -> false;
                case SEARCH_REPLICA -> copyRole == ShardRole.PRIMARY;
                case REPLICA, UNKNOWN -> true;
            };
        }

        public String uuid() {
            return placement.uuid();
        }
    }
}
