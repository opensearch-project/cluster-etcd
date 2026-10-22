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
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.ShardState;
import org.opensearch.core.index.Index;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Which of the assigned primary relocations this node can act on this pass, and with which allocation ids.
 *
 * <p>A relocation is OpenSearch's planned move of a primary ({@code NODE_CONTRACT.md}, <i>Proposed: relocating a
 * document-replication primary</i>). In a routing table it is one entry, the source's, {@code RELOCATING} to the target's
 * node, under the source's allocation id with the target's as its relocation id; the target's entry is derived from it.
 * Both nodes have to build that one entry with the same two ids. The source's node mints the target's id
 * ({@code ShardRouting.relocate}) and publishes it; the target's node reads it from the projection. This step is where
 * the target's node does that reading, and where both ends check that the other is in the peer view. It runs after the
 * view is read, like {@link PeerRecovery}, and once per pass, because it remembers which targets this node has built.
 *
 * <h2>The source</h2>
 *
 * <p>Acted on only while the target's node is in the peer view. The source replicates every write to its target while
 * the target recovers, and a copy on a node the state does not contain fails as an unknown node. Otherwise the
 * relocation is dropped with a reason, and {@link DataNodeState} keeps, or puts back, a plain primary.
 *
 * <h2>The target</h2>
 *
 * <ul>
 *   <li><b>Already started here</b>: the relocation has completed on this node ({@code LocalShardStateAction} starts a
 *       relocation target by replacing the source's entry with the started target). The shard is then a plain primary,
 *       whatever the assignment still says until the supervisor rewrites it.
 *   <li><b>Recovering here</b>: the live state holds the source's entry relocating to this node, and its ids are used,
 *       so that a projection that changes during the recovery does not restart it.
 *   <li><b>Otherwise</b>, built once the projection shows the source's copy as a started primary relocating to this
 *       node, with the id its node minted, and the source's node is in the peer view. Until then, nothing is built.
 *   <li><b>Failed</b>: a target this node has seen recovering that is no longer in the live state was failed, and
 *       {@code LocalShardStateAction} cancelled the relocation. Building it again under the same id would do nothing,
 *       because the server does not create a failed shard again until its routing entry names a different allocation. So
 *       it is declined with a reason, until the source's node mints a new id: the supervisor cancels and asks again.
 * </ul>
 */
public final class PrimaryRelocations {

    private static final Logger logger = LogManager.getLogger(PrimaryRelocations.class);

    private final String localNodeId;

    /**
     * Target allocation ids whose relocating entry this node has seen in its live state, while the assignment still names
     * them. Seen rather than built, because an apply can still be on its way on the next pass.
     */
    private final Set<String> built = new HashSet<>();

    /** Of those, the ones whose target failed here. */
    private final Set<String> failed = new HashSet<>();

    public PrimaryRelocations(String localNodeId) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId");
    }

    public DesiredState reduce(DesiredState desired, CoordinatorView view, ClusterState live) {
        List<String> declined = new ArrayList<>();
        Map<DesiredState.IndexKey, DesiredState.DesiredIndex> kept = new TreeMap<>();
        Set<String> named = new HashSet<>();
        boolean changed = false;

        for (DesiredState.DesiredIndex index : desired.indices().values()) {
            if (index.relocatingTo().isEmpty() && index.relocatingFrom().isEmpty()) {
                kept.put(index.key(), index);
                continue;
            }
            Index built = new Index(IndexNaming.indexName(index.tenant(), index.uuid(), index.config().name()), index.uuid());
            IndexRoutingTable routing = live.routingTable().index(built);
            IndexStatus placement = placementOf(view, index.uuid());

            Map<Integer, NodeId> to = new TreeMap<>();
            index.relocatingTo().forEach((shard, target) -> {
                if (view.peers().containsKey(target)) {
                    to.put(shard, target);
                } else {
                    declined.add(
                        index.key() + "/" + shard + ": not relocating the primary to [" + target + "] yet: that node is not in "
                            + "this node's peer view, and the source replicates to its target from the start"
                    );
                }
            });

            Map<Integer, DesiredState.IncomingRelocation> from = new TreeMap<>();
            index.relocatingFrom().forEach((shard, incoming) -> {
                IndexShardRoutingTable copies = routing == null ? null : routing.shard(shard);
                if (startedPrimaryHere(copies)) {
                    // Done here. A plain primary from now on, which is what DataNodeState reuses.
                    return;
                }
                ShardRouting recovering = relocatingHere(copies, incoming.source());
                if (recovering != null) {
                    String target = recovering.allocationId().getRelocationId();
                    named.add(target);
                    // Seen in the live state, so a later absence is a failure rather than an apply still on its way.
                    this.built.add(target);
                    from.put(shard, new DesiredState.IncomingRelocation(incoming.source(), recovering.allocationId().getId(), target));
                    return;
                }
                IndexStatus.Copy source = placement == null ? null : sourceOf(placement.copiesOf(shard), incoming.source());
                if (source == null || source.relocationTargetAllocationId() == null || view.peers().containsKey(incoming.source()) == false) {
                    declined.add(
                        index.key() + "/" + shard + ": waiting to build the relocation target from [" + incoming.source() + "]: "
                            + "the projection does not yet show that node's started primary relocating here, with the id it "
                            + "minted for the target, on a node in this node's peer view"
                    );
                    return;
                }
                String target = source.relocationTargetAllocationId();
                named.add(target);
                if (failed.contains(target) || this.built.contains(target)) {
                    // Seen recovering before, and neither recovering nor started now: failed, and the relocation cancelled.
                    failed.add(target);
                    declined.add(
                        index.key() + "/" + shard + ": the relocation target under allocation id [" + target + "] failed on this "
                            + "node; it is built again only under a new id, which the source's node mints when the relocation is "
                            + "cancelled and asked for again"
                    );
                    return;
                }
                from.put(shard, new DesiredState.IncomingRelocation(incoming.source(), source.allocationId(), target));
            });

            Map<Integer, ShardRole> roles = new TreeMap<>(index.shards());
            index.relocatingFrom().keySet().forEach(shard -> {
                // A target that cannot be built yet is nothing at all: building it as a plain primary is two primaries.
                boolean doneHere = routing != null && startedPrimaryHere(routing.shard(shard));
                if (from.containsKey(shard) == false && doneHere == false) {
                    roles.remove(shard);
                }
            });

            DesiredState.DesiredIndex reduced = index.with(roles, index.primaryTerms()).withRelocations(to, from);
            changed |= reduced.equals(index) == false;
            if (reduced.shards().isEmpty() == false) {
                kept.put(index.key(), reduced);
            }
        }

        this.built.retainAll(named);
        failed.retainAll(named);
        if (changed == false && declined.isEmpty()) {
            return desired;
        }
        if (declined.isEmpty() == false) {
            logger.debug("clusterless: relocations not acted on this pass: {}", declined);
        }
        List<String> deferred = new ArrayList<>(desired.deferred());
        deferred.addAll(declined);
        return new DesiredState(desired.assignmentVersion(), kept, desired.coordinatorTenants(), deferred);
    }

    private boolean startedPrimaryHere(IndexShardRoutingTable copies) {
        if (copies == null) {
            return false;
        }
        for (ShardRouting copy : copies) {
            if (copy.primary() && copy.started() && localNodeId.equals(copy.currentNodeId())) {
                return true;
            }
        }
        return false;
    }

    /** The source's entry relocating to this node, from {@code source}, or null. */
    private ShardRouting relocatingHere(IndexShardRoutingTable copies, NodeId source) {
        if (copies == null) {
            return null;
        }
        for (ShardRouting copy : copies) {
            if (copy.primary() && copy.relocating() && source.value().equals(copy.currentNodeId()) && localNodeId.equals(copy.relocatingNodeId())) {
                return copy;
            }
        }
        return null;
    }

    /** The source's copy as the projection shows it, if it is a started primary relocating to this node. */
    private IndexStatus.Copy sourceOf(List<IndexStatus.Copy> copies, NodeId source) {
        for (IndexStatus.Copy copy : copies) {
            if (copy.node().equals(source)
                && copy.role() == ShardRole.PRIMARY
                && copy.state() == ShardState.STARTED
                && copy.allocationId() != null
                && copy.relocatingTo() != null
                && copy.relocatingTo().value().equals(localNodeId)) {
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
}
