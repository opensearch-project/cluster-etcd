/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.core.index.Index;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Whether two cluster states say the same thing about this node's indices.
 *
 * <p>This exists because of a bug found by running the thing, and it is worth writing down what the bug was.
 * The fabricator does {@code version(previous.version() + 1)}, which is right — the applier ignores a state
 * that is not newer. But a poll that applies unconditionally then publishes a <em>new</em> state on every pass
 * whether or not anything changed, because the version alone makes every state different. On a one-second poll
 * that was a fresh cluster state every second, climbing past version 1900, each one running every
 * {@code ClusterStateApplier} and {@code ClusterStateListener} in the server for no reason. The shard stayed
 * started throughout — D2.2 held — so nothing broke; it was pure waste, plus a log line per second that made
 * the interesting ones invisible.
 *
 * <p>Level-triggered convergence needs a fixed point, and this is it: build the state the assignment implies,
 * and apply it only if it differs from what is already applied. Without that, "converged" is a state the system
 * passes through rather than one it rests in.
 *
 * <p><b>Compared on content, not by {@code equals}.</b> {@link ClusterState} has no equality and could not
 * usefully have one — the version differs by construction, and so would any timestamp in it. What matters is
 * what this plugin actually fabricates: which indices exist, their metadata, and the routing table for each.
 * {@link IndexMetadata} and {@link IndexRoutingTable} both implement {@code equals}, which is what makes this a
 * comparison rather than a heuristic. Cluster-scoped settings are compared too, for a reason given at the call —
 * they are the one part of the state outside the indices that anything here would ever change. Beyond those, the
 * state is constant: the node list is this node, and the blocks and customs are whatever a fresh builder produces.
 */
public final class StateEquivalence {

    private StateEquivalence() {}

    /**
     * @return true when applying {@code intended} would change nothing about the indices this node holds
     */
    public static boolean sameContent(ClusterState applied, ClusterState intended) {
        // Cluster-scoped settings, and they are here because leaving them out makes an instruction vanish.
        // ClusterApplierService applies settings from an applied state -- clusterSettings.applySettings(
        // event.state().metadata().settings()) -- but only when metadataChanged(), which only happens on a pass that
        // publishes. So a state differing *only* by a cluster setting would compare equal here, never be published,
        // and the setting would never reach its update consumer: the node would carry the value in the state it
        // fabricated and the running components would not have it.
        //
        // Nothing sets one yet. It matters because the supervisor's DESIGN.md §27.3 wants exactly this shape --
        // turning segment cleanup off on a shard's primaries for the length of a deployment, via the dynamic
        // cluster setting cluster.remote_store.index.segment_metadata.retention.max_count = -1, so that a rollback
        // stays possible -- and an instruction that is silently dropped there leaves cleanup running while the
        // deployment believes it is off, which is the failure that design is trying to avoid.
        if (applied.metadata().settings().equals(intended.metadata().settings()) == false) {
            return false;
        }

        Set<Index> appliedIndices = indicesOf(applied);
        Set<Index> intendedIndices = indicesOf(intended);
        if (appliedIndices.equals(intendedIndices) == false) {
            // An index appearing or disappearing is the most consequential difference there is, and the cheapest
            // to detect. Note that Index equality includes the uuid, so an index recreated under the same name
            // is a different index here — which is the whole reason identity is the uuid.
            return false;
        }

        for (Index index : intendedIndices) {
            if (Objects.equals(applied.metadata().index(index), intended.metadata().index(index)) == false) {
                return false;
            }
            if (Objects.equals(applied.routingTable().index(index), intended.routingTable().index(index)) == false) {
                // Covers a copy that changed role, moved state, or gained an allocation id — and, importantly,
                // a copy this node was told to stop holding while the index itself stays.
                return false;
            }
        }
        return true;
    }

    private static Set<Index> indicesOf(ClusterState state) {
        Set<Index> indices = new HashSet<>();
        for (IndexMetadata index : state.metadata()) {
            indices.add(index.getIndex());
        }
        return indices;
    }
}
