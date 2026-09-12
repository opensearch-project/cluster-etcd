/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;

/**
 * How a shard copy is getting on, as distinct from what it is — see {@link ShardRole}.
 *
 * <p>Two states OpenSearch has are deliberately absent from the contract, and the mapping is part of it
 * rather than left for each node implementation to invent:
 *
 * <ul>
 *   <li><b>{@code RELOCATING}</b> is reported as {@code STARTED} here and {@code INITIALIZING} on the
 *       target. The two observations then mean what they say even if the move is interrupted, instead of
 *       leaving one state that nobody owns.
 *   <li><b>{@code CLOSED}/{@code CLOSING}</b> — a closed index is reported as having no copies at all.
 * </ul>
 */
public enum ShardState {

    /** Recovering or being created; present, occupying disk, not yet serving. */
    INITIALIZING,

    /** Serving. */
    STARTED,

    /** The copy exists and is broken. It occupies disk and cannot serve. */
    FAILED;

    public boolean isServing() {
        return this == STARTED;
    }

    public String wireName() {
        return name();
    }

    /**
     * The contract state for a copy the routing table places on this node.
     *
     * <p>{@code UNASSIGNED} cannot occur for a copy this node holds and is rejected rather than mapped:
     * an unassigned routing has no node, so reporting one in this node's inventory would be claiming a
     * copy that is nowhere. {@code FAILED} is not a routing state at all — a shard that fails is removed
     * from the routing table rather than marked in it — so it comes from the node's own record of the
     * failure, never from here.
     *
     * <p><b>{@code SPLITTING} is a state the contract does not enumerate</b>, and it is a finding rather
     * than an oversight here: it is OpenSearch's in-place shard split, newer than the document, which
     * lists only {@code RELOCATING} and {@code CLOSED}/{@code CLOSING} as states it deliberately discards.
     * Mapped to {@code STARTED} because that is what it is — the server's own {@code active()} counts
     * splitting alongside started and relocating, and the copy is on disk and serving throughout. The
     * information lost is the same information a relocation loses, and for the same reason: the
     * supervisor plans placement and a split is not a placement.
     */
    public static ShardState of(ShardRouting routing) {
        ShardRoutingState state = routing.state();
        return switch (state) {
            case INITIALIZING -> INITIALIZING;
            case STARTED, RELOCATING, SPLITTING -> STARTED;
            case UNASSIGNED -> throw new IllegalArgumentException(
                "shard " + routing.shardId() + " is unassigned and cannot appear in a node's inventory"
            );
        };
    }
}
