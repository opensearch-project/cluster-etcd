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
    FAILED,

    /**
     * A state this build does not recognise, from a supervisor newer than it.
     *
     * <p>Read-only, like the sentinel on every other enum here. It exists because this enum acquired a second
     * direction: a node <em>writes</em> its own copies' states, where the set is closed and known, and it now also
     * <em>reads</em> other nodes' states out of {@code tenant/<t>/status/indices/<uuid>} to route to them. Failing
     * that parse over one unfamiliar string would cost a coordinator the placement of a whole index, so an
     * unrecognised state is carried as "present, and not something I can act on" instead.
     */
    UNKNOWN;

    public boolean isServing() {
        return this == STARTED;
    }

    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name();
    }

    /**
     * The contract state named by a document, tolerantly.
     *
     * <p>Case-insensitive on purpose rather than by accident: node-written documents in this package spell enums
     * in lower case and the supervisor's projections spell these in upper case, and a reader that depended on
     * which would be one refactor away from breaking on a document it should have understood.
     */
    public static ShardState from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        return switch (value.trim().toUpperCase(java.util.Locale.ROOT)) {
            case "INITIALIZING" -> INITIALIZING;
            case "STARTED" -> STARTED;
            case "FAILED" -> FAILED;
            default -> UNKNOWN;
        };
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
