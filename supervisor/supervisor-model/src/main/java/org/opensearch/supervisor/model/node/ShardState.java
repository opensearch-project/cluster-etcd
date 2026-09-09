package org.opensearch.supervisor.model.node;

import java.util.Locale;

/**
 * The state of one shard copy on one node, as the node reports it.
 *
 * <p>There is no {@code RELOCATING}. {@code NODE_CONTRACT.md} models a move as a copy added and then a
 * copy removed, so that an interrupted move leaves two ordinary observations rather than one state nobody
 * owns.
 */
public enum ShardState implements WireEnum {

    /** Serving as primary. */
    PRIMARY,

    /** Serving as replica. */
    REPLICA,

    /** Recovering or being created; present but not yet serving. */
    INITIALIZING,

    /** The copy exists and is broken. It occupies disk and cannot serve. */
    FAILED,

    /**
     * A state this build does not recognise, from a node newer than this supervisor.
     *
     * <p>Read-only, and the conservative reading is deliberate: an unknown state must never be counted as
     * a healthy copy, because that would let the supervisor believe a shard is served when it is not, and
     * must never be counted as absent either, because that would have it place a duplicate. It is
     * "something is there and I cannot say what", which is exactly a finding for a human.
     *
     * <p>The alternative — failing the whole inventory parse — loses every other shard on that node over
     * one unrecognised string, which during a rolling upgrade is how a cluster gets rebuilt underneath
     * itself.
     */
    UNKNOWN;

    /**
     * Whether this copy is serving traffic.
     *
     * <p>{@code UNKNOWN} is deliberately not serving, and {@code FAILED} is not either. Every state
     * occupies disk, including those two — {@code NODE_CONTRACT.md} open question 3 leans toward "present
     * for capacity, absent for health and routing" — so there is no counterpart predicate here: capacity
     * counts copies, not states.
     */
    public boolean isServing() {
        return this == PRIMARY || this == REPLICA;
    }

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name();
    }

    public static ShardState from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        return switch (value.toUpperCase(Locale.ROOT)) {
            case "PRIMARY" -> PRIMARY;
            case "REPLICA" -> REPLICA;
            case "INITIALIZING" -> INITIALIZING;
            case "FAILED" -> FAILED;
            default -> UNKNOWN;
        };
    }
}
