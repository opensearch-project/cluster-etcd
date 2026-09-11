package org.opensearch.supervisor.model.settings;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * {@code cluster.routing.rebalance.enable} — separate from {@link AllocationEnable} on purpose, and the
 * separation matters more than it looks.
 *
 * <p>Allocation is how a cluster gets back the redundancy it is missing; rebalancing is how it gets tidier.
 * An operator in the middle of an incident wants the first and emphatically does not want the second, and one
 * knob for both would make that impossible to say. Turning rebalancing off is the normal, healthy thing to do
 * during a rolling restart or a capacity event; turning allocation off is a bigger decision.
 *
 * <p>The implication runs one way only: no allocation means no rebalancing, because a move is an allocation
 * followed by a removal. No rebalancing does not mean no allocation.
 */
public enum RebalanceEnable implements WireEnum {

    /** Move anything. */
    ALL,
    /** Move primaries only. */
    PRIMARIES,
    /** Move replicas only — the conservative choice, and what this service can currently do at all. */
    REPLICAS,
    /** Move nothing. */
    NONE,
    /** Either unset, or set to something unrecognised. Treated as {@link #NONE} — see {@link #allows}. */
    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a reading of somebody else's document, not a value to"
                + " write; something has tried to serialise a rebalance setting it did not understand");
        }
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Whether a copy of this role may be moved.
     *
     * <p>{@code UNKNOWN} refuses, which is the same reasoning as the allocation brake: a value an operator
     * typed meant something, and reading it as "move anything" would silently ignore an instruction to stop.
     * The difference is that here the conservative reading costs nothing at all — an unmoved shard is a
     * slightly untidy cluster, not an unavailable one.
     */
    public boolean allows(boolean primary) {
        return switch (this) {
            case ALL -> true;
            case PRIMARIES -> primary;
            case REPLICAS -> primary == false;
            case NONE, UNKNOWN -> false;
        };
    }

    public static RebalanceEnable from(String raw) {
        if (raw == null || raw.isBlank()) {
            return UNKNOWN;
        }
        try {
            RebalanceEnable value = valueOf(raw.trim().toUpperCase(Locale.ROOT));
            return value == UNKNOWN ? UNKNOWN : value;
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
