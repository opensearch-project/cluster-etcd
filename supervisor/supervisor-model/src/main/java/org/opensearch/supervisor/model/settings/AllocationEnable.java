package org.opensearch.supervisor.model.settings;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * The value of {@code routing.allocation.enable} — an operator's brake on shard movement, at cluster or index
 * level.
 *
 * <p>Exists for maintenance: "stop moving my data around while I restart these machines" is a thing an
 * operator legitimately needs to say, and a reconciler that cannot be told to stop is one nobody will trust
 * with a production cluster.
 */
public enum AllocationEnable implements WireEnum {

    /** The default. Everything may be allocated. */
    ALL,

    /** Primaries only; replicas wait. */
    PRIMARIES,

    /**
     * Primaries of indices that have never been allocated — and <b>this supervisor cannot tell those apart</b>.
     *
     * <p>OpenSearch distinguishes them by a shard's unassigned reason, which is history: it knows the shard was
     * created rather than lost. Nothing here keeps per-shard history, deliberately — R2.3 forbids state that
     * lives only in memory, and nothing in the store records it. From an observation, "never allocated" and
     * "every copy has gone" look identical.
     *
     * <p>So it is applied as {@link #NONE}, which is the safe direction to be wrong in. An operator who set
     * this was protecting a cluster from data movement during maintenance; allowing recovery of an existing
     * primary would give them more movement than they asked for, while blocking a new index leaves a
     * create-index task visibly pending and nothing at risk. The decider says so in its reason rather than
     * quietly picking an interpretation.
     */
    NEW_PRIMARIES,

    /** Nothing may be allocated. */
    NONE,

    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name().toLowerCase(Locale.ROOT);
    }

    public static AllocationEnable from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    /** Whether a role may be allocated at all under this setting. */
    public boolean allows(boolean primary) {
        return switch (this) {
            case ALL -> true;
            case PRIMARIES -> primary;
            // See the constant's own note: applied as NONE because "new" is not observable here.
            case NEW_PRIMARIES, NONE -> false;
            // An unrecognised value is a value an operator meant something by. Refusing to allocate is the
            // conservative reading and it becomes visible immediately; guessing "all" would silently ignore
            // an instruction to stop.
            case UNKNOWN -> false;
        };
    }
}
