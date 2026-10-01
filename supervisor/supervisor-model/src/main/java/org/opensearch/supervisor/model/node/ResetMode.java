package org.opensearch.supervisor.model.node;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * How a consumer-pointer reset names its target position.
 *
 * <p>Shared by the task that asks for a reset and the assignment that carries it, because <b>this is the field
 * three repositories could each spell differently</b>. Upstream's enum is
 * {@code StreamPoller.ResetState.RESET_BY_OFFSET} and {@code RESET_BY_TIMESTAMP}; the control plane validates the
 * wire value case-insensitively and stores whatever the client sent, so {@code "OFFSET"}, {@code "offset"} and
 * {@code "Offset"} all arrive here.
 *
 * <p>So this parses tolerantly and writes one canonical lower-case form. The assignment therefore has exactly one
 * spelling, and the node maps that to the upstream enum — one translation, in one place, instead of three services
 * guessing at a middle form.
 */
public enum ResetMode implements WireEnum {

    /** A position in the stream, as a string because offsets are not all longs. */
    OFFSET,

    /** Epoch milliseconds, as a string for the same reason. */
    TIMESTAMP,

    /**
     * A mode this build does not recognise.
     *
     * <p><b>Kept rather than dropped</b>, which is the opposite of what the data plane does with a malformed reset,
     * and deliberately so on both sides: a reset whose mode is unknown is a real instruction this software cannot
     * carry out, so it has to reach a node and be refused visibly. Silently discarding it would leave an operator
     * with a task that never completes and nothing anywhere saying why.
     */
    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name().toLowerCase(Locale.ROOT);
    }

    public static ResetMode from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
