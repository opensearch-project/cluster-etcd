package org.opensearch.supervisor.model.task;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * Where a task has got to. The control plane only ever writes {@link #PENDING}; every other value is the
 * supervisor's half of the handoff, and {@code cluster.pending_tasks} reports only the first — so the pile
 * starts draining the moment claiming works, before completion is even implemented.
 */
public enum TaskState implements WireEnum {

    /** As written by the control plane. */
    PENDING,

    /** A supervisor has taken it up. Carries a holder and a lease that can expire. */
    CLAIMED,

    /** Done. Reaped after a retention window, which is the only reason it is written rather than deleted. */
    COMPLETED,

    /**
     * Given up on after too many attempts.
     *
     * <p><b>Never reaped.</b> R4.2 wants a task that has failed repeatedly to become visible rather than
     * looping silently, and a deleted failure is not visible. Clearing these is an operator's decision.
     */
    FAILED,

    UNKNOWN;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED;
    }

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name().toLowerCase(Locale.ROOT);
    }

    public static TaskState from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
