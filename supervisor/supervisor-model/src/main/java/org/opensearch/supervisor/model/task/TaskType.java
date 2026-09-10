package org.opensearch.supervisor.model.task;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * What a task is about. The six the control plane writes today, per {@code KEY_LAYOUT.md}, hyphenated on the
 * wire and underscored in Java.
 */
public enum TaskType implements WireEnum {

    CREATE_INDEX,
    DELETE_INDEX,
    UPDATE_MAPPING,
    UPDATE_SETTINGS,
    CLOSE_INDEX,
    OPEN_INDEX,

    /**
     * A type this build does not recognise.
     *
     * <p>Deliberately <b>not</b> treated as complete. Completing a task whose meaning is unknown would drop
     * work silently and report success for it; blocking the queue on it would stall every other task behind
     * it. So it is left alone and reported, which is the only honest option — a newer control plane writing
     * a task an older supervisor cannot serve is a real deployment state, not a corruption.
     */
    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name().toLowerCase(Locale.ROOT).replace('_', '-');
    }

    public static TaskType from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        try {
            return valueOf(value.toUpperCase(Locale.ROOT).replace('-', '_'));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
