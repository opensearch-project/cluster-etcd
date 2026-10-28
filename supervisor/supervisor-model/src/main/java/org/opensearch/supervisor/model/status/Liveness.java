package org.opensearch.supervisor.model.status;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * How a node looks to the supervisor, derived from how long since its heartbeat key last changed revision rather than from the node's own clock. STALE is the first threshold and LOST the second; the gap between them is the tolerance for a GC pause.
 */
public enum Liveness implements WireEnum {

    LIVE,STALE,LOST,

    /** A value this build does not recognise. Read-only; never written back.
     *
     * <p>See {@link WireEnum} for why the guard is on the serializer rather than on {@code @JsonValue}.
     */
    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name().toLowerCase(Locale.ROOT);
    }

    public static Liveness from(String value) {
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
