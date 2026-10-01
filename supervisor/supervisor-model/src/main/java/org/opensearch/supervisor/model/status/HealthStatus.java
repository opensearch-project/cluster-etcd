package org.opensearch.supervisor.model.status;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * OpenSearch's health colours. Red if any primary is not serving, yellow if any replica is not, green otherwise.
 */
public enum HealthStatus implements WireEnum {

    GREEN,YELLOW,RED,

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

    public static HealthStatus from(String value) {
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
