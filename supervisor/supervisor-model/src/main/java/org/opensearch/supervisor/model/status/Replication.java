package org.opensearch.supervisor.model.status;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * Which replication strategy an index uses, copied from index.replication.type. It decides whether a copy has peers to resolve at all: a segment-replication copy pulls from the remote store and needs none.
 */
public enum Replication implements WireEnum {

    DOCUMENT,SEGMENT,

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
        return name();
    }

    public static Replication from(String value) {
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
