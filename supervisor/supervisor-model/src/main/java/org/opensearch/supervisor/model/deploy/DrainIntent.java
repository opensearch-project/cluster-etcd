package org.opensearch.supervisor.model.deploy;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/** What happens to a drain's nodes afterwards ({@code DESIGN.md} §29.3), which decides what moves off them. */
public enum DrainIntent implements WireEnum {

    /** The nodes come back. Primaries move off; replicas stay, keep counting, and are not replaced before {@code until}. */
    RETURN,

    /** The nodes are being decommissioned. Every copy moves off, built elsewhere before it is dropped. */
    REMOVE,

    /** A value this build does not recognise. Read-only; never written back. */
    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name().toLowerCase(Locale.ROOT);
    }

    public static DrainIntent from(String value) {
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
