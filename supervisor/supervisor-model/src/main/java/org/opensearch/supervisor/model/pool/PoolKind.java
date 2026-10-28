package org.opensearch.supervisor.model.pool;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * Whether a pool serves one tenant or many. Documentation, and a guard: binding a second tenant to a
 * {@code DEDICATED} pool is refused rather than silently making it shared.
 */
public enum PoolKind implements WireEnum {

    /** One large tenant, on nodes nothing else uses. */
    DEDICATED,

    /** The long tail of small tenants, on a common set of nodes. */
    SHARED,

    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name().toLowerCase(Locale.ROOT);
    }

    public static PoolKind from(String value) {
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
