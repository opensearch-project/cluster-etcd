package org.opensearch.supervisor.model;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A tenant identifier.
 *
 * <p>The pattern is copied deliberately from the control plane's {@code TenantId}, character for
 * character. The two services key the same store, so a tenant the control plane can serve and this one
 * cannot represent is a tenant that is silently unmanaged — which is exactly the failure
 * {@code SUPERVISOR_KEYS.md} §6 exists to make impossible. If the control plane widens its pattern, this
 * one has to widen with it.
 */
public record TenantId(String value) implements Comparable<TenantId> {

    private static final Pattern VALID = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_.\\-]{0,127}");

    public TenantId {
        Objects.requireNonNull(value, "tenant id must not be null");
        if (VALID.matcher(value).matches() == false) {
            throw new IllegalArgumentException(
                "invalid tenant id [" + value + "]: must be 1-128 characters of [A-Za-z0-9_.-], "
                    + "starting alphanumeric, and must not contain '/'"
            );
        }
    }

    public static TenantId of(String value) {
        return new TenantId(value);
    }

    @Override
    public int compareTo(TenantId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
