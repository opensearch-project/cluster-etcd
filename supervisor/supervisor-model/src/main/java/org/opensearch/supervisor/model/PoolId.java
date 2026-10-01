package org.opensearch.supervisor.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A node pool identifier.
 *
 * <p>The pool is the unit of ownership: a set of nodes together with the tenants bound to it, owned by
 * exactly one supervisor at a time. See {@code DESIGN.md} §2 — every other single-writer property in the
 * system falls out of that.
 */
public record PoolId(@JsonValue String value) implements Comparable<PoolId> {

    private static final Pattern VALID = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_.\\-]{0,63}");

    public PoolId {
        Objects.requireNonNull(value, "pool id must not be null");
        if (VALID.matcher(value).matches() == false) {
            throw new IllegalArgumentException(
                "invalid pool id [" + value + "]: must be 1-64 characters of [A-Za-z0-9_.-], "
                    + "starting alphanumeric, and must not contain '/'"
            );
        }
    }

    @JsonCreator
    public static PoolId of(String value) {
        return new PoolId(value);
    }

    @Override
    public int compareTo(PoolId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
