/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A node pool identifier.
 *
 * <p>The pool is the unit of ownership, and the reason {@code nodes/assignment/<node-id>} has exactly one
 * writer without a lock: one supervisor owns a pool at a time. A node declares its pool from its own
 * deployment configuration and {@code pools/nodes/<node-id>} overrides it — which is operator-owned
 * precisely because a node must not be able to move itself into someone else's pool.
 */
public record PoolId(String value) implements Comparable<PoolId> {

    private static final Pattern VALID = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_.\\-]{0,63}");

    public PoolId {
        Objects.requireNonNull(value, "pool id must not be null");
        if (VALID.matcher(value).matches() == false) {
            throw new IllegalArgumentException(
                "invalid pool id ["
                    + value
                    + "]: must be 1-64 characters of [A-Za-z0-9_.-], starting alphanumeric, and must not contain '/'"
            );
        }
    }

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
