/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A tenant identifier.
 *
 * <p>The pattern is copied character for character from the control plane's and the supervisor's, because
 * all three key the same store: a tenant the control plane can serve and this plugin cannot represent is a
 * tenant whose shards silently never appear in an inventory. If one widens, all three widen.
 */
public record TenantId(String value) implements Comparable<TenantId> {

    private static final Pattern VALID = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_.\\-]{0,127}");

    public TenantId {
        Objects.requireNonNull(value, "tenant id must not be null");
        if (VALID.matcher(value).matches() == false) {
            throw new IllegalArgumentException(
                "invalid tenant id ["
                    + value
                    + "]: must be 1-128 characters of [A-Za-z0-9_.-], starting alphanumeric, and must not contain '/'"
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
