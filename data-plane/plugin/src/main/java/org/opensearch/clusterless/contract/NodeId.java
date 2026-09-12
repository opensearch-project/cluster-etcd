/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A node's <em>persistent</em> identity — the one that survives a restart, not its name and not its
 * ephemeral id.
 *
 * <p>{@code NODE_CONTRACT.md} requires {@code [A-Za-z0-9_-]{1,64}}, which an OpenSearch node id already
 * satisfies. The validation is kept anyway, for two reasons that are not the same reason: keys are built
 * by concatenation, so an id containing {@code '/'} would escape its subtree and land under another
 * node's; and D8.4 says two processes must never share a node id, which a derived id can only guarantee
 * if the thing it is derived from cannot collide.
 */
public record NodeId(String value) implements Comparable<NodeId> {

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9_\\-]{1,64}");

    public NodeId {
        Objects.requireNonNull(value, "node id must not be null");
        if (VALID.matcher(value).matches() == false) {
            throw new IllegalArgumentException(
                "invalid node id [" + value + "]: must be 1-64 characters of [A-Za-z0-9_-] and must not contain '/'"
            );
        }
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public int compareTo(NodeId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
