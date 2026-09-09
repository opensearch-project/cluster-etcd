package org.opensearch.supervisor.model;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A node's <em>persistent</em> identity — the one that survives a restart, not its name and not its
 * ephemeral id.
 *
 * <p>{@code KEY_LAYOUT.md} is explicit that names are reused, and bookkeeping keyed on a name will
 * confuse a node or an index with its deleted predecessor. The pattern matches what an OpenSearch node id
 * already is.
 */
public record NodeId(String value) implements Comparable<NodeId> {

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9_\\-]{1,64}");

    public NodeId {
        Objects.requireNonNull(value, "node id must not be null");
        if (VALID.matcher(value).matches() == false) {
            throw new IllegalArgumentException(
                "invalid node id [" + value + "]: must be 1-64 characters of [A-Za-z0-9_-] "
                    + "and must not contain '/'"
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
