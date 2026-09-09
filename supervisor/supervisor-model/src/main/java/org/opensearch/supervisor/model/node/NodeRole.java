package org.opensearch.supervisor.model.node;

import java.util.Locale;

/**
 * What a node may be given. {@code NODE_CONTRACT.md}: {@code data} takes shard copies, {@code coordinator}
 * takes routing and no copies, and a node may have both.
 */
public enum NodeRole implements WireEnum {

    DATA,
    COORDINATOR,

    /**
     * A role this build does not recognise, from a node newer than this supervisor.
     *
     * <p>Read-only. Failing the parse instead would lose the node's entire registration over one unknown
     * string, and a node with an unreadable registration is a node that gets no shards — a rolling upgrade
     * would drain the cluster one node at a time. Tolerating it costs nothing: an unknown role grants no
     * capability, so the node is simply treated as having only the roles that were understood.
     */
    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name().toLowerCase(Locale.ROOT);
    }

    public static NodeRole from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "data" -> DATA;
            case "coordinator" -> COORDINATOR;
            default -> UNKNOWN;
        };
    }
}
