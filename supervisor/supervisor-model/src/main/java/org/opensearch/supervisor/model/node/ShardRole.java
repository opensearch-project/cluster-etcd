package org.opensearch.supervisor.model.node;

import java.util.Locale;

/**
 * What a shard copy <em>is</em>, as distinct from how it is getting on — see {@link ShardState}.
 *
 * <p>Role and state are separate dimensions, which the first version of this contract got wrong by
 * collapsing them into one enum. A copy can be a primary that is still initializing, and health cannot be
 * computed without being able to say so: {@code active_primary_shards} counts copies that are both
 * {@code PRIMARY} and {@code STARTED}, and an index whose primary exists but is initializing is red, not
 * green. With one enum there was no way to express it.
 *
 * <p>The three roles are OpenSearch's own. {@code SEARCH_REPLICA} was missing from the first version
 * entirely.
 */
public enum ShardRole implements WireEnum {

    /** Takes writes. Exactly one per shard. */
    PRIMARY,

    /** A document-replication replica: takes replicated writes and serves reads. */
    REPLICA,

    /** A segment-replication search replica: serves reads, never takes writes. */
    SEARCH_REPLICA,

    /** A role this build does not recognise. Read-only; never counted as serving or as absent. */
    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name();
    }

    public static ShardRole from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        return switch (value.toUpperCase(Locale.ROOT)) {
            case "PRIMARY" -> PRIMARY;
            case "REPLICA" -> REPLICA;
            case "SEARCH_REPLICA" -> SEARCH_REPLICA;
            default -> UNKNOWN;
        };
    }
}
