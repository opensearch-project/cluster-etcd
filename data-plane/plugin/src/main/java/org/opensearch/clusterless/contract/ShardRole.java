/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.cluster.routing.ShardRouting;

import java.util.Locale;

/**
 * What a shard copy <em>is</em>, as distinct from how it is getting on — see {@link ShardState}.
 *
 * <p>Role and state are separate dimensions. A copy can be a primary that is still initializing, and
 * health cannot be computed without being able to say so: {@code active_primary_shards} counts copies that
 * are both {@code PRIMARY} and {@code STARTED}.
 */
public enum ShardRole {

    /** Takes writes. Exactly one per shard. */
    PRIMARY,

    /** A document-replication replica: takes replicated writes and serves reads. */
    REPLICA,

    /** A segment-replication search replica: serves reads, never takes writes. */
    SEARCH_REPLICA,

    /**
     * A role this build does not recognise, from a supervisor newer than this node.
     *
     * <p><b>Read-only, and never treated as serving or as absent.</b> A rolling upgrade makes one side newer
     * than the other routinely, so an unrecognised value is the normal case rather than the exceptional one.
     * Failing the whole parse instead would lose every other shard in the assignment over one string, and an
     * assignment that fails to parse is one this node cannot converge to at all.
     *
     * <p>A copy this node cannot classify is one it must not create: it has no idea whether the supervisor
     * meant something that takes writes. So the shard is left out of the desired state and reported, which
     * is a finding for a human rather than a guess.
     */
    UNKNOWN;

    /**
     * @throws IllegalStateException if this is the sentinel. Writing it would fabricate a document claiming a
     *     value no supervisor ever sent.
     */
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

    /**
     * The role OpenSearch's routing table says a copy has.
     *
     * <p>The order matters: a search-only replica is not a primary, but {@code primary()} and
     * {@code isSearchOnly()} are independent flags on {@link ShardRouting} rather than one enum, so
     * testing for the primary first would report a search replica as a primary if both were ever set.
     *
     * <p>Never returns {@link #UNKNOWN}: this reads the server's own state, where there is no such thing.
     */
    public static ShardRole of(ShardRouting routing) {
        if (routing.isSearchOnly()) {
            return SEARCH_REPLICA;
        }
        return routing.primary() ? PRIMARY : REPLICA;
    }
}
