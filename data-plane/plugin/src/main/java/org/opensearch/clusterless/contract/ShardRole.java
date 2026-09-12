/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.cluster.routing.ShardRouting;

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
    SEARCH_REPLICA;

    public String wireName() {
        return name();
    }

    /**
     * The role OpenSearch's routing table says a copy has.
     *
     * <p>The order matters: a search-only replica is not a primary, but {@code primary()} and
     * {@code isSearchOnly()} are independent flags on {@link ShardRouting} rather than one enum, so
     * testing for the primary first would report a search replica as a primary if both were ever set.
     */
    public static ShardRole of(ShardRouting routing) {
        if (routing.isSearchOnly()) {
            return SEARCH_REPLICA;
        }
        return routing.primary() ? PRIMARY : REPLICA;
    }
}
