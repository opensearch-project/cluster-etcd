/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.core.index.shard.ShardId;

/**
 * Whether the remote store already holds segments for a shard.
 *
 * <p>The question a node cannot answer from its own disk and must not guess at. When a primary is about to be
 * created and this node has no local copy of it, exactly two situations are possible and they want opposite
 * treatment: the index is new and the shard should start empty, or a copy of this shard existed and its data is
 * in the repository. Getting it wrong in one direction discards a shard; in the other it fails a recovery that
 * can never succeed. {@link RemoteRecovery} has the table.
 *
 * <p>An interface for the reason {@link LocalShardStore} is one: the implementation talks to a repository, and
 * every test about the <em>decision</em> should be able to state the answer rather than arrange for it.
 */
public interface RemoteShardData {

    /** What the repository says, including that it did not say. */
    enum Answer {
        /** Segment metadata exists for this shard: a copy of it has been uploaded before. */
        HAS_DATA,
        /** The repository is reachable and holds nothing for this shard. */
        EMPTY,
        /**
         * The question could not be answered — unreachable repository, credentials, a path that will not
         * resolve. Deliberately distinct from {@link #EMPTY}, because they are the same bytes on the wire and
         * opposite facts: treating one as the other is how a network blip becomes data loss.
         */
        UNKNOWN
    }

    /**
     * @param shardId the shard, whose index uuid decides the remote path
     * @param repository the repository named in the index's settings
     */
    Answer probe(ShardId shardId, String repository);

    /** For a node with no remote store, where the question should never be asked. */
    RemoteShardData UNAVAILABLE = (shardId, repository) -> Answer.UNKNOWN;
}
