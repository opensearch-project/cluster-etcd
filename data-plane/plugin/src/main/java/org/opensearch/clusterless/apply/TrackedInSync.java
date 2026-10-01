/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.core.index.shard.ShardId;

import java.util.Set;

/**
 * The copies a primary on this node holds in sync, read from its own {@code ReplicationTracker}.
 *
 * <p>The only source of that set there is. The primary marks a copy in sync when that copy's recovery finishes
 * ({@code RecoverySourceHandler}, {@code markAllocationIdAsInSync}), and no document records it. Under a cluster manager
 * the set reaches the cluster state when the copy is marked started; here nothing carries it, so it is published from
 * this source (finding 12) and fabricated from it. It cannot be guessed instead:
 * {@code ReplicationTracker.updateFromClusterManager} asserts that every in-sync id handed to a primary is already in
 * sync in its tracker.
 */
@FunctionalInterface
public interface TrackedInSync {

    /** The in-sync ids of this node's primary of the shard, itself included, or null if there is none in primary mode. */
    Set<String> of(ShardId shardId);

    /** A node that cannot tell, which is what a test usually wants and what a node with no indices service has. */
    TrackedInSync UNKNOWN = shardId -> null;
}
