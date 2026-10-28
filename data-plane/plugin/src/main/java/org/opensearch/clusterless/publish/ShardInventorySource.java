/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

import org.opensearch.clusterless.contract.ShardInventory;

import java.util.List;

/**
 * What this node actually holds.
 *
 * <p>An interface rather than a method on the publisher because of what depends on it being <em>the
 * truth</em>. The supervisor writes assignments but does not know what any node holds; it finds out by
 * reading what the node published. So the inventory has to say what is on disk even when that disagrees
 * with the assignment — and a seam here is what lets that be tested against states a real node is hard to
 * hold in, such as holding a copy nobody asked for.
 */
@FunctionalInterface
public interface ShardInventorySource {

    /**
     * Every copy on this node, in any order.
     *
     * <p>Must include copies that are <b>not yet started</b>, with their state. A projection filtered to
     * started copies would deadlock document-replication recovery: a replica placed in the same pass as
     * its primary reads the routing projection before the primary is serving, and if the primary is not
     * listed it can never see the primary it is waiting for (D3.8, D5.3).
     *
     * @throws RuntimeException if the node's own state cannot be read. The publisher treats that as a
     *     reason to leave the last inventory in place, never as a reason to publish an empty one — absent
     *     copies read as <em>gone</em>, and the supervisor would start rebuilding shards that exist.
     */
    List<ShardInventory.HeldCopy> heldCopies();

    /** A node that holds nothing: a coordinator, or a data node before its first assignment. */
    ShardInventorySource EMPTY = List::of;
}
