/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.core.index.shard.ShardId;

/**
 * Whether this node already has a copy of a shard on disk.
 *
 * <p>This exists because of a gap in what the contract can tell a node, and the gap is deliberate on the
 * contract's side. {@code NODE_CONTRACT.md} leaves allocation ids out of the shard inventory — measured at 37%
 * of the document's size budget for a field the supervisor has no use for — and explicitly warns against
 * {@code cluster-etcd}'s habit of reading them back out of the store, since "allocation ids are on the node's
 * local disk; a store round-trip makes them lossable in a way they otherwise would not be".
 *
 * <p>Both of those are right, and together they mean <b>the store cannot tell a restarting node whether it used
 * to hold a shard.</b> Nothing in an assignment distinguishes "create this copy" from "you already have this
 * copy, resume it". Only the disk knows, which is what D2.5 is really asking for when it says a node must
 * converge from an empty disk and from a returning one "without special-casing either" — the answer is not to
 * special-case, it is to look.
 *
 * <p>The consequence if this answers wrongly is asymmetric, which is why it is a seam with a conservative
 * default rather than an inline check. Answering "no data" when there is some means recovering a shard from
 * empty and discarding what was on disk. Answering "there is data" when there is none means an existing-store
 * recovery that finds nothing and fails the shard. Nothing is lost, but it is less graceful than it sounds. The
 * failed copy is rebuilt on the next pass (see {@code DataNodeState.routingFor}), and this answer is still wrong
 * then, so it fails again every pass. In the inventory it alternates between initializing and absent, and never
 * starts. The server's reason does not reach {@code applied}.
 */
@FunctionalInterface
public interface LocalShardStore {

    boolean hasShardData(ShardId shardId);

    /** A node with nothing on disk. What a genuinely new node's answer is, and what a test usually wants. */
    LocalShardStore EMPTY = shardId -> false;
}
