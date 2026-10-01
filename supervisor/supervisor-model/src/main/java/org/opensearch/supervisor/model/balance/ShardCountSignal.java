package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardInventory;

/**
 * Every copy weighs one.
 *
 * <p>What OpenSearch's balancer uses, and on its own it is the weakness worth naming: a node holding a hundred
 * one-megabyte shards and a node holding one hundred-gigabyte shard look identically loaded, so a balancer
 * that only counts will happily move the small ones around while the real imbalance sits untouched.
 *
 * <p>Kept, because it is not useless. Shard count is the right proxy for the costs that are not bytes — file
 * handles, heap for segment metadata, a slice of every refresh and merge cycle — which is exactly the ground
 * {@code ShardsLimitDecider} covers as a hard limit and this covers as a gradient.
 */
public final class ShardCountSignal implements BalanceSignal {

    public static final String NAME = "shard_count";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public double weightOf(ShardInventory.ShardCopy copy, IndexConfig index) {
        return 1.0;
    }
}
