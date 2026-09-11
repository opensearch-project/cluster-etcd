package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardInventory;

/**
 * A copy weighs what it occupies.
 *
 * <p>The signal OpenSearch's balancer does not have, and the one available for free: the node contract already
 * carries {@code size_bytes} per copy, so nothing new has to be published for this to work.
 *
 * <p>A copy that has not reported a size yet weighs zero rather than being guessed at. A recovering copy has no
 * meaningful size — it is still arriving — and inventing one would let a node look loaded by data it does not
 * hold, which is the wrong direction: it would discourage moves onto a node that has room.
 */
public final class ShardSizeSignal implements BalanceSignal {

    public static final String NAME = "shard_size";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public double weightOf(ShardInventory.ShardCopy copy, IndexConfig index) {
        Long bytes = copy.sizeBytes();
        return bytes == null || bytes < 0 ? 0.0 : bytes.doubleValue();
    }
}
