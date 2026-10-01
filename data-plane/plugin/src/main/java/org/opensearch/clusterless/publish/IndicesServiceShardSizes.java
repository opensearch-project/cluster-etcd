/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.function.Supplier;

/**
 * Shard sizes, from the shards themselves.
 *
 * <p>{@code size_bytes} is the most load-bearing field in the inventory and more so than it first looks:
 * it is what a disk-aware decider needs, and it is the whole of the balancer's size signal — the thing
 * OpenSearch's count-only balancer cannot see. A pool where every shard reports roughly the same size is a
 * pool that can only be balanced by count, so publishing it accurately is worth real effort here.
 *
 * <p>Everything this returns is best effort, and {@code null} is a legitimate answer: a copy whose store
 * cannot be read right now has an <em>unknown</em> size, which the supervisor fills from the largest copy
 * of the same shard elsewhere in the pool. Guessing zero instead would tell it the copy costs nothing.
 */
public final class IndicesServiceShardSizes implements ClusterStateInventorySource.ShardSizes {

    private static final Logger logger = LogManager.getLogger(IndicesServiceShardSizes.class);

    private final Supplier<IndicesService> indicesService;

    public IndicesServiceShardSizes(Supplier<IndicesService> indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    public Long sizeOf(ShardId shardId) {
        IndicesService indices = indicesService.get();
        if (indices == null) {
            return null;
        }
        try {
            IndexService indexService = indices.indexService(shardId.getIndex());
            if (indexService == null) {
                return null;
            }
            IndexShard shard = indexService.getShardOrNull(shardId.id());
            if (shard == null) {
                return null;
            }
            return shard.storeStats().sizeInBytes();
        } catch (Exception e) {
            // Reading a store's size touches the filesystem and races with a shard closing. Neither is
            // worth a failed inventory; both are worth an unknown size.
            logger.debug("clusterless: could not measure {}", shardId, e);
            return null;
        }
    }
}
