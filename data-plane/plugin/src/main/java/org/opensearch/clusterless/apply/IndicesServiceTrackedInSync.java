/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.Set;
import java.util.function.Supplier;

/**
 * {@link TrackedInSync} from the shard itself.
 *
 * <p>Asked only of a shard in primary mode, because {@code IndexShard.getReplicationGroup} asserts it: a replica has no
 * replication group, and neither does a primary that is still recovering or has not been activated. Null in every such
 * case, and when the shard closes underneath, which is an unknown set rather than an empty one.
 */
public final class IndicesServiceTrackedInSync implements TrackedInSync {

    private static final Logger logger = LogManager.getLogger(IndicesServiceTrackedInSync.class);

    private final Supplier<IndicesService> indicesService;

    public IndicesServiceTrackedInSync(Supplier<IndicesService> indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    public Set<String> of(ShardId shardId) {
        IndicesService indices = indicesService.get();
        if (indices == null) {
            return null;
        }
        try {
            IndexService indexService = indices.indexService(shardId.getIndex());
            return of(indexService == null ? null : indexService.getShardOrNull(shardId.id()));
        } catch (Exception e) {
            logger.debug("clusterless: could not read the in-sync set of {}", shardId, e);
            return null;
        }
    }

    /** The reading itself, from a shard in hand. */
    public static Set<String> of(IndexShard shard) {
        if (shard == null || shard.isPrimaryMode() == false) {
            return null;
        }
        return Set.copyOf(shard.getReplicationGroup().getInSyncAllocationIds());
    }
}
