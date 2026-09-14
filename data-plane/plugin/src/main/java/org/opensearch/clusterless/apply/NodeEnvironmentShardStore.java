/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Whether a shard's data is on this node's disks, asked of the node environment.
 *
 * <p>{@code availableShardPaths} is where the server itself looks, and the directory is keyed by index
 * <b>uuid</b> — which is the reason this works at all. The fabricated index name could change between versions
 * of this plugin without losing a byte, but the uuid comes from the control plane and does not, so a returning
 * node finds its own data under the same path a previous process wrote it to.
 *
 * <p>Existence of the directory is the test, not validity of what is in it. A directory with a corrupt or
 * partial store still means "do not recover this from empty": an existing-store recovery that fails is a
 * reported shard failure an operator can act on, and an empty-store recovery that succeeds is silent data loss.
 * Deciding which of those to risk is the whole job of this class, and it is not a close call.
 */
public final class NodeEnvironmentShardStore implements LocalShardStore {

    private static final Logger logger = LogManager.getLogger(NodeEnvironmentShardStore.class);

    private final NodeEnvironment nodeEnvironment;

    public NodeEnvironmentShardStore(NodeEnvironment nodeEnvironment) {
        this.nodeEnvironment = Objects.requireNonNull(nodeEnvironment, "nodeEnvironment");
    }

    @Override
    public boolean hasShardData(ShardId shardId) {
        try {
            for (Path path : nodeEnvironment.availableShardPaths(shardId)) {
                if (Files.exists(path)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            // Answering "there is data" on an unreadable disk is the conservative direction: it risks a failed
            // recovery, where the other answer risks discarding a shard. A node whose data paths cannot be
            // listed has a problem either way, and this one is visible.
            logger.warn("clusterless: could not check the local paths for {}; assuming it has data", shardId, e);
            return true;
        }
    }
}
