/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Asks a repository whether it holds segments for a shard, using the server's own directory.
 *
 * <p>Built through {@link RemoteSegmentStoreDirectoryFactory} rather than by listing blobs, because the path a
 * shard's segments live at is derived from several inputs and reproducing that derivation here would be the same
 * class of bug as two nodes disagreeing about it — with the worst possible failure, since a probe looking in the
 * wrong place finds nothing and "nothing" is the answer that discards data. Going through the factory means the
 * path is computed by the code that computes it for the shard itself.
 *
 * <p>Two of those inputs still have to be supplied, and both are pinned deliberately:
 *
 * <ul>
 *   <li><b>The path strategy is {@code FIXED}</b>, because {@link DataNodeState} stamps no remote-store custom
 *       data and {@code RemoteStoreUtils.determineRemoteStorePathStrategy} falls back to {@code FIXED} without
 *       it. That coupling is invisible from either side alone, so a test asserts it: if the fabrication ever
 *       stamps custom data, this probe starts looking somewhere else and the test is what says so.
 *   <li><b>The fixed prefix comes from the same cluster setting the server reads</b>
 *       ({@code cluster.remote_store.segments.path.prefix}), for the same reason.
 * </ul>
 *
 * <p>{@code init()} returns the latest metadata, or null when nothing has ever been uploaded — which is exactly
 * the question. Anything thrown is {@link Answer#UNKNOWN}: unreachable is not empty, and this class never
 * pretends it is.
 */
public final class RepositoryRemoteShardData implements RemoteShardData {

    private static final Logger logger = LogManager.getLogger(RepositoryRemoteShardData.class);

    private final Supplier<RepositoriesService> repositories;
    private final ThreadPool threadPool;
    private final String segmentsPathFixedPrefix;

    public RepositoryRemoteShardData(
        Supplier<RepositoriesService> repositories,
        ThreadPool threadPool,
        org.opensearch.common.settings.Settings settings
    ) {
        this.repositories = Objects.requireNonNull(repositories, "repositories");
        this.threadPool = Objects.requireNonNull(threadPool, "threadPool");
        this.segmentsPathFixedPrefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(settings);
    }

    @Override
    public Answer probe(ShardId shardId, String repository) {
        if (repositories.get() == null) {
            // Before Guice has finished, which is ordinary rather than exceptional and is not an empty repository.
            return Answer.UNKNOWN;
        }
        // Privileged for the reason every store call is: the check walks the stack and finds whichever jar happens
        // to be above this one. See PrivilegedKeyValueStore.
        return AccessController.doPrivileged(() -> {
            RemoteSegmentStoreDirectoryFactory factory = new RemoteSegmentStoreDirectoryFactory(
                repositories,
                threadPool,
                segmentsPathFixedPrefix
            );
            try (
                RemoteSegmentStoreDirectory directory = (RemoteSegmentStoreDirectory) factory.newDirectory(
                    repository,
                    shardId.getIndex().getUUID(),
                    shardId,
                    new RemoteStorePathStrategy(RemoteStoreEnums.PathType.FIXED)
                )
            ) {
                boolean hasData = directory.init() != null;
                logger.debug("clusterless: [{}] {} data for {}", repository, hasData ? "holds" : "holds no", shardId);
                return hasData ? Answer.HAS_DATA : Answer.EMPTY;
            } catch (Exception e) {
                // Deliberately not EMPTY. A repository that cannot be read says nothing about whether a shard was
                // ever uploaded, and the caller treats not knowing as a reason to leave the shard alone.
                logger.warn("clusterless: could not read [{}] to find out whether it holds {}", repository, shardId, e);
                return Answer.UNKNOWN;
            }
        });
    }
}
