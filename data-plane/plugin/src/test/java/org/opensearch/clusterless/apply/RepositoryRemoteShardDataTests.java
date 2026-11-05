/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;
import org.junit.AfterClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The repository probe against a real {@code fs} repository, under a shard whose primary is uploading to it.
 *
 * <p>What matters here is that asking leaves the repository as it found it. The probe used to close the
 * {@code RemoteSegmentStoreDirectory} it built, and {@code close()} purges: {@code deleteStaleSegmentsAsync(0)} deletes
 * every metadata file without a lock, the newest included, and then {@code deleteIfEmpty()} deletes the shard's whole
 * remote directory if nothing locked is left. Nothing here takes locks, so that was everything. Found by the E2d spike,
 * whose own test is the same probe with one locked upload surviving.
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // ExtrasFS plants files in the fs repository's directories
public class RepositoryRemoteShardDataTests extends OpenSearchSingleNodeTestCase {

    private static final String REPOSITORY = "segments";
    private static Path repo;

    @AfterClass
    public static void clearRepository() {
        repo = null;
    }

    @Override
    protected Settings nodeSettings() {
        if (repo == null) {
            repo = createTempDir();
        }
        return Settings.builder()
            .put(super.nodeSettings())
            // A segments_only remote-store node, as -Pclusterless.segments_only=true starts one.
            .put("node.attr.remote_store.segment.repository", REPOSITORY)
            .put("node.attr.remote_store.mode", "segments_only")
            .put("node.attr.remote_store.repository." + REPOSITORY + ".type", "fs")
            .put("node.attr.remote_store.repository." + REPOSITORY + ".settings.location", repo.toString())
            .put("path.repo", repo.toString())
            // The data plane stamps no remote-store custom data, so every node uses the FIXED layout, as the probe does.
            .put("cluster.remote_store.index.path.type", "fixed")
            .build();
    }

    public void testProbingLeavesEveryUploadInPlace() throws Exception {
        String name = "uploading";
        client().admin()
            .indices()
            .prepareCreate(name)
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.replication.type", "SEGMENT")
                    .put("index.refresh_interval", "-1")
            )
            .get();
        ensureGreen(name);
        Index index = resolveIndex(name);
        for (int round = 0; round < 3; round++) {
            client().prepareIndex(name).setId("doc-" + round).setSource("n", round).get();
            client().admin().indices().prepareRefresh(name).get();
        }
        assertBusy(() -> assertTrue(metadataFiles(index).toString(), metadataFiles(index).size() >= 3), 30, TimeUnit.SECONDS);
        Set<String> metadataBefore = metadataFiles(index);
        Set<String> dataBefore = dataFiles(index);

        ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        RemoteShardData.Answer answer = new RepositoryRemoteShardData(
            () -> getInstanceFromNode(RepositoriesService.class),
            threadPool,
            Settings.EMPTY
        ).probe(new ShardId(index, 0), REPOSITORY);
        assertEquals(RemoteShardData.Answer.HAS_DATA, answer);

        // A purge would have been handed to REMOTE_PURGE before probe returned, so once that pool is idle it has run.
        assertBusy(() -> assertTrue("REMOTE_PURGE is idle", idle(threadPool, ThreadPool.Names.REMOTE_PURGE)), 30, TimeUnit.SECONDS);
        Set<String> metadataAfter = metadataFiles(index);
        Set<String> dataAfter = dataFiles(index);
        assertTrue("every metadata file is still there: " + metadataBefore + " -> " + metadataAfter, metadataAfter.containsAll(metadataBefore));
        assertTrue("every data file is still there: " + dataBefore.size() + " -> " + dataAfter.size(), dataAfter.containsAll(dataBefore));
    }

    private static boolean idle(ThreadPool threadPool, String name) {
        for (ThreadPoolStats.Stats stats : threadPool.stats()) {
            if (stats.getName().equals(name)) {
                return stats.getActive() == 0 && stats.getQueue() == 0;
            }
        }
        return true;
    }

    private static Set<String> metadataFiles(Index index) throws IOException {
        return list(segments(index).resolve("metadata"));
    }

    private static Set<String> dataFiles(Index index) throws IOException {
        return list(segments(index).resolve("data"));
    }

    /** Shard 0's segments under the FIXED layout. */
    private static Path segments(Index index) {
        return repo.resolve(index.getUUID()).resolve("0").resolve("segments");
    }

    private static Set<String> list(Path directory) throws IOException {
        if (Files.isDirectory(directory) == false) {
            return Set.of();
        }
        try (Stream<Path> files = Files.list(directory)) {
            return files.map(file -> file.getFileName().toString()).collect(Collectors.toCollection(TreeSet::new));
        }
    }
}
