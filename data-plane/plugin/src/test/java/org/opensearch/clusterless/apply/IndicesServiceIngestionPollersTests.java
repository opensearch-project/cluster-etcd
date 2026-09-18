/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.clusterless.contract.IngestionReset;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link IndicesServiceIngestionPollers} against a real started primary with a real {@code IngestionEngine}.
 *
 * <p><b>The one part of the reset path that had no test</b>, for a reason that turned out to be wrong: every
 * "what is not proven" note about this seam assumed a node could not be started in this sandbox, which was true of
 * {@code tools/jbuild.sh} and is not true of Gradle. Everything above the seam is a pure function and tested as
 * one in {@code IngestionResetsTests}; this is the part that touches a poller, and it is where the two obligations
 * that cost a real debugging session live:
 *
 * <ul>
 *   <li><b>pause before resetting.</b> {@code IngestionEngine.resetStreamPoller} throws
 *       {@code IllegalStateException("Cannot reset consumer when poller is not paused")}, and a reset normally
 *       arrives at a node whose poller is already running — the resume that accompanies it is desired state and
 *       travels faster. {@link #testAResetOnARunningPollerSucceedsAndLandsRunning} is that case.
 *   <li><b>land on the state the document asked for</b>, not on whatever the poller was doing.
 * </ul>
 *
 * <p>Observing a reset at all needs the trick a real operator found: because every ingested message carries its own
 * {@code _id}, re-consuming a range rewrites the same documents with the same content, so a working reset and a
 * reset that did nothing leave an identical index with an identical count. {@link ListIngestionSource#rewrite}
 * changes the payload behind a stable id, which is the only thing that makes the rewind visible to a search.
 */
public class IndicesServiceIngestionPollersTests extends OpenSearchSingleNodeTestCase {

    private static final String INDEX = "pull-based";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(ListIngestionSource.TestPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ListIngestionSource.reset();
    }

    public void testAResetOnARunningPollerSucceedsAndLandsRunning() throws Exception {
        ListIngestionSource.publish("1", "foo");
        ListIngestionSource.publish("2", "bar");
        ListIngestionSource.publish("3", "baz");
        createPullBasedIndex();
        awaitTitles(Map.of("1", "foo", "2", "bar", "3", "baz"));

        // The payloads change behind the ids, so a rewind is visible and a no-op is not mistaken for one.
        ListIngestionSource.rewrite(0, "1", "foo modified");
        ListIngestionSource.rewrite(1, "2", "bar modified");
        ListIngestionSource.rewrite(2, "3", "baz modified");

        // The poller is running: this is the ordinary case, and the one a node that trusted the ordering fails.
        // landPaused false, because that is what a resume's index document says.
        pollers().pauseThenReset(shard(), IngestionReset.Mode.OFFSET, "2", false);

        // Offsets are 0-based, so offset 2 is the third message: only it is re-consumed. The first two keep the
        // content they were indexed with, which is what proves the pointer landed where it was sent rather than
        // at the earliest pointer -- and that the poller was left running, since nothing would be re-consumed at
        // all if the reset had left it paused.
        awaitTitles(Map.of("1", "foo", "2", "bar", "3", "baz modified"));
    }

    public void testAResetThatLandsPausedStopsConsumingAfterwards() throws Exception {
        ListIngestionSource.publish("1", "foo");
        ListIngestionSource.publish("2", "bar");
        createPullBasedIndex();
        awaitTitles(Map.of("1", "foo", "2", "bar"));

        pollers().pauseThenReset(shard(), IngestionReset.Mode.OFFSET, "1", true);

        // Asserted on what arrives *after* the reset, not on what the reset itself consumed, and the distinction
        // is upstream's rather than a convenience. resetStreamPoller ends with streamPoller.start(), and
        // updateIngestionSettings applies the pause flag only after the reset has run -- so a reset always brings
        // the consumer up running for a moment, and documents from the new pointer can land even when the reset is
        // landing paused. An earlier version of this test asserted that nothing was consumed at all and failed for
        // exactly that reason. "Landed paused" means the poller is stopped once the dust settles, which is what
        // this checks: a message published afterwards must not arrive.
        ListIngestionSource.publish("3", "published after the reset");

        assertFalse(
            "a poller left paused must not consume anything published afterwards",
            titlesReach(Map.of("1", "foo", "2", "bar", "3", "published after the reset"), 6)
        );
        assertNull("the document published after the reset must not be there", titles().get("3"));
    }

    public void testReadinessRefusesAnIndexWithNoIngestionSource() throws Exception {
        createIndex("ordinary", Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build());
        ensureGreen("ordinary");
        ShardId ordinary = new ShardId(resolveIndex("ordinary"), 0);

        assertEquals(IngestionResets.Pollers.Readiness.NOT_INGESTING, pollers().readinessOf(ordinary));
    }

    public void testReadinessIsReadyForAStartedPullBasedPrimary() throws Exception {
        ListIngestionSource.publish("1", "foo");
        createPullBasedIndex();
        awaitTitles(Map.of("1", "foo"));

        assertEquals(IngestionResets.Pollers.Readiness.READY, pollers().readinessOf(shard()));
    }

    public void testReadinessIsNotYetForAShardThisNodeDoesNotHold() {
        ShardId absent = new ShardId("nowhere", "_na_", 0);
        assertEquals(IngestionResets.Pollers.Readiness.NOT_YET, pollers().readinessOf(absent));
    }

    // ---------------------------------------------------------------- fixtures

    private IndicesServiceIngestionPollers pollers() {
        return new IndicesServiceIngestionPollers(() -> getInstanceFromNode(org.opensearch.indices.IndicesService.class));
    }

    private ShardId shard() {
        return new ShardId(resolveIndex(INDEX), 0);
    }

    private void createPullBasedIndex() throws Exception {
        createIndex(
            INDEX,
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                // Segment replication is not decoration: IndexMetadata refuses a pull-based index with document
                // replication unless index.ingestion_source.all_active is on, because an IngestionEngine keeps no
                // translog and so has nothing for a docrep replica to follow. Which is the same reason
                // node.attr.remote_store.mode=segments_only exists.
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put(IndexMetadata.SETTING_INGESTION_SOURCE_TYPE, "LIST")
                .put(IndexMetadata.SETTING_INGESTION_SOURCE_POINTER_INIT_RESET, "earliest")
                .build()
        );
        ensureGreen(INDEX);
    }

    /** Every document's title, by id. */
    private Map<String, String> titles() {
        client().admin().indices().prepareRefresh(INDEX).get();
        SearchResponse response = client().prepareSearch(INDEX).setQuery(QueryBuilders.matchAllQuery()).setSize(100).get();
        Map<String, String> byId = new java.util.HashMap<>();
        for (SearchHit hit : response.getHits().getHits()) {
            byId.put(hit.getId(), String.valueOf(hit.getSourceAsMap().get("title")));
        }
        return byId;
    }

    private void awaitTitles(Map<String, String> expected) throws Exception {
        assertTrue("never reached " + expected + "; last saw " + titles(), titlesReach(expected, 20));
    }

    /** Whether the index reaches exactly this set of id → title, within {@code attempts} half-seconds. */
    private boolean titlesReach(Map<String, String> expected, int attempts) throws Exception {
        List<Map<String, String>> seen = new ArrayList<>();
        for (int attempt = 0; attempt < attempts; attempt++) {
            Map<String, String> actual = titles();
            if (expected.equals(actual)) {
                return true;
            }
            seen.add(actual);
            TimeUnit.MILLISECONDS.sleep(500);
        }
        logger.info("clusterless test: never saw {}, saw {}", expected, seen);
        return false;
    }
}
