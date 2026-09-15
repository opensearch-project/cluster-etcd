/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The four cells of the recovery-source table, and the two that would destroy something.
 *
 * <p>Every test here is about a primary that this node has no local copy of, which is the only case where the
 * question arises. The reason they matter more than most tests in this repository: two of the four combinations
 * of (what the repository holds, what we tell the server) are silent — one discards a shard and one leaves an
 * index unable to start, and neither produces an error at the moment of the mistake.
 */
public class RemoteRecoveryTests extends OpenSearchTestCase {

    private static final ClusterName CLUSTER = new ClusterName("clusterless");
    private static final String LOCAL = "pv1sDMfZRxSTMiW5VrbN9w";
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    /** A node with a remote store for both segments and translog, which is what makes REMOTE_STORE usable. */
    private static RemoteStore withTranslog() {
        return RemoteStore.of(TestFixtures.remoteStoreNode(LOCAL, "segments"));
    }

    public void testDataInTheRepositoryRecoversFromItRatherThanStartingEmpty() {
        // The whole point. Starting empty here would discard a complete copy of the shard and then serve as its
        // authoritative primary.
        RemoteRecovery.Plan plan = plan(withTranslog(), RemoteShardData.Answer.HAS_DATA);

        RecoverySource source = plan.sources().get(shard(0));
        assertNotNull("a recovery source must have been decided", source);
        assertEquals(RecoverySource.Type.REMOTE_STORE, source.getType());
        assertEquals("the copy is still built", 1, plan.desired().copyCount());
        assertTrue(plan.desired().deferred().toString(), plan.desired().deferred().isEmpty());
    }

    public void testAnEmptyRepositoryMeansANewShardAndAnEmptyStore() {
        RemoteRecovery.Plan plan = plan(withTranslog(), RemoteShardData.Answer.EMPTY);

        assertEquals(RecoverySource.Type.EMPTY_STORE, plan.sources().get(shard(0)).getType());
        assertEquals(1, plan.desired().copyCount());
        assertTrue(plan.desired().deferred().isEmpty());
    }

    public void testAnUnanswerableRepositoryDeclinesTheCopyRatherThanGuessing() {
        // Both guesses are unsafe, so neither is made. Unreachable is not empty: treating it as empty is how a
        // network blip becomes data loss, and defaulting the other way makes a new index fail for ever.
        RemoteRecovery.Plan plan = plan(withTranslog(), RemoteShardData.Answer.UNKNOWN);

        assertEquals("the copy must not be built", 0, plan.desired().copyCount());
        assertTrue(plan.sources().isEmpty());
        assertEquals(1, plan.desired().deferred().size());
        String reason = plan.desired().deferred().get(0);
        assertTrue(reason, reason.contains("could not tell"));
        assertTrue("the reason must name the shard", reason.contains("/0"));
    }

    public void testAProbeThatThrowsIsTreatedAsUnanswerable() {
        RemoteRecovery recovery = new RemoteRecovery(withTranslog(), LocalShardStore.EMPTY, (shardId, repository) -> {
            throw new IllegalStateException("the repository is on fire");
        }, LOCAL);

        RemoteRecovery.Plan plan = recovery.planFor(desired(Map.of(0, ShardRole.PRIMARY)), ClusterState.EMPTY_STATE);

        assertEquals(0, plan.desired().copyCount());
        assertEquals(1, plan.desired().deferred().size());
    }

    public void testDataInTheRepositoryWithNoRemoteTranslogIsDeclinedWithTheReasonWhy() {
        // The configuration cluster-etcd used deliberately: segments in a repository, no translog, because
        // pull-based ingestion makes a transaction log redundant. REMOTE_STORE recovery cannot serve it --
        // syncTranslogFilesFromRemoteTranslog asserts and casts to the remote translog factory -- and starting
        // empty would discard the shard, so the copy is declined and the reason says what is missing.
        RemoteRecovery.Plan plan = plan(segmentsOnly(), RemoteShardData.Answer.HAS_DATA);

        assertEquals(0, plan.desired().copyCount());
        String reason = plan.desired().deferred().get(0);
        assertTrue(reason, reason.contains("index.remote_store.translog.repository"));
        assertTrue(reason, reason.contains("holds data for this shard"));
    }

    public void testAnEmptyRepositoryStillStartsEmptyWithNoRemoteTranslog() {
        // The translog only matters for reading data back out. A new shard needs nothing read.
        RemoteRecovery.Plan plan = plan(segmentsOnly(), RemoteShardData.Answer.EMPTY);

        assertEquals(RecoverySource.Type.EMPTY_STORE, plan.sources().get(shard(0)).getType());
        assertEquals(1, plan.desired().copyCount());
    }

    // ---------------------------------------------------------------- when the question is not asked at all

    public void testANodeWithNoRemoteStoreDecidesNothing() {
        RemoteRecovery recovery = new RemoteRecovery(RemoteStore.NONE, LocalShardStore.EMPTY, failIfAsked(), LOCAL);

        RemoteRecovery.Plan plan = recovery.planFor(desired(Map.of(0, ShardRole.PRIMARY)), ClusterState.EMPTY_STATE);

        assertTrue(plan.sources().isEmpty());
        assertEquals(1, plan.desired().copyCount());
    }

    public void testAnIndexTheRemoteStoreDoesNotBackIsNotProbed() {
        // Document replication, so the disk is still the only place its data could be.
        IndexConfig docrep = new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "1", "index.number_of_replicas", "0"),
            Map.of(),
            3041
        );
        RemoteRecovery recovery = new RemoteRecovery(withTranslog(), LocalShardStore.EMPTY, failIfAsked(), LOCAL);

        RemoteRecovery.Plan plan = recovery.planFor(desiredWith(docrep, Map.of(0, ShardRole.PRIMARY)), ClusterState.EMPTY_STATE);

        assertTrue(plan.sources().isEmpty());
        assertEquals(1, plan.desired().copyCount());
    }

    public void testASearchReplicaIsNotProbed() {
        // It reads the repository while opening its engine, from an ordinary store recovery source, so there is
        // nothing here to decide -- and probing every replica every pass would be a network read per shard.
        RemoteRecovery recovery = new RemoteRecovery(withTranslog(), LocalShardStore.EMPTY, failIfAsked(), LOCAL);

        RemoteRecovery.Plan plan = recovery.planFor(desired(Map.of(0, ShardRole.SEARCH_REPLICA)), ClusterState.EMPTY_STATE);

        assertTrue(plan.sources().isEmpty());
        assertEquals(1, plan.desired().copyCount());
    }

    public void testAShardWithLocalDataIsNotProbed() {
        // There are bytes to resume from, which is both cheaper and better informed than anything a repository
        // could say.
        RemoteRecovery recovery = new RemoteRecovery(withTranslog(), shardId -> true, failIfAsked(), LOCAL);

        RemoteRecovery.Plan plan = recovery.planFor(desired(Map.of(0, ShardRole.PRIMARY)), ClusterState.EMPTY_STATE);

        assertTrue(plan.sources().isEmpty());
        assertEquals(1, plan.desired().copyCount());
    }

    public void testACopyAlreadyServingHereIsNotProbed() {
        // The steady state, and the reason this costs nothing per pass: a converged node probes nothing at all.
        // Also the important one for a network read -- one probe per shard per second would be a real cost.
        ClusterState live = new DataNodeState(
            CLUSTER,
            node(),
            desired(Map.of(0, ShardRole.PRIMARY)),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY,
            withTranslog(),
            Set.of()
        ).buildClusterState(ClusterState.EMPTY_STATE);
        RemoteRecovery recovery = new RemoteRecovery(withTranslog(), LocalShardStore.EMPTY, failIfAsked(), LOCAL);

        RemoteRecovery.Plan plan = recovery.planFor(desired(Map.of(0, ShardRole.PRIMARY)), live);

        assertTrue(plan.sources().isEmpty());
        assertEquals(1, plan.desired().copyCount());
    }

    // ---------------------------------------------------------------- what the fabricator does with the answer

    public void testTheFabricatorUsesTheLookedUpSourceForAFreshCopy() {
        RemoteRecovery.Plan plan = plan(withTranslog(), RemoteShardData.Answer.HAS_DATA);

        ClusterState state = new DataNodeState(
            CLUSTER,
            node(),
            plan.desired(),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY,
            withTranslog(),
            Set.of(),
            plan.sources()
        ).buildClusterState(ClusterState.EMPTY_STATE);

        IndexMetadata index = state.metadata().iterator().next();
        assertEquals(
            RecoverySource.Type.REMOTE_STORE,
            state.routingTable().index(index.getIndex()).shard(0).shards().get(0).recoverySource().getType()
        );
    }

    public void testLocalBytesWinOverALookedUpSource() {
        // The two cannot disagree in the direction that discards data: if the disk has gained bytes since the
        // lookup, the disk is right and the lookup is the stale one.
        ClusterState state = new DataNodeState(
            CLUSTER,
            node(),
            desired(Map.of(0, ShardRole.PRIMARY)),
            MappingSource.WRAPPED,
            shardId -> true,
            withTranslog(),
            Set.of(),
            Map.of(shard(0), RecoverySource.EmptyStoreRecoverySource.INSTANCE)
        ).buildClusterState(ClusterState.EMPTY_STATE);

        IndexMetadata index = state.metadata().iterator().next();
        assertEquals(
            RecoverySource.Type.EXISTING_STORE,
            state.routingTable().index(index.getIndex()).shard(0).shards().get(0).recoverySource().getType()
        );
    }

    public void testTheFabricatorStampsNoRemoteStoreCustomData() {
        // The coupling the probe depends on, asserted from this side too. RepositoryRemoteShardData asks the
        // repository using PathType.FIXED, which is only correct because determineRemoteStorePathStrategy falls
        // back to FIXED when there is no REMOTE_STORE_CUSTOM_KEY. If this fabrication ever stamps that custom
        // data, the probe starts reading a different path -- finds nothing, reports EMPTY, and a returning
        // primary discards its shard. This test is the thing that says so.
        ClusterState state = new DataNodeState(
            CLUSTER,
            node(),
            desired(Map.of(0, ShardRole.PRIMARY)),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY,
            withTranslog(),
            Set.of()
        ).buildClusterState(ClusterState.EMPTY_STATE);

        assertNull(state.metadata().iterator().next().getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY));
    }

    // ---------------------------------------------------------------- fixtures

    /**
     * A node whose indices keep segments in the repository and their translog locally.
     *
     * <p>Expressed the way OpenSearch expresses it: {@code node.attr.remote_store.mode=segments_only}, which was
     * added upstream for this architecture and makes {@code RemoteStoreNodeAttribute} require the segment
     * repository alone. Without the mode attribute a node naming a segment repository must name a translog and a
     * cluster-state one too, so this is the real configuration rather than a fixture trick.
     */
    private static RemoteStore segmentsOnly() {
        RemoteStore store = RemoteStore.of(TestFixtures.segmentsOnlyNode(LOCAL, "segments"));
        assertTrue("the node still has a segment repository", store.configured());
        assertFalse("and its indices do not use a remote translog", store.translogConfigured());
        return store;
    }

    private static RemoteShardData failIfAsked() {
        return (shardId, repository) -> {
            throw new AssertionError("the repository must not be asked about " + shardId);
        };
    }

    private static RemoteRecovery.Plan plan(RemoteStore remoteStore, RemoteShardData.Answer answer) {
        RemoteRecovery recovery = new RemoteRecovery(
            remoteStore,
            LocalShardStore.EMPTY,
            (shardId, repository) -> answer,
            LOCAL
        );
        return recovery.planFor(desired(Map.of(0, ShardRole.PRIMARY)), ClusterState.EMPTY_STATE);
    }

    private static DiscoveryNode node() {
        return TestFixtures.remoteStoreNode(LOCAL, "segments");
    }

    private static ShardId shard(int number) {
        return new ShardId(new Index(IndexNaming.indexName(ACME, UUID, "my-index"), UUID), number);
    }

    private static DesiredState desired(Map<Integer, ShardRole> shards) {
        return desiredWith(TestFixtures.segmentReplicatedIndexConfig(UUID), shards);
    }

    private static DesiredState desiredWith(IndexConfig config, Map<Integer, ShardRole> shards) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        return new DesiredState(42, Map.of(key, new DesiredState.DesiredIndex(key, config, shards)), List.of(), List.of());
    }
}
