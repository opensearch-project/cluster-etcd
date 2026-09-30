/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * The primary term a node fabricates, which decides whether a promotion is one.
 *
 * <p>Terms were a constant 1 here for every shard for ever, which is why nothing could be fenced and why a
 * promotion was impossible: {@code IndexShard.updateShardState} promotes a started replica only on a term that
 * <em>increases</em>, and does nothing at all on one that does not. The supervisor now issues a term per shard.
 */
public class PrimaryTermsTests extends OpenSearchTestCase {

    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    public void testTheAssignedTermReachesTheFabricatedMetadata() {
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 4L)));

        assertEquals(4L, onlyIndex(state).primaryTerm(0));
    }

    public void testANewIndexsFirstPrimaryIsAtTermTwo() {
        // Recorded by the supervisor as a consequence for this side, and worth a test because "first primary at 2"
        // reads like an off-by-one. Its counter starts at 1 and placing a primary always advances it, so 2 is the
        // first term any primary is ever told -- a shard with no primary yet cannot be told apart from one whose
        // primary is on a lost node, and the second is the case the term exists to fence.
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 2L)));

        assertEquals(2L, onlyIndex(state).primaryTerm(0));
    }

    public void testAnAssignmentWithNoTermsStaysAtOne() {
        // An un-upgraded supervisor writes none, and a node must then fabricate exactly what it fabricated before
        // terms existed -- which is how the two versions interoperate without either knowing about the other.
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY), Map.of()));

        assertEquals(1L, onlyIndex(state).primaryTerm(0));
    }

    public void testGainingExplicitTermsIsAChangeAndThenAFixedPoint() {
        // The supervisor rewrites every assignment once when terms land, so this node sees 1 become an explicit
        // value. That has to be a change -- otherwise the state is never published and the term never reaches the
        // server -- and then it has to settle, or the pass publishes for ever.
        ClusterState atOne = withShardStarted(build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY), Map.of())));
        ClusterState atTwo = build(atOne, desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 2L)));

        assertFalse("an explicit term above the running one must be a change", StateEquivalence.sameContent(atOne, atTwo));
        assertEquals(2L, onlyIndex(atTwo).primaryTerm(0));

        ClusterState again = build(atTwo, desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 2L)));
        assertTrue("and the pass after must change nothing", StateEquivalence.sameContent(atTwo, again));
    }

    public void testAPromotionKeepsTheAllocationIdSoItIsAPromotion() {
        // The whole point. updateShardState decides between promoting in place and replacing the copy by whether the
        // new routing entry is the same allocation -- a fresh id makes isSameAllocation false, so the shard is
        // removed and another created, at the old primary's term, which is two copies believing they are primary at
        // one term. So the allocation id must survive the role change.
        ClusterState asReplica = withShardStarted(build(ClusterState.EMPTY_STATE, docrep(Map.of(0, ShardRole.REPLICA), Map.of(0, 4L))));
        ShardRouting replica = onlyCopy(asReplica, 0);
        assertFalse(replica.primary());

        ClusterState promoted = build(asReplica, docrep(Map.of(0, ShardRole.PRIMARY), Map.of(0, 5L)));
        ShardRouting primary = onlyCopy(promoted, 0);

        assertTrue("it must now be the primary", primary.primary());
        assertEquals("and the same copy, or it is a replacement rather than a promotion",
            replica.allocationId().getId(), primary.allocationId().getId());
        assertEquals("with the term the supervisor advanced to", 5L, onlyIndex(promoted).primaryTerm(0));
        assertTrue("and still started, since a promotion does not re-recover", primary.started());
    }

    public void testAFailedReplicaIsNotPromotedInPlace() {
        // Promoting in place keeps the allocation id, and the server skips a failed shard until the id changes. So a
        // replica the server has failed becomes a new primary, recovering from what is on disk, rather than a promotion
        // of a copy that no longer exists.
        ClusterState asReplica = withShardStarted(build(ClusterState.EMPTY_STATE, docrep(Map.of(0, ShardRole.REPLICA), Map.of(0, 4L))));
        ShardRouting replica = onlyCopy(asReplica, 0);

        ClusterState promoted = build(withShardFailed(asReplica, 0), docrep(Map.of(0, ShardRole.PRIMARY), Map.of(0, 5L)));

        ShardRouting primary = onlyCopy(promoted, 0);
        assertTrue(primary.primary());
        assertTrue(primary.initializing());
        assertNotEquals(replica.allocationId().getId(), primary.allocationId().getId());
        assertEquals(5L, onlyIndex(promoted).primaryTerm(0));
    }

    public void testASearchReplicaIsNotPromotedInPlace() {
        // Upstream refuses it -- moveActiveReplicaToPrimary throws on a search-only copy, which has no history to
        // promote -- so the fabrication must not attempt it. A fresh entry is the honest outcome here.
        ClusterState asSearchReplica = withShardStarted(
            build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.SEARCH_REPLICA), Map.of(0, 4L)))
        );
        String before = onlyCopy(asSearchReplica, 0).allocationId().getId();

        ClusterState asPrimary = build(asSearchReplica, desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 5L)));

        assertTrue(onlyCopy(asPrimary, 0).primary());
        assertNotEquals("a search replica cannot be promoted in place", before, onlyCopy(asPrimary, 0).allocationId().getId());
    }

    // ---------------------------------------------------------------- the term that goes backwards

    public void testALowerTermThanAStartedCopyIsDeclinedWithAReason() {
        // The contract says the supervisor never lowers a term in place, and a node told a lower one defers it. The
        // server would refuse it anyway -- terms only go up -- so the value of declining is that it is *said*: a
        // term that went backwards means the supervisor's counter did, which its compare-and-set exists to prevent.
        ClusterState running = withShardStarted(build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 7L))));

        DesiredState reduced = PrimaryTerms.reduce(desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 5L)), running);

        assertEquals(0, reduced.copyCount());
        assertEquals(1, reduced.deferred().size());
        assertTrue(reduced.deferred().get(0), reduced.deferred().get(0).contains("must not go backwards"));
    }

    public void testTheSameOrAHigherTermIsNotDeclined() {
        ClusterState running = withShardStarted(build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 5L))));

        assertEquals(1, PrimaryTerms.reduce(desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 5L)), running).copyCount());
        assertEquals(1, PrimaryTerms.reduce(desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 6L)), running).copyCount());
    }

    public void testALowerTermOnACopyThatIsNotStartedIsNotDeclined() {
        // Nothing is in flight at the higher term, so the lower one is a stale read of a document about to be
        // rewritten. Level-triggering covers it, and declining would stall a shard that is merely still recovering.
        ClusterState initializing = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 7L)));

        DesiredState reduced = PrimaryTerms.reduce(desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 5L)), initializing);

        assertEquals(1, reduced.copyCount());
        assertTrue(reduced.deferred().toString(), reduced.deferred().isEmpty());
    }

    public void testAnAssignmentWithNoTermsIsNeverDeclined() {
        ClusterState running = withShardStarted(build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY), Map.of(0, 7L))));

        assertEquals(1, PrimaryTerms.reduce(desired(Map.of(0, ShardRole.PRIMARY), Map.of()), running).copyCount());
    }

    // ---------------------------------------------------------------- fixtures

    public void testAPromotionReachesItsTermWhenASiblingReplicaIsDeclined() {
        // The steps in the order AssignmentPoller runs them. PeerRecovery declined the replica of shard 1 and rebuilt
        // the index with no terms at all. This step then had nothing to check, and the fabrication took the larger
        // of 1 and the running term -- so the replica of shard 0, promoted to 3, stayed a primary at 2: the old
        // primary's term, which is the one thing a promotion must not keep. For as long as the sibling stayed
        // declined.
        IndexConfig twoShards = new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "2", "index.number_of_replicas", "1", "index.replication.type", "DOCUMENT"),
            Map.of(),
            3041
        );
        ClusterState asReplica = withShardStarted(
            build(ClusterState.EMPTY_STATE, desiredWith(twoShards, Map.of(0, ShardRole.REPLICA), Map.of(0, 2L)))
        );
        DesiredState promoted = desiredWith(twoShards, Map.of(0, ShardRole.PRIMARY, 1, ShardRole.REPLICA), Map.of(0, 3L, 1, 2L));
        CoordinatorView nothingVisible = new CoordinatorView(List.of(), Map.of(), List.of());

        DesiredState reduced = PrimaryTerms.reduce(PeerRecovery.reduce(promoted, nothingVisible), asReplica);
        ClusterState next = build(asReplica, reduced);

        assertEquals("only the replica is declined: " + reduced.deferred(), 1, reduced.deferred().size());
        assertTrue(onlyCopy(next, 0).primary());
        assertEquals("the promotion's term, not the old primary's", 3L, onlyIndex(next).primaryTerm(0));

        DesiredState again = PrimaryTerms.reduce(PeerRecovery.reduce(promoted, nothingVisible), next);
        assertTrue("and the pass after changes nothing", StateEquivalence.sameContent(next, build(next, again)));
    }

    private static ClusterState build(ClusterState previous, DesiredState desired) {
        return new DataNodeState(
            new org.opensearch.cluster.ClusterName("clusterless"),
            TestFixtures.localNode("pv1sDMfZRxSTMiW5VrbN9w"),
            desired,
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(previous);
    }

    private static DesiredState desired(Map<Integer, ShardRole> shards, Map<Integer, Long> terms) {
        return desiredWith(config("SEGMENT"), shards, terms);
    }

    private static DesiredState docrep(Map<Integer, ShardRole> shards, Map<Integer, Long> terms) {
        return desiredWith(config("DOCUMENT"), shards, terms);
    }

    private static DesiredState desiredWith(IndexConfig config, Map<Integer, ShardRole> shards, Map<Integer, Long> terms) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        return new DesiredState(
            42,
            Map.of(key, new DesiredState.DesiredIndex(key, config, shards, terms)),
            List.of(),
            List.of()
        );
    }

    private static IndexConfig config(String replication) {
        return new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "1", "index.number_of_replicas", "1", "index.replication.type", replication),
            Map.of(),
            3041
        );
    }

    private static IndexMetadata onlyIndex(ClusterState state) {
        return state.metadata().iterator().next();
    }

    private static ShardRouting onlyCopy(ClusterState state, int shard) {
        return state.routingTable().index(onlyIndex(state).getIndex()).shard(shard).shards().get(0);
    }

    /**
     * What {@code LocalShardStateAction.localShardFailed} leaves in the state: the copy unassigned, as a cluster manager
     * would leave it. Before that, the entry stayed as it was and a pass reused it, allocation id and all, so the server
     * never recreated the shard it had failed.
     */
    private static ClusterState withShardFailed(ClusterState state, int shard) {
        org.opensearch.core.index.Index index = onlyIndex(state).getIndex();
        org.opensearch.cluster.routing.IndexRoutingTable.Builder routing =
            org.opensearch.cluster.routing.IndexRoutingTable.builder(index);
        for (org.opensearch.cluster.routing.IndexShardRoutingTable table : state.routingTable().index(index)) {
            for (ShardRouting copy : table) {
                routing.addShard(
                    table.shardId().id() == shard
                        ? copy.moveToUnassigned(
                            new org.opensearch.cluster.routing.UnassignedInfo(
                                org.opensearch.cluster.routing.UnassignedInfo.Reason.ALLOCATION_FAILED,
                                "failed shard on node [" + copy.currentNodeId() + "]: engine failure"
                            )
                        )
                        : copy
                );
            }
        }
        return ClusterState.builder(state)
            .routingTable(org.opensearch.cluster.routing.RoutingTable.builder().add(routing).build())
            .build();
    }

    private static ClusterState withShardStarted(ClusterState state) {
        org.opensearch.core.index.Index index = onlyIndex(state).getIndex();
        org.opensearch.cluster.routing.IndexRoutingTable.Builder routing =
            org.opensearch.cluster.routing.IndexRoutingTable.builder(index);
        for (org.opensearch.cluster.routing.IndexShardRoutingTable shard : state.routingTable().index(index)) {
            for (ShardRouting copy : shard) {
                routing.addShard(copy.initializing() ? copy.moveToStarted() : copy);
            }
        }
        return ClusterState.builder(state)
            .routingTable(org.opensearch.cluster.routing.RoutingTable.builder().add(routing).build())
            .build();
    }
}
