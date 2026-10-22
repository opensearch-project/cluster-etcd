/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.IndexStatus;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.NodeRole;
import org.opensearch.clusterless.contract.NodeStatus;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.ShardState;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A primary relocation as each end's node fabricates it, pass after pass, and as {@link PrimaryRelocations} decides
 * what each pass can act on. {@code PrimaryRelocationReplicationTests} runs the same entries through a real recovery
 * and handoff.
 */
public class RelocationFabricationTests extends OpenSearchTestCase {

    private static final ClusterName CLUSTER = new ClusterName("clusterless");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final DesiredState.IndexKey KEY = new DesiredState.IndexKey(ACME, UUID);
    private static final String SOURCE = "pv1sDMfZRxSTMiW5VrbN9w";
    private static final String TARGET = "72ZSKeRtQmuqOGyPvYzmLw";
    private static final String REPLICA = "BBBBBBBBBBBBBBBBBBBBBB";
    private static final long TERM = 4;

    // ---------------------------------------------------------------- the source

    public void testTheSourceRelocatesItsStartedPrimaryAndKeepsTheEntry() {
        ClusterState started = startLocal(source(primary(), sourceView(null), ClusterState.EMPTY_STATE), SOURCE);

        ClusterState first = source(relocatingTo(TARGET), sourceView(null), started);
        ShardRouting entry = shardTable(first).primaryShard();
        assertTrue(entry.toString(), entry.relocating());
        assertEquals(TARGET, entry.relocatingNodeId());
        assertEquals("the same copy, relocating", localPrimary(started).allocationId().getId(), entry.allocationId().getId());
        assertNotNull("the target's id, minted here", entry.allocationId().getRelocationId());
        assertNotNull("the target's node, which is replicated to from the start", first.nodes().get(TARGET));
        assertEquals("one primary entry, and the replica", 2, shardTable(first).size());

        ClusterState second = source(relocatingTo(TARGET), sourceView(entry.allocationId().getRelocationId()), first);
        assertEquals("the pass after keeps the minted id", entry, shardTable(second).primaryShard());
        assertTrue(StateEquivalence.sameContent(first, second));
    }

    public void testDroppingTheRelocationCancelsIt() {
        ClusterState started = startLocal(source(primary(), sourceView(null), ClusterState.EMPTY_STATE), SOURCE);
        ClusterState relocating = source(relocatingTo(TARGET), sourceView(null), started);

        ClusterState cancelled = source(primary(), sourceView(null), relocating);

        ShardRouting entry = shardTable(cancelled).primaryShard();
        assertTrue(entry.toString(), entry.started());
        assertEquals("the copy that was relocating, not a new one", localPrimary(started).allocationId().getId(), entry.allocationId().getId());
    }

    public void testAnInitializingPrimaryIsNotRelocatedUntilItHasStarted() {
        ClusterState initializing = source(primary(), sourceView(null), ClusterState.EMPTY_STATE);

        ClusterState asked = source(relocatingTo(TARGET), sourceView(null), initializing);

        assertTrue(shardTable(asked).primaryShard().initializing());
        assertEquals(localPrimary(initializing), localPrimary(asked));
    }

    // ---------------------------------------------------------------- the target

    public void testTheTargetBuildsTheSourcesEntryUnderThePublishedIds() {
        ClusterState first = target(relocatingFrom("aid-source", "aid-target"), targetView("aid-target"), ClusterState.EMPTY_STATE);

        IndexShardRoutingTable shard = shardTable(first);
        assertEquals("the pair is one primary entry: " + shard, 1, shard.shards().stream().filter(ShardRouting::primary).count());
        ShardRouting pair = shard.primaryShard();
        assertEquals(SOURCE, pair.currentNodeId());
        assertEquals(TARGET, pair.relocatingNodeId());
        assertEquals("aid-source", pair.allocationId().getId());
        assertEquals("aid-target", pair.allocationId().getRelocationId());
        assertTrue("the replica is routed beside it: " + shard, shard.shards().stream().anyMatch(copy -> REPLICA.equals(copy.currentNodeId())));
        assertNotNull("the source's node, which is recovered from", first.nodes().get(SOURCE));
        assertEquals("the target's own metadata, not a coordinator's", TERM, first.metadata().index(indexName()).primaryTerm(0));

        RoutingNode here = first.getRoutingNodes().node(TARGET);
        assertEquals(1, here.size());
        ShardRouting copy = here.iterator().next();
        assertTrue(copy.isRelocationTarget());
        assertEquals("aid-target", copy.allocationId().getId());

        ClusterState second = target(relocatingFrom("aid-source", "aid-target"), targetView("aid-target"), first);
        assertEquals(pair, shardTable(second).primaryShard());
        assertTrue(StateEquivalence.sameContent(first, second));
    }

    public void testACompletedTargetIsAPlainPrimary() {
        // LocalShardStateAction starts the target by replacing the source's entry with it. The assignment still names the
        // relocation until the supervisor sees the start, and the projection still shows the source.
        ClusterState recovering = target(relocatingFrom("aid-source", "aid-target"), targetView("aid-target"), ClusterState.EMPTY_STATE);
        ClusterState completed = completeOnTarget(recovering);

        ClusterState after = target(relocatingFrom("aid-source", "aid-target"), targetView("aid-target"), completed);

        IndexShardRoutingTable shard = shardTable(after);
        assertEquals("one primary: " + shard, 1, shard.shards().stream().filter(ShardRouting::primary).count());
        ShardRouting primary = shard.primaryShard();
        assertEquals(TARGET, primary.currentNodeId());
        assertTrue(primary.started());
        assertEquals("aid-target", primary.allocationId().getId());
        assertNull(primary.allocationId().getRelocationId());
    }

    // ---------------------------------------------------------------- what a pass can act on

    public void testATargetWaitsForThePublishedId() {
        PrimaryRelocations relocations = new PrimaryRelocations(TARGET);

        DesiredState reduced = relocations.reduce(desired(fromSource()), targetView(null), live(TARGET));

        assertTrue("nothing built, not even a plain primary", reduced.indices().isEmpty());
        assertEquals(reduced.deferred().toString(), 1, reduced.deferred().size());
    }

    public void testATargetIsResolvedFromTheProjection() {
        DesiredState reduced = new PrimaryRelocations(TARGET).reduce(desired(fromSource()), targetView("aid-target"), live(TARGET));

        assertEquals(
            new DesiredState.IncomingRelocation(NodeId.of(SOURCE), "aid-source", "aid-target"),
            reduced.indices().get(KEY).relocatingFrom().get(0)
        );
        assertTrue(reduced.deferred().toString(), reduced.deferred().isEmpty());
    }

    public void testARecoveringTargetKeepsItsIdsWhateverTheProjectionSays() {
        ClusterState recovering = target(relocatingFrom("aid-source", "aid-target"), targetView("aid-target"), ClusterState.EMPTY_STATE);

        DesiredState reduced = new PrimaryRelocations(TARGET).reduce(desired(fromSource()), targetView(null), recovering);

        assertEquals("aid-target", reduced.indices().get(KEY).relocatingFrom().get(0).targetAllocationId());
    }

    public void testATargetThatFailedIsNotBuiltAgainUnderTheSameId() {
        PrimaryRelocations relocations = new PrimaryRelocations(TARGET);
        ClusterState recovering = target(relocatingFrom("aid-source", "aid-target"), targetView("aid-target"), ClusterState.EMPTY_STATE);
        relocations.reduce(desired(fromSource()), targetView("aid-target"), recovering);
        // LocalShardStateAction fails a relocation target by cancelling the relocation, which takes the target away.
        ClusterState cancelled = withShard(recovering, shardTable(recovering).primaryShard().cancelRelocation(), replicaRouting(recovering));

        for (int pass = 0; pass < 3; pass++) {
            DesiredState reduced = relocations.reduce(desired(fromSource()), targetView("aid-target"), cancelled);
            assertTrue("pass " + pass, reduced.indices().isEmpty());
            assertTrue(reduced.deferred().toString(), reduced.deferred().get(0).contains("failed"));
        }

        DesiredState reissued = relocations.reduce(desired(fromSource()), targetView("aid-target-2"), cancelled);
        assertEquals("a new id is a new target", "aid-target-2", reissued.indices().get(KEY).relocatingFrom().get(0).targetAllocationId());
    }

    public void testATargetStartedHereIsAPlainPrimaryWithoutAReason() {
        ClusterState completed = completeOnTarget(
            target(relocatingFrom("aid-source", "aid-target"), targetView("aid-target"), ClusterState.EMPTY_STATE)
        );

        DesiredState reduced = new PrimaryRelocations(TARGET).reduce(desired(fromSource()), targetView(null), completed);

        assertEquals(ShardRole.PRIMARY, reduced.indices().get(KEY).shards().get(0));
        assertTrue(reduced.indices().get(KEY).relocatingFrom().isEmpty());
        assertTrue(reduced.deferred().toString(), reduced.deferred().isEmpty());
    }

    public void testASourceWaitsForItsTargetsNodeToBeInThePeerView() {
        CoordinatorView withoutTarget = new CoordinatorView(sourceView(null).indices(), Map.of(NodeId.of(REPLICA), peer(REPLICA)), List.of());

        DesiredState reduced = new PrimaryRelocations(SOURCE).reduce(desired(relocatingTo(TARGET)), withoutTarget, live(SOURCE));

        assertTrue(reduced.indices().get(KEY).relocatingTo().isEmpty());
        assertEquals("still the primary", ShardRole.PRIMARY, reduced.indices().get(KEY).shards().get(0));
        assertEquals(reduced.deferred().toString(), 1, reduced.deferred().size());
    }

    // ---------------------------------------------------------------- fixtures

    private static DesiredState.DesiredIndex primary() {
        return new DesiredState.DesiredIndex(KEY, config(), Map.of(0, ShardRole.PRIMARY), Map.of(0, TERM));
    }

    private static DesiredState.DesiredIndex relocatingTo(String target) {
        return primary().withRelocations(Map.of(0, NodeId.of(target)), Map.of());
    }

    private static DesiredState.DesiredIndex fromSource() {
        return primary().withRelocations(Map.of(), Map.of(0, DesiredState.IncomingRelocation.from(NodeId.of(SOURCE))));
    }

    private static DesiredState.DesiredIndex relocatingFrom(String sourceAllocationId, String targetAllocationId) {
        return primary().withRelocations(
            Map.of(),
            Map.of(0, new DesiredState.IncomingRelocation(NodeId.of(SOURCE), sourceAllocationId, targetAllocationId))
        );
    }

    private static DesiredState desired(DesiredState.DesiredIndex index) {
        return new DesiredState(42, Map.of(KEY, index), List.of(), List.of());
    }

    /** The source's node, whose primary's tracker holds the replica in sync. */
    private static ClusterState source(DesiredState.DesiredIndex index, CoordinatorView view, ClusterState previous) {
        return fabricate(SOURCE, index, view, previous, shardId -> Set.of("aid-replica"));
    }

    /** The target's node, before the handoff: not in primary mode, so its tracker cannot say. */
    private static ClusterState target(DesiredState.DesiredIndex index, CoordinatorView view, ClusterState previous) {
        return fabricate(TARGET, index, view, previous, TrackedInSync.UNKNOWN);
    }

    private static ClusterState fabricate(
        String local,
        DesiredState.DesiredIndex index,
        CoordinatorView view,
        ClusterState previous,
        TrackedInSync tracked
    ) {
        return new CoordinatorState(
            new DataNodeState(CLUSTER, TestFixtures.localNode(local), desired(index), MappingSource.WRAPPED, LocalShardStore.EMPTY),
            TestFixtures.localNode(local),
            view,
            MappingSource.WRAPPED,
            tracked
        ).buildClusterState(previous);
    }

    /** As the source's node sees it: its own primary and a replica, and the target's node, which is not a copy yet. */
    private static CoordinatorView sourceView(String targetAllocationId) {
        List<IndexStatus.Copy> copies = new ArrayList<>();
        copies.add(new IndexStatus.Copy(NodeId.of(SOURCE), ShardRole.PRIMARY, ShardState.STARTED, true, "aid-source", null,
            targetAllocationId == null ? null : NodeId.of(TARGET), targetAllocationId));
        copies.add(new IndexStatus.Copy(NodeId.of(REPLICA), ShardRole.REPLICA, ShardState.STARTED, true, "aid-replica", null));
        return view(copies, Map.of(0, ShardRole.PRIMARY), SOURCE, TARGET, REPLICA);
    }

    /** As the target's node sees it: the source relocating here, with the id it minted or none yet, and a replica. */
    private static CoordinatorView targetView(String targetAllocationId) {
        List<IndexStatus.Copy> copies = new ArrayList<>();
        copies.add(new IndexStatus.Copy(NodeId.of(SOURCE), ShardRole.PRIMARY, ShardState.STARTED, true, "aid-source",
            List.of("aid-replica", "aid-source"), NodeId.of(TARGET), targetAllocationId));
        copies.add(new IndexStatus.Copy(NodeId.of(REPLICA), ShardRole.REPLICA, ShardState.STARTED, true, "aid-replica", null));
        return view(copies, Map.of(0, ShardRole.PRIMARY), SOURCE, REPLICA);
    }

    private static CoordinatorView view(List<IndexStatus.Copy> copies, Map<Integer, ShardRole> localRoles, String... peers) {
        Map<NodeId, NodeStatus> resolved = new LinkedHashMap<>();
        for (String peer : peers) {
            resolved.put(NodeId.of(peer), peer(peer));
        }
        IndexStatus placement = new IndexStatus("my-index", UUID, 3041, 1, 1, IndexStatus.Replication.DOCUMENT, Map.of(0, copies));
        return new CoordinatorView(List.of(new CoordinatorView.RoutedIndex(ACME, config(), placement, Set.of(0), localRoles)), resolved, List.of());
    }

    /** The state after this node's own copy has started, as the server reports it. */
    private static ClusterState startLocal(ClusterState state, String local) {
        IndexShardRoutingTable shard = shardTable(state);
        IndexShardRoutingTable.Builder started = new IndexShardRoutingTable.Builder(shard.shardId());
        for (ShardRouting copy : shard) {
            started.addShard(local.equals(copy.currentNodeId()) && copy.initializing() ? copy.moveToStarted() : copy);
        }
        return withTable(state, started.build());
    }

    /** What LocalShardStateAction.startShard does with a started relocation target. */
    private static ClusterState completeOnTarget(ClusterState recovering) {
        ShardRouting pair = shardTable(recovering).primaryShard();
        return withShard(recovering, pair.getTargetRelocatingShard().moveToStarted(), replicaRouting(recovering));
    }

    private static ShardRouting replicaRouting(ClusterState state) {
        return shardTable(state).shards().stream().filter(copy -> REPLICA.equals(copy.currentNodeId())).findFirst().orElseThrow();
    }

    private static ClusterState withShard(ClusterState state, ShardRouting primary, ShardRouting replica) {
        return withTable(state, new IndexShardRoutingTable.Builder(primary.shardId()).addShard(primary).addShard(replica).build());
    }

    private static ClusterState withTable(ClusterState state, IndexShardRoutingTable shard) {
        IndexRoutingTable index = IndexRoutingTable.builder(shard.shardId().getIndex()).addIndexShard(shard).build();
        return ClusterState.builder(state).routingTable(RoutingTable.builder(state.routingTable()).add(index).build()).build();
    }

    /** A live state holding nothing, on the given node. */
    private static ClusterState live(String local) {
        return new DataNodeState(
            CLUSTER,
            TestFixtures.localNode(local),
            new DesiredState(1, Map.of(), List.of(), List.of()),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(ClusterState.EMPTY_STATE);
    }

    private static IndexShardRoutingTable shardTable(ClusterState state) {
        return state.routingTable().index(state.metadata().index(indexName()).getIndex()).shard(0);
    }

    private static ShardRouting localPrimary(ClusterState state) {
        return shardTable(state).primaryShard();
    }

    private static String indexName() {
        return IndexNaming.indexName(ACME, UUID, "my-index");
    }

    private static NodeStatus peer(String nodeId) {
        return new NodeStatus(
            NodeId.of(nodeId),
            "eph-" + nodeId,
            "data-" + nodeId.substring(0, 4),
            EnumSet.of(NodeRole.DATA),
            "127.0.0.1",
            9300 + Math.abs(nodeId.hashCode() % 100),
            "3.9.0",
            Map.of(),
            NodeStatus.Liveness.LIVE
        );
    }

    private static IndexConfig config() {
        return new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "1", "index.number_of_replicas", "1", "index.replication.type", "DOCUMENT"),
            Map.of(),
            3041
        );
    }
}
