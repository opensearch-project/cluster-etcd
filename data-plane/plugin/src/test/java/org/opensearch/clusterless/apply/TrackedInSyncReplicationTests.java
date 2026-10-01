/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestUtils;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The fabricated in-sync set, handed to a real primary.
 *
 * <p>{@code CoordinatorStateTests} feeds the fabrication a tracker. Here the tracker is the primary's own, after a real
 * peer recovery, and the set and routing table go to {@code IndexShard.updateShardState} -- the call
 * {@code IndicesClusterStateService} makes on every applied state -- and then a write goes through
 * {@code ReplicationOperation}, which sends to {@code primary.getReplicationGroup().getReplicationTargets()}. So what is
 * checked is the server's own conclusion: whether {@code ReplicationTracker.updateFromClusterManager} accepts the set
 * (it asserts every in-sync id is already in sync in the tracker), and whether the replica is still written to.
 */
public class TrackedInSyncReplicationTests extends OpenSearchIndexLevelReplicationTestCase {

    /** Above any version the harness itself applies, since the tracker ignores a state it has already seen. */
    private final AtomicLong clusterStateVersion = new AtomicLong(1_000_000);

    public void testAReplicaTheFabricationCallsInSyncIsStillWrittenTo() throws Exception {
        try (ReplicationGroup group = createGroup(1)) {
            group.startAll();
            IndexShard primary = group.getPrimary();
            IndexShard replica = group.getReplicas().get(0);
            IndexShardRoutingTable table = startedTable(primary, replica);

            Set<String> inSync = CoordinatorState.inSyncOf(
                table,
                primary.routingEntry().currentNodeId(),
                IndicesServiceTrackedInSync.of(primary)
            );
            assertEquals(Set.of(primary.routingEntry().allocationId().getId(), replica.routingEntry().allocationId().getId()), inSync);

            // Twice, as every pass applies it again.
            apply(primary, inSync, table);
            apply(primary, inSync, table);

            assertTrue(
                "the replica must still be a replication target: " + primary.getReplicationGroup(),
                primary.getReplicationGroup().getReplicationTargets().contains(replica.routingEntry())
            );
            int docs = group.indexDocs(randomIntBetween(1, 10));
            assertEquals("every write reached the replica", docs, getShardDocUIDs(replica).size());
        }
    }

    public void testTheServerRejectsAStartedCopyOutsideTheInSyncSet() throws Exception {
        // What this side fabricated before, and the reason a started copy its primary does not hold in sync is now left
        // out of the primary's routing table rather than routed. updateFromClusterManager drops a started copy that is
        // not in the set, and ReplicationTracker's invariant then asserts that every copy in the routing table is
        // tracked. Without assertions, the copy is silently no longer written to.
        try (ReplicationGroup group = createGroup(1)) {
            group.startAll();
            IndexShard primary = group.getPrimary();
            IndexShard replica = group.getReplicas().get(0);

            AssertionError e = expectThrows(
                AssertionError.class,
                () -> apply(primary, Set.of(primary.routingEntry().allocationId().getId()), startedTable(primary, replica))
            );
            assertTrue(e.getMessage(), e.getMessage().contains("not in-sync with routing table"));
        }
    }

    public void testAStartedCopyLeftOutOfTheTableIsAcceptedAndNoLongerWrittenTo() throws Exception {
        // What the fabrication now builds when the primary's tracker does not hold a started copy -- after a restart or a
        // promotion, say: the copy is not routed, and the set is the primary alone. The server accepts it, and the copy
        // is no longer written to, which is why its own node has to rebuild it (finding 12, the replica side).
        try (ReplicationGroup group = createGroup(1)) {
            group.startAll();
            IndexShard primary = group.getPrimary();
            IndexShard replica = group.getReplicas().get(0);
            IndexShardRoutingTable primaryOnly = new IndexShardRoutingTable.Builder(primary.shardId()).addShard(primary.routingEntry()).build();

            apply(primary, CoordinatorState.inSyncOf(primaryOnly, primary.routingEntry().currentNodeId(), Set.of()), primaryOnly);
            apply(primary, CoordinatorState.inSyncOf(primaryOnly, primary.routingEntry().currentNodeId(), Set.of()), primaryOnly);

            assertEquals(Set.of(primary.routingEntry().allocationId().getId()), IndicesServiceTrackedInSync.of(primary));
            int docs = group.indexDocs(randomIntBetween(1, 10));
            assertEquals(docs, getShardDocUIDs(primary).size());
            assertEquals("the replica is not written to", 0, getShardDocUIDs(replica).size());

            // Out of the group before it closes, since closing asserts every copy holds the same documents, and this one
            // was left behind on purpose.
            group.removeReplica(replica);
            closeShards(replica);
        }
    }

    private static IndexShardRoutingTable startedTable(IndexShard primary, IndexShard replica) {
        ShardRouting replicaRouting = replica.routingEntry();
        assertTrue(replicaRouting.started());
        return new IndexShardRoutingTable.Builder(primary.shardId()).addShard(primary.routingEntry()).addShard(replicaRouting).build();
    }

    private void apply(IndexShard primary, Set<String> inSync, IndexShardRoutingTable table) throws IOException {
        primary.updateShardState(
            primary.routingEntry(),
            primary.getPendingPrimaryTerm(),
            null,
            clusterStateVersion.incrementAndGet(),
            inSync,
            table,
            IndexShardTestUtils.getFakeDiscoveryNodes(table.getShards())
        );
    }
}
