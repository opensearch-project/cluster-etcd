/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestUtils;
import org.opensearch.indices.recovery.RecoveryTarget;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A primary relocation between real shards, each given the routing table and in-sync set its own node fabricates.
 *
 * <p>The two nodes never share a cluster state, so each builds the relocating entry on its own: the source's node by
 * {@code ShardRouting.relocate}, which mints the target's id, and the target's node by {@link RelocatingEntry}, from the
 * ids the projection carries. The server's recovery and handoff then run between the two shards as they would between
 * two nodes. What is checked is the server's own conclusion: that the target recovers from the source under the entry
 * its node built, takes over in the handoff, and that its tracker accepts the in-sync set {@link CoordinatorState}
 * computes for it, keeping the shard's other replica in the group.
 */
public class PrimaryRelocationReplicationTests extends OpenSearchIndexLevelReplicationTestCase {

    private static final String TARGET_NODE = "target-node";

    /** Above any version the harness itself applies, since the tracker ignores a state it has already seen. */
    private final AtomicLong clusterStateVersion = new AtomicLong(1_000_000);

    public void testTheTargetsNodeBuildsTheSourcesEntryExactly() throws Exception {
        try (ReplicationGroup group = createGroup(0)) {
            group.startAll();
            ShardRouting relocating = group.getPrimary().routingEntry().relocate(TARGET_NODE, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

            ShardRouting built = RelocatingEntry.of(
                relocating.shardId(),
                relocating.currentNodeId(),
                TARGET_NODE,
                relocating.allocationId().getId(),
                relocating.allocationId().getRelocationId()
            );

            assertEquals(relocating, built);
            assertSame("a plain ShardRouting, not the subclass that built it", ShardRouting.class, built.getClass());
            assertEquals(relocating.getTargetRelocatingShard(), built.getTargetRelocatingShard());
        }
    }

    public void testATargetBuiltFromThePublishedIdsTakesOverAndKeepsTheReplica() throws Exception {
        try (ReplicationGroup group = createGroup(1)) {
            group.startAll();
            group.indexDocs(randomIntBetween(1, 10));
            IndexShard source = group.getPrimary();
            IndexShard replica = group.getReplicas().get(0);
            String sourceNode = source.routingEntry().currentNodeId();

            // The source's node: its primary relocates, and the table and set are the ones CoordinatorState builds.
            ShardRouting relocating = source.routingEntry().relocate(TARGET_NODE, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            IndexShardRoutingTable sourceTable = table(relocating, replica.routingEntry());
            apply(source, relocating, sourceTable, CoordinatorState.inSyncOf(sourceTable, sourceNode, IndicesServiceTrackedInSync.of(source)));

            // The target's node: the same entry, from the two ids the source's node published.
            ShardRouting pair = RelocatingEntry.of(
                relocating.shardId(),
                sourceNode,
                TARGET_NODE,
                relocating.allocationId().getId(),
                relocating.allocationId().getRelocationId()
            );
            // Under the source's index metadata, which is what the target's node fabricates too: the same primary term.
            // A shard made with metadata of its own gets a random term, below the source's operations' term, and replaying
            // them during the recovery trips an assertion on a recovery thread, which leaves the recovery hanging.
            IndexShard target = newShard(
                pair.getTargetRelocatingShard(),
                source.indexSettings().getIndexMetadata(),
                null,
                new EngineBackedIndexerFactory(new InternalEngineFactory())
            );
            try {
                recoverUnstartedReplica(
                    target,
                    source,
                    (shard, node) -> new RecoveryTarget(shard, node, recoveryListener, threadPool),
                    true,
                    CoordinatorState.inSyncOf(sourceTable, sourceNode, IndicesServiceTrackedInSync.of(source)),
                    sourceTable,
                    shards -> java.util.List.of()
                );

                assertTrue("the handoff made the target the primary", target.isPrimaryMode());
                assertFalse(source.isPrimaryMode());

                // The target's node's table after the handoff, while the assignment still names the relocation. Twice, as
                // every pass applies it again.
                IndexShardRoutingTable targetTable = table(pair, replica.routingEntry());
                Set<String> inSync = CoordinatorState.inSyncOf(targetTable, TARGET_NODE, IndicesServiceTrackedInSync.of(target));
                assertNotNull("the target is the primary now, so its set is the one to publish", inSync);
                assertTrue(inSync.toString(), inSync.contains(replica.routingEntry().allocationId().getId()));
                apply(target, target.routingEntry(), targetTable, inSync);
                apply(target, target.routingEntry(), targetTable, inSync);
                assertTrue(
                    "the replica is still written to: " + target.getReplicationGroup(),
                    target.getReplicationGroup().getReplicationTargets().contains(replica.routingEntry())
                );

                // Completed on the target's node, as LocalShardStateAction completes it: the started target replaces the
                // source's entry.
                ShardRouting started = target.routingEntry().moveToStarted();
                IndexShardRoutingTable completed = table(started, replica.routingEntry());
                Set<String> afterwards = CoordinatorState.inSyncOf(completed, TARGET_NODE, IndicesServiceTrackedInSync.of(target));
                apply(target, started, completed, afterwards);
                apply(target, started, completed, afterwards);
                assertEquals(Set.of(started.allocationId().getId(), replica.routingEntry().allocationId().getId()), afterwards);
                assertTrue(target.getReplicationGroup().getReplicationTargets().contains(replica.routingEntry()));
                assertDocCount(target, getShardDocUIDs(source).size());
            } finally {
                closeShards(target);
            }
        }
    }

    private static IndexShardRoutingTable table(ShardRouting primary, ShardRouting replica) {
        return new IndexShardRoutingTable.Builder(primary.shardId()).addShard(primary).addShard(replica).build();
    }

    private void apply(IndexShard shard, ShardRouting routing, IndexShardRoutingTable table, Set<String> inSync) throws IOException {
        shard.updateShardState(
            routing,
            shard.getPendingPrimaryTerm(),
            null,
            clusterStateVersion.incrementAndGet(),
            inSync,
            table,
            IndexShardTestUtils.getFakeDiscoveryNodes(table.getShards())
        );
    }
}
