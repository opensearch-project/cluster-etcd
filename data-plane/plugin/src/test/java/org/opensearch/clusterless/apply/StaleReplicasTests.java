/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.IndexStatus;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.ShardState;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * When a started document-replication replica is rebuilt because its primary no longer holds it in sync -- and, as much,
 * when it is not.
 *
 * <p>Most of these are the second half: a replica that has just started can read its primary's set from before it was in
 * it, and rebuilding then restarts a healthy copy, possibly over and over. So absence alone is never enough.
 */
public class StaleReplicasTests extends OpenSearchTestCase {

    private static final String LOCAL = "pv1sDMfZRxSTMiW5VrbN9w";
    private static final String PRIMARY_NODE = "AAAAAAAAAAAAAAAAAAAAAA";
    private static final String OTHER_NODE = "BBBBBBBBBBBBBBBBBBBBBB";
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final String PRIMARY = "aid-primary";
    private static final String PROMOTED = "aid-promoted";

    private final StaleReplicas stale = new StaleReplicas(LOCAL);

    public void testAReplicaThatHasJustStartedIsNotRebuiltForASetFromBeforeItJoined() {
        ShardRouting recovering = recovering();
        assertEquals(Set.of(), pass(recovering, primaryAt(PRIMARY, List.of(PRIMARY))));
        ShardRouting started = recovering.moveToStarted();

        for (int i = 0; i < StaleReplicas.PASSES_TO_JOIN; i++) {
            assertEquals("pass " + i, Set.of(), pass(started, primaryAt(PRIMARY, List.of(PRIMARY))));
        }
    }

    public void testAReplicaInTheSetIsNeverRebuilt() {
        ShardRouting started = startedAfterRecoveringFrom(PRIMARY);
        String me = started.allocationId().getId();

        for (int i = 0; i < 3 * StaleReplicas.PASSES_TO_JOIN; i++) {
            assertEquals(Set.of(), pass(started, primaryAt(PRIMARY, List.of(me, PRIMARY))));
        }
    }

    public void testAReplicaDroppedFromTheSetIsRebuilt() {
        // What a failed write to it does: the primary stops tracking it, and its published set loses it.
        ShardRouting started = startedAfterRecoveringFrom(PRIMARY);
        String me = started.allocationId().getId();
        assertEquals(Set.of(), pass(started, primaryAt(PRIMARY, List.of(me, PRIMARY))));

        assertEquals(Set.of(me), pass(started, primaryAt(PRIMARY, List.of(PRIMARY))));
    }

    public void testAReplicaWhosePrimaryChangedIsRebuiltAtOnce() {
        // A promoted primary has the id it had as a replica, and knows nothing of the others; a restarted one has a new id.
        // On the first pass it is seen, and without waiting: the set is current, because it is not the one recovered from.
        ShardRouting started = startedAfterRecoveringFrom(PRIMARY);
        String me = started.allocationId().getId();

        assertEquals(Set.of(me), pass(started, primaryAt(PROMOTED, List.of(PROMOTED))));
    }

    public void testAReplicaANewPrimaryHoldsIsKept() {
        ShardRouting started = startedAfterRecoveringFrom(PRIMARY);
        String me = started.allocationId().getId();

        for (int i = 0; i < 3 * StaleReplicas.PASSES_TO_JOIN; i++) {
            assertEquals(Set.of(), pass(started, primaryAt(PROMOTED, List.of(me, PROMOTED))));
        }
    }

    public void testAReplicaThatNeverJoinsIsRebuiltOnlyAfterTheWait() {
        // A primary that changed while this copy was still recovering is the case this covers.
        ShardRouting started = startedAfterRecoveringFrom(PRIMARY);
        String me = started.allocationId().getId();

        for (int i = 0; i < StaleReplicas.PASSES_TO_JOIN; i++) {
            assertEquals("pass " + i, Set.of(), pass(started, primaryAt(PRIMARY, List.of(PRIMARY))));
        }
        assertEquals(Set.of(me), pass(started, primaryAt(PRIMARY, List.of(PRIMARY))));
    }

    public void testAPrimaryThatPublishesNoSetRebuildsNothing() {
        // An older primary, or one whose tracker cannot say. Absent is not empty.
        ShardRouting started = startedAfterRecoveringFrom(PRIMARY);

        for (int i = 0; i < 3 * StaleReplicas.PASSES_TO_JOIN; i++) {
            assertEquals(Set.of(), pass(started, primaryAt(PROMOTED, null)));
        }
    }

    public void testAPrimaryThatIsNotStartedIsNotJudgedBy() {
        ShardRouting started = startedAfterRecoveringFrom(PRIMARY);
        IndexStatus.Copy initializing = new IndexStatus.Copy(NodeId.of(OTHER_NODE), ShardRole.PRIMARY, ShardState.INITIALIZING, false, PROMOTED, List.of(PROMOTED));

        assertEquals(Set.of(), pass(started, initializing));
    }

    public void testASegmentReplicatedIndexIsNotThisStepsBusiness() {
        ShardRouting started = startedAfterRecoveringFrom(PRIMARY);

        assertEquals(Set.of(), stale.toRebuild(desired(config("SEGMENT")), live(started), view(config("SEGMENT"), started, primaryAt(PROMOTED, List.of(PROMOTED)))));
    }

    public void testARebuiltCopyStartsAfresh() {
        // Its new id is recovering from the primary that dropped the old one, and that is the one it is judged against.
        ShardRouting started = startedAfterRecoveringFrom(PRIMARY);
        String me = started.allocationId().getId();
        pass(started, primaryAt(PRIMARY, List.of(me, PRIMARY)));
        assertEquals(Set.of(me), pass(started, primaryAt(PRIMARY, List.of(PRIMARY))));

        ShardRouting again = recovering();
        assertEquals(Set.of(), pass(again, primaryAt(PRIMARY, List.of(PRIMARY))));
        assertEquals("a set from before it rejoined is not held against it", Set.of(), pass(again.moveToStarted(), primaryAt(PRIMARY, List.of(PRIMARY))));
    }

    // ---------------------------------------------------------------- fixtures

    private ShardRouting startedAfterRecoveringFrom(String primary) {
        ShardRouting recovering = recovering();
        pass(recovering, primaryAt(primary, List.of(primary)));
        return recovering.moveToStarted();
    }

    private Set<String> pass(ShardRouting mine, IndexStatus.Copy primary) {
        return stale.toRebuild(desired(config("DOCUMENT")), live(mine), view(config("DOCUMENT"), mine, primary));
    }

    private static IndexStatus.Copy primaryAt(String allocationId, List<String> inSync) {
        return new IndexStatus.Copy(NodeId.of(PRIMARY_NODE), ShardRole.PRIMARY, ShardState.STARTED, true, allocationId, inSync);
    }

    private static ShardRouting recovering() {
        return ShardRouting.newUnassigned(
            new ShardId(index(), 0),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "test")
        ).initialize(LOCAL, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    private static Index index() {
        return new Index(IndexNaming.indexName(ACME, UUID, "my-index"), UUID);
    }

    private static ClusterState live(ShardRouting mine) {
        IndexRoutingTable routing = IndexRoutingTable.builder(index()).addShard(mine).build();
        return ClusterState.builder(new ClusterName("clusterless")).routingTable(RoutingTable.builder().add(routing).build()).build();
    }

    private static DesiredState desired(IndexConfig config) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        return new DesiredState(42, Map.of(key, new DesiredState.DesiredIndex(key, config, Map.of(0, ShardRole.REPLICA))), List.of(), List.of());
    }

    private static CoordinatorView view(IndexConfig config, ShardRouting mine, IndexStatus.Copy primary) {
        List<IndexStatus.Copy> copies = new ArrayList<>();
        copies.add(primary);
        copies.add(
            new IndexStatus.Copy(
                NodeId.of(LOCAL),
                ShardRole.REPLICA,
                mine.started() ? ShardState.STARTED : ShardState.INITIALIZING,
                mine.started(),
                mine.allocationId().getId(),
                null
            )
        );
        IndexStatus status = new IndexStatus("my-index", UUID, 3041, 1, 1, IndexStatus.Replication.from(config.replication()), Map.of(0, copies));
        return new CoordinatorView(
            List.of(new CoordinatorView.RoutedIndex(ACME, config, status, Set.of(0), Map.of(0, ShardRole.REPLICA))),
            Map.of(),
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
}
