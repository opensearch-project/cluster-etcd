package org.opensearch.supervisor.integ;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.core.Supervisor;
import org.opensearch.supervisor.fakenode.FakeNode;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.status.ClusterStatus;
import org.opensearch.supervisor.model.status.HealthStatus;
import org.opensearch.supervisor.model.status.IndexStatus;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The whole thing, against a real store: desired state in, shards placed, health published.
 *
 * <p>What this adds over the in-memory end-to-end tests is not the logic — that is covered far more thoroughly
 * there, and faster. It is that the logic has never run against opaque revisions assigned by something other
 * than a counter in the same process, nor against a clock it does not control. Either could hide a defect that no
 * amount of unit testing reaches.
 */
abstract class AbstractConvergenceIT extends StoreIntegBase {

    private static final NodeId NODE_A = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId NODE_B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");

    @Test
    void anIndexIsPlacedAcrossRealNodesAndReportsItselfGreen() {
        definePool(POOL_A);
        registerTenant(ACME, POOL_A);
        createIndex(ACME, INDEX_NAME, INDEX_UUID, 2, 1);

        FakeNode a = node(NODE_A, POOL_A);
        FakeNode b = node(NODE_B, POOL_A);
        a.start();
        b.start();
        Supervisor supervisor = supervisor("s1");

        // Four copies: two shards, one replica each, and the pool has exactly two nodes to hold them.
        eventually("all four shard copies to be placed and started", () -> {
            supervisor.tick();
            a.tick();
            b.tick();
            assertEquals(4, a.inventory().copyCount() + b.inventory().copyCount(),
                "held: A=" + a.inventory().copyCount() + " B=" + b.inventory().copyCount());
        });

        // Health is what an operator reads, and it is derived from placements rather than stored, so this also
        // checks that the projections agree with the inventories.
        eventually("the index to report green", () -> {
            supervisor.tick();
            a.tick();
            b.tick();
            IndexStatus status = IndexStatus.from(
                store.get(Keys.indexStatus(ACME, INDEX_UUID)).orElseThrow().value());
            assertEquals(HealthStatus.GREEN, status.status(),
                "status=" + status.status() + " active=" + status.activeShards()
                    + " unassigned=" + status.unassignedShards());
        });

        ClusterStatus cluster = ClusterStatus.from(
            store.get(Keys.clusterStatus(ACME)).orElseThrow().value());
        assertEquals(HealthStatus.GREEN, cluster.status());
        assertEquals(2, cluster.numberOfDataNodes());
        assertEquals(4, cluster.activeShards());
        assertEquals(2, cluster.activePrimaryShards());

        // And the pool's convergence document exists, which is the answer to "is this getting better?".
        String convergence = new String(
            store.get(Keys.poolConvergence(POOL_A)).orElseThrow().value(), StandardCharsets.UTF_8);
        assertTrue(convergence.contains(POOL_A.value()), convergence);

        // A converged pool should then stop changing anything. Checked against the assignment versions rather
        // than the diff, because a supervisor that keeps rewriting assignments is the failure that matters: it
        // would keep the data plane permanently busy applying identical desired state.
        long settled = assignmentVersions(a, b);
        pump(supervisor, List.of(a, b), Duration.ofSeconds(6));
        assertEquals(settled, assignmentVersions(a, b),
            "a converged pool must stop rewriting assignments");
    }

    @Test
    void aNodeThatGoesSilentHasItsCopiesRebuiltElsewhere() {
        // Liveness against a real clock, which is the one thing every other liveness test could not do: they
        // advance a MutableClock, so revision-silence has never actually had to accumulate on its own.
        definePool(POOL_A);
        registerTenant(ACME, POOL_A);
        createIndex(ACME, INDEX_NAME, INDEX_UUID, 1, 1);

        FakeNode a = node(NODE_A, POOL_A);
        FakeNode b = node(NODE_B, POOL_A);
        a.start();
        b.start();
        Supervisor supervisor = supervisor("s1");

        eventually("both copies of the shard to be placed", () -> {
            supervisor.tick();
            a.tick();
            b.tick();
            assertEquals(2, a.inventory().copyCount() + b.inventory().copyCount());
        });

        // A third node arrives, then B stops heartbeating entirely -- not a clean shutdown, just silence, which
        // is what a crashed or partitioned node looks like.
        NodeId nodeC = NodeId.of("nodeCCCCCCCCCCCCCCCCCC");
        FakeNode c = node(nodeC, POOL_A);
        c.start();

        // With StoreIntegBase.LIVENESS this node's declared 1s interval beats the floor's implied 667ms, so lost
        // is 6x1s = 6s rather than the 67s the production defaults impose. The mechanism is identical -- revision
        // silence accumulating past a threshold -- and only the threshold is smaller.
        eventually("the silent node's copy to be rebuilt on the survivor", Duration.ofSeconds(90), () -> {
            supervisor.tick();
            a.tick();
            c.tick();
            assertEquals(2, a.inventory().copyCount() + c.inventory().copyCount(),
                "A=" + a.inventory().copyCount() + " C=" + c.inventory().copyCount());
        });
    }

    private long assignmentVersions(FakeNode... nodes) {
        long total = 0;
        for (FakeNode node : nodes) {
            total += store.get(Keys.nodeAssignment(node.nodeId()))
                .map(entry -> org.opensearch.supervisor.model.node.NodeJson.readAssignment(entry.value()).version())
                .orElse(0L);
        }
        return total;
    }
}
