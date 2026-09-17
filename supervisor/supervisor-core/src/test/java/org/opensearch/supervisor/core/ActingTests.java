package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.fakenode.FakeNode;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.task.TaskState;
import org.opensearch.supervisor.model.node.ResetMode;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.plan.Planner;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.status.IndexStatus;
import org.opensearch.supervisor.model.status.HealthStatus;
import org.opensearch.supervisor.model.status.Replication;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Acting, end to end: a real supervisor places a shard and a real {@link FakeNode} picks it up and serves it.
 *
 * <p>Milestone 5's deliverable, and the first time anything in this service writes something a data node will
 * act on. The stale-writer defences from {@code DESIGN.md} §12 are exercised individually here, because each
 * of them is invisible when working and expensive when absent.
 */
class ActingTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final NodeId NODE = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId NODE_B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final Duration TTL = Duration.ofSeconds(15);

    private InMemoryKeyValueStore store;
    private MutableClock clock;

    private static final class MutableClock extends Clock {
        private Instant now = Instant.ofEpochMilli(1_000_000);

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        clock = new MutableClock();
        store.put(Keys.poolDefinition(POOL), new PoolDefinition(POOL, PoolKind.SHARED, false, null, null, java.util.List.of(), false).toBytes());
        store.put(Keys.poolBinding(ACME), PoolBinding.to(POOL).toBytes());
        store.put(Keys.tenantRegistration(ACME),
            "{\"tenant\":\"acme\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * A data node in a pool that serves segment-replication indices, which means it has a remote store.
     *
     * <p>Not incidental detail: RemoteStoreDecider refuses a search-only copy on a node declaring no segment
     * repository, because such a copy starts, reports itself started and never receives a segment. Fixtures that
     * omitted it were modelling a pool that could not actually serve the replicas they asserted about.
     */
    private static Map<String, String> remoteStoreNode(String zone) {
        return Map.of("zone", zone,
            org.opensearch.supervisor.model.allocate.RemoteStoreDecider.SEGMENT_REPOSITORY_ATTRIBUTE, "segments");
    }

    private FakeNode zoned(NodeId id, String zone) {
        return new FakeNode(store, new Registration(
            id, "placeholder", id.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0." + id.value().charAt(4), 9300, "3.7.0", remoteStoreNode(zone), 5000, 0L
        ), clock, "eph-" + id.value());
    }

    private FakeNode node(NodeId id) {
        return new FakeNode(store, new Registration(
            id, "placeholder", id.value(), POOL, EnumSet.of(NodeRole.DATA),
            // Distinct per node: the same-shard decider treats one address as one failure domain, so a fixture
            // sharing addresses would make every second copy unplaceable.
            "10.0.0." + id.value().charAt(4), 9300, "3.7.0", remoteStoreNode("z1"), 5000, 0L
        ), clock, "eph-" + id.value());
    }

    private Supervisor acting(String id) {
        return new Supervisor(store, clock, id, Set.of(), 1.0, TTL, Balancer.Config.defaults(),
            new Planner.Policy(200, 5, 2), false);
    }

    private void createIndex(int shards, int replicas) {
        store.put(Keys.indicesPrefix(ACME) + "my-index", IndexConfig.toDocument(
            "my-index", UUID, true, IndexConfig.settingsOf(shards, replicas, Replication.SEGMENT)
        ));
    }

    private void advance() {
        clock.advance(Duration.ofSeconds(2));
    }

    // ---------------------------------------------------------------- the deliverable

    @Test
    void aSupervisorPlacesAPrimaryAndTheNodeServesIt() {
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        createIndex(1, 0);
        Supervisor supervisor = acting("s1");

        // Take the lease, notice the missing primary, wait for the finding to hold, then act.
        for (int pass = 0; pass < 5; pass++) {
            supervisor.tick();
            data.tick();
            advance();
        }

        Assignment assignment = NodeJson.readAssignment(store.get(Keys.nodeAssignment(NODE)).orElseThrow().value());
        assertEquals(ShardRole.PRIMARY,
            assignment.tenants().get(ACME).indices().get(UUID).shards().get(0));
        assertEquals(POOL, assignment.pool());
        assertTrue(assignment.epoch() >= 1, "the lease epoch must be stamped in");

        assertEquals(ShardState.STARTED,
            data.inventory().tenants().get(ACME).indices().get(UUID).shards().get(0).state());

        Diff diff = supervisor.lastDiffOf(POOL);
        assertTrue(diff.isEmpty(), () -> "expected convergence, got " + diff.findings());
    }

    @Test
    void everyShardOfAMultiShardIndexGetsPlaced() {
        FakeNode a = node(NODE);
        FakeNode b = node(NODE_B);
        a.start();
        b.start();
        a.tick();
        b.tick();
        createIndex(4, 0);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 8; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            advance();
        }

        assertEquals(4, a.inventory().copyCount() + b.inventory().copyCount());
        assertTrue(supervisor.lastDiffOf(POOL).isEmpty());
        assertTrue(a.inventory().copyCount() > 0 && b.inventory().copyCount() > 0, "should have spread");
    }

    @Test
    void aSurplusCopyIsNeverCreatedWhileANodeIsStillInitializing() {
        // The failure this design most has to avoid: acting each pass on a finding that has not cleared yet,
        // placing the same shard on a different node every time. The assignment the supervisor already wrote
        // is what prevents it, read back out of the store rather than remembered.
        FakeNode a = node(NODE);
        FakeNode b = node(NODE_B);
        a.start();
        b.start();
        a.tick();
        b.tick();
        createIndex(1, 0);
        Supervisor supervisor = acting("s1");

        // Ten supervisor passes with the node ticking only rarely, so the finding stays visible for a while.
        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            advance();
        }
        a.tick();
        b.tick();

        assertEquals(1, a.inventory().copyCount() + b.inventory().copyCount(),
            "exactly one copy, not one per pass");
    }

    @Test
    void aReadOnlySupervisorStillWritesNothing() {
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        createIndex(2, 1);

        Supervisor readOnly = new Supervisor(store, clock, "s1", Set.of(), 1.0, TTL,
            Balancer.Config.defaults(), Planner.Policy.defaults(), true);
        for (int pass = 0; pass < 8; pass++) {
            readOnly.tick();
            data.tick();
            advance();
        }

        assertTrue(readOnly.isReadOnly());
        assertTrue(store.scanPrefix(Keys.NODE_ASSIGNMENT_PREFIX).isEmpty());
        assertFalse(readOnly.lastDiffOf(POOL).isEmpty(), "and it still reports what is wrong");
    }

    /**
     * Observes twice so the finding is old enough to act on, and returns the plan with the observation it was
     * computed from. Ages advance with the observation's pass number, so diffing one observation twice proves
     * nothing -- a mistake worth noting because it silently produces an empty plan.
     */
    private record Planned(org.opensearch.supervisor.model.observe.Observation observation,
                           org.opensearch.supervisor.model.plan.Plan plan) {}

    private Planned planTwice() {
        Observer observer = new Observer(store, clock, new LivenessTracker(clock.instant()));
        var ages = new org.opensearch.supervisor.model.diff.FindingAges();
        var observation = observer.observe(POOL, Pools.read(store), Set.of(ACME));
        var diff = org.opensearch.supervisor.model.diff.Differ.diff(observation, ages);
        advance();
        observation = observer.observe(POOL, Pools.read(store), Set.of(ACME));
        diff = org.opensearch.supervisor.model.diff.Differ.diff(observation, ages);
        return new Planned(observation, Planner.plan(observation, diff, 1, Planner.Policy.defaults()));
    }

    // ---------------------------------------------------------------- the stale-writer defences

    @Test
    void aPlanOlderThanItsBoundIsAbandonedRatherThanApplied() {
        // Defence one, and the one that catches almost everything: it fires for an ordinary GC pause as much
        // as for a lost lease, and needs no coordination with anybody.
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        createIndex(1, 0);

        // Build a plan, then let far too much time pass before applying it.
        Planned planned = planTwice();
        assertFalse(planned.plan().isEmpty());

        clock.advance(Duration.ofMinutes(1));
        Applier.Outcome outcome = new Applier(store, clock, Duration.ofSeconds(5))
            .apply(planned.plan(), planned.observation(), () -> true);

        assertTrue(outcome.abandoned());
        assertEquals(0, outcome.applied());
        assertTrue(outcome.abandonedBecause().contains("old"));
    }

    @Test
    void aPlanIsAbandonedIfTheLeaseHasGoneBeforeTheWrite() {
        // Defence two. The lease is a supplier rather than a boolean precisely so it is evaluated here, at
        // the last possible moment, rather than whenever the caller happened to look.
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        createIndex(1, 0);

        Planned planned = planTwice();
        assertFalse(planned.plan().isEmpty());

        Applier.Outcome outcome = new Applier(store, clock)
            .apply(planned.plan(), planned.observation(), () -> false);

        assertTrue(outcome.abandoned());
        assertTrue(store.get(Keys.nodeAssignment(NODE)).isEmpty());
    }

    @Test
    void aWriteIsRejectedIfSomebodyElseChangedTheAssignmentSinceItWasObserved() {
        // Defence three, and the one that holds when the other two miss. R6.2: a stale supervisor loses
        // individual writes rather than corrupting shared state.
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        createIndex(1, 0);

        Planned planned = planTwice();
        assertFalse(planned.plan().isEmpty());

        // Another supervisor gets there first, from a newer view than this one had.
        store.put(Keys.nodeAssignment(NODE), NodeJson.write(new Assignment(
            99, POOL, 42, NODE, clock.millis(), Map.of()
        )));

        Applier.Outcome outcome = new Applier(store, clock)
            .apply(planned.plan(), planned.observation(), () -> true);

        assertEquals(0, outcome.applied());
        assertEquals(1, outcome.rejected());
        assertEquals(99, NodeJson.readAssignment(store.get(Keys.nodeAssignment(NODE)).orElseThrow().value()).version(),
            "the other supervisor's write must survive intact");
    }

    @Test
    void anAssignmentIsMergedRatherThanReplaced() {
        // An assignment is a complete desired set, so a write that dropped its existing contents would not
        // merely fail to add a shard -- it would tell the node to discard everything else it holds.
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        createIndex(3, 0);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            data.tick();
            advance();
        }

        Assignment assignment = NodeJson.readAssignment(store.get(Keys.nodeAssignment(NODE)).orElseThrow().value());
        assertEquals(3, assignment.tenants().get(ACME).indices().get(UUID).shards().size(),
            "all three shards should be in one assignment: " + assignment);
        assertEquals(3, data.inventory().copyCount());
    }

    @Test
    void aSecondTenantsShardsAppearAlongsideTheFirstsRatherThanInsteadOfThem() {
        TenantId globex = TenantId.of("globex");
        String globexUuid = "globexIndexUuidAAAAAA";
        store.put(Keys.poolBinding(globex), PoolBinding.to(POOL).toBytes());
        store.put(Keys.tenantRegistration(globex),
            "{\"tenant\":\"globex\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        store.put(Keys.indicesPrefix(globex) + "other", IndexConfig.toDocument(
            "other", globexUuid, true, IndexConfig.settingsOf(1, 0, Replication.SEGMENT)
        ));
        createIndex(1, 0);

        FakeNode data = node(NODE);
        data.start();
        data.tick();
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            data.tick();
            advance();
        }

        Assignment assignment = NodeJson.readAssignment(store.get(Keys.nodeAssignment(NODE)).orElseThrow().value());
        assertEquals(Set.of(ACME, globex), assignment.tenants().keySet());
        assertEquals(2, data.inventory().copyCount());
    }

    // ---------------------------------------------------------------- rebalancing, end to end

    private static final NodeId NODE_C = NodeId.of("nodeCCCCCCCCCCCCCCCCCC");

    /** One big shard and one small one, pinned identically everywhere, since a shard is one size. */
    private FakeNode skewed(NodeId id) {
        return node(id).withShardSize(0, 20L * 1024 * 1024 * 1024).withShardSize(1, 50L * 1024 * 1024);
    }

    @Test
    void anEmptyNodeJoiningALoadedPoolIsGivenTheBiggestShardAndTheOldCopyIsDropped() {
        // The whole feature, end to end. Two assertions carry it: the new node ends up holding one of the big
        // shards rather than one of the small ones, which is the size signal doing its job; and the pool ends up
        // with exactly the copies it wanted, not one more, which means both halves ran -- the rebalancer built a
        // copy on the new node and the reaper independently decided the original was now surplus.
        FakeNode a = skewed(NODE);
        FakeNode b = skewed(NODE_B);
        a.start();
        b.start();
        createIndex(4, 1);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 12; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            advance();
        }
        assertEquals(8, a.inventory().copyCount() + b.inventory().copyCount(), "all eight copies placed first");

        // A third node arrives holding nothing at all.
        FakeNode c = skewed(NODE_C);
        c.start();
        for (int pass = 0; pass < 40; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            c.tick();
            advance();
        }

        assertTrue(c.inventory().copyCount() > 0, "the new node should have been given work");
        assertEquals(8, a.inventory().copyCount() + b.inventory().copyCount() + c.inventory().copyCount(),
            "and every surplus copy a move created must have been dropped again");

        var landed = c.inventory().tenants().get(ACME).indices().get(UUID).shards();
        assertTrue(landed.keySet().stream().anyMatch(shard -> shard <= 1),
            "a 20 GiB shard is worth moving and a 50 MiB one is not, so the new node should hold at least one"
                + " of shards 0 or 1: " + landed);
    }

    @Test
    void aPoolWhoseImbalanceIsAllInPrimariesCannotBeRelievedAndTheDocumentSaysWhy() {
        // The known gap, asserted rather than described, so that the day primary movement lands this test fails
        // and says so. With no replicas every copy is a primary, so the new node can be given nothing at all.
        FakeNode a = skewed(NODE);
        a.start();
        createIndex(4, 0);
        Supervisor supervisor = acting("s1");
        for (int pass = 0; pass < 12; pass++) {
            supervisor.tick();
            a.tick();
            advance();
        }
        assertEquals(4, a.inventory().copyCount());

        FakeNode c = skewed(NODE_C);
        c.start();
        for (int pass = 0; pass < 20; pass++) {
            supervisor.tick();
            a.tick();
            c.tick();
            advance();
        }

        assertEquals(0, c.inventory().copyCount(), "nothing can move, because every copy is a primary");
        String convergence = new String(store.get(Keys.poolConvergence(POOL)).orElseThrow().value(),
            java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(convergence.contains("moving a primary is not implemented"), convergence);
        assertTrue(convergence.contains("spread"), convergence);
    }

    @Test
    void redundancyNeverDipsWhileAShardIsBeingMoved() {
        // The property the whole add-then-drop shape exists for. Checked every single tick rather than at the
        // end, because "it converged" would hide a pass in which the shard briefly had one copy.
        FakeNode a = skewed(NODE);
        FakeNode b = skewed(NODE_B);
        a.start();
        b.start();
        createIndex(2, 1);
        Supervisor supervisor = acting("s1");
        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            advance();
        }

        FakeNode c = skewed(NODE_C);
        c.start();
        for (int pass = 0; pass < 20; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            c.tick();
            advance();
            for (int shard = 0; shard < 2; shard++) {
                assertTrue(startedCopiesOf(shard, a, b, c) >= 2,
                    "shard " + shard + " dropped below two started copies on pass " + pass);
            }
        }
    }

    @Test
    void aBalancedPoolStopsMovingThingsAndSaysTheSpreadOutLoud() {
        // Termination, from the outside. If the threshold rule were wrong, this loop would move a shard back and
        // forth for ever and the assignment versions would climb without end.
        FakeNode a = skewed(NODE);
        FakeNode b = skewed(NODE_B);
        FakeNode c = skewed(NODE_C);
        a.start();
        b.start();
        c.start();
        createIndex(3, 1);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 30; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            c.tick();
            advance();
        }
        long settledVersion = assignmentVersionOf(NODE) + assignmentVersionOf(NODE_B) + assignmentVersionOf(NODE_C);

        for (int pass = 0; pass < 15; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            c.tick();
            advance();
        }

        assertEquals(settledVersion,
            assignmentVersionOf(NODE) + assignmentVersionOf(NODE_B) + assignmentVersionOf(NODE_C),
            "a settled pool must stop rewriting assignments");
        assertEquals(6, a.inventory().copyCount() + b.inventory().copyCount() + c.inventory().copyCount());
    }

    @Test
    void anOperatorCanStopRebalancingWithoutStoppingRecovery() {
        // The reason the two settings are separate. Rebalancing off, and yet a brand new index is still placed.
        FakeNode a = skewed(NODE);
        FakeNode b = skewed(NODE_B);
        a.start();
        b.start();
        createIndex(2, 1);
        store.put(Keys.clusterSettings(ACME),
            ("{\"persistent\":{\"" + org.opensearch.supervisor.model.settings.ClusterSettings.REBALANCE_ENABLE
                + "\":\"none\"},\"transient\":{}}").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            advance();
        }
        assertEquals(4, a.inventory().copyCount() + b.inventory().copyCount(),
            "recovery is untouched by the rebalance brake");

        FakeNode c = skewed(NODE_C);
        c.start();
        for (int pass = 0; pass < 20; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            c.tick();
            advance();
        }

        assertEquals(0, c.inventory().copyCount(), "but nothing may be moved onto the new node");
        // And the reason is readable. It belongs in the pool's convergence document rather than a tenant's diff:
        // the diff here is legitimately empty -- nothing is wrong -- and an earlier version of this had nowhere
        // to put a pool-level sentence, so the explanation was computed every pass and never written down.
        String convergence = new String(store.get(Keys.poolConvergence(POOL)).orElseThrow().value(),
            java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(convergence.contains("rebalancing is set to [none]"), convergence);
    }

    private long assignmentVersionOf(NodeId node) {
        return store.get(Keys.nodeAssignment(node))
            .map(entry -> NodeJson.readAssignment(entry.value()).version())
            .orElse(0L);
    }

    private int startedCopiesOf(int shard, FakeNode... nodes) {
        int started = 0;
        for (FakeNode node : nodes) {
            var tenant = node.inventory().tenants().get(ACME);
            if (tenant == null) {
                continue;
            }
            var index = tenant.indices().get(UUID);
            if (index == null) {
                continue;
            }
            var copy = index.shards().get(shard);
            if (copy != null && copy.state() == org.opensearch.supervisor.model.node.ShardState.STARTED) {
                started++;
            }
        }
        return started;
    }

    // ---------------------------------------------------------------- the operator's brake

    @Test
    void anOperatorCanStopAllocationAndTheDiffSaysWhy() {
        // The whole point of a brake is that pulling it is visible. An operator who disables allocation and
        // then sees "shard 0 has no primary" with no further explanation has been given a mystery of their own
        // making, which is the worst kind.
        FakeNode data = node(NODE);
        data.start();
        createIndex(1, 0);
        store.put(Keys.clusterSettings(ACME),
            ("{\"persistent\":{\"" + org.opensearch.supervisor.model.settings.ClusterSettings.ALLOCATION_ENABLE
                + "\":\"none\"},\"transient\":{}}").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 8; pass++) {
            supervisor.tick();
            data.tick();
            advance();
        }

        assertEquals(0, data.inventory().copyCount(), "nothing should have been placed");
        assertTrue(store.scanPrefix(Keys.NODE_ASSIGNMENT_PREFIX).isEmpty());

        // And the reason reaches the document an operator reads, not just the log.
        String diff = new String(store.get(Keys.tenantDiff(ACME)).orElseThrow().value(),
            java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(diff.contains("enable_allocation"), diff);
        assertTrue(diff.contains("somebody asked it to stop"), diff);
    }

    @Test
    void releasingTheBrakeLetsTheWorkProceed() {
        FakeNode data = node(NODE);
        data.start();
        createIndex(1, 0);
        store.put(Keys.clusterSettings(ACME),
            ("{\"persistent\":{\"" + org.opensearch.supervisor.model.settings.ClusterSettings.ALLOCATION_ENABLE
                + "\":\"none\"},\"transient\":{}}").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 6; pass++) {
            supervisor.tick();
            data.tick();
            advance();
        }
        assertEquals(0, data.inventory().copyCount());

        store.delete(Keys.clusterSettings(ACME));
        for (int pass = 0; pass < 6; pass++) {
            supervisor.tick();
            data.tick();
            advance();
        }

        assertEquals(1, data.inventory().copyCount());
        assertTrue(supervisor.lastDiffOf(POOL).isEmpty());
    }

    @Test
    void anUnplaceableShardCarriesTheAllocatorsReasonIntoTheProjectedDiff() {
        // The sentence the deciders exist to produce. An earlier version computed it and threw it away.
        FakeNode data = node(NODE);
        data.start();
        createIndex(1, 1);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            data.tick();
            advance();
        }

        String diff = new String(store.get(Keys.tenantDiff(ACME)).orElseThrow().value(),
            java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(diff.contains("unplaceable_because"), diff);
        assertTrue(diff.contains("same_shard"), "the one other node already holds the primary: " + diff);
    }

    // ---------------------------------------------------------------- replicas and awareness

    @Test
    void anIndexWithAReplicaConvergesWithItsCopiesOnDifferentNodes() {
        FakeNode a = node(NODE);
        FakeNode b = node(NODE_B);
        a.start();
        b.start();
        createIndex(1, 1);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 12; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            advance();
        }

        assertEquals(1, a.inventory().copyCount());
        assertEquals(1, b.inventory().copyCount());
        assertTrue(supervisor.lastDiffOf(POOL).isEmpty(),
            () -> "expected convergence, got " + supervisor.lastDiffOf(POOL).findings());
    }

    @Test
    void aReplicaIsNeverPlacedOnTheNodeHoldingItsPrimary() {
        // With one node there is nowhere for the second copy to go, and the honest outcome is a yellow index
        // rather than two copies on one machine pretending to be redundancy.
        FakeNode only = node(NODE);
        only.start();
        createIndex(1, 1);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 12; pass++) {
            supervisor.tick();
            only.tick();
            advance();
        }

        assertEquals(1, only.inventory().copyCount(), "one node, one copy");
        assertEquals(1, supervisor.lastDiffOf(POOL).of(FindingKind.UNASSIGNED_SEARCH_REPLICA).size(),
            "createIndex builds a segment-replication index, so the copy it still wants is a search replica");
    }

    @Test
    void copiesAreSpreadAcrossZonesWhenThePoolSaysTo() {
        // The rule that makes redundancy mean anything: three copies in one availability zone survive a disk,
        // not a zone, and the zone is usually why the tenant asked for three.
        store.put(Keys.poolDefinition(POOL), new org.opensearch.supervisor.model.pool.PoolDefinition(
            POOL, PoolKind.SHARED, false, null, null, java.util.List.of("zone"), false
        ).toBytes());

        FakeNode z1a = zoned(NODE, "z1");
        FakeNode z1b = zoned(NodeId.of("nodeCCCCCCCCCCCCCCCCCC"), "z1");
        FakeNode z2 = zoned(NODE_B, "z2");
        z1a.start();
        z1b.start();
        z2.start();
        createIndex(1, 1);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 14; pass++) {
            supervisor.tick();
            z1a.tick();
            z1b.tick();
            z2.tick();
            advance();
        }

        assertEquals(1, z2.inventory().copyCount(), "z2 must hold one of the two copies");
        assertEquals(1, z1a.inventory().copyCount() + z1b.inventory().copyCount(),
            "and z1 exactly one, not both");
    }

    // ---------------------------------------------------------------- convergence

    @Test
    void aLostNodesShardIsReplacedOnceItIsGivenUpForDead() {
        // The whole point of the exercise, and the narrowest case that is genuinely useful: a shard that
        // exists nowhere gets placed somewhere.
        FakeNode a = node(NODE);
        a.start();
        createIndex(1, 0);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 6; pass++) {
            supervisor.tick();
            a.tick();
            advance();
        }
        assertEquals(1, a.inventory().copyCount());

        // Node A dies. A second node is available to take over.
        FakeNode b = node(NODE_B);
        b.start();

        // The supervisor has to *observe* A's silence, not merely have time pass: liveness is measured from
        // when this process last saw the heartbeat key's revision change, precisely so that a node's own
        // clock cannot be used against it. Jumping the clock and taking one pass would prove nothing --
        // worst-case detection is the threshold plus a pass interval, and it has to be waited out.
        for (int pass = 0; pass < 40; pass++) {
            supervisor.tick();
            b.tick();
            clock.advance(Duration.ofSeconds(5));
        }

        assertEquals(1, b.inventory().copyCount(), "the shard should have been rebuilt on the surviving node");
        assertTrue(supervisor.lastDiffOf(POOL).of(FindingKind.UNASSIGNED_PRIMARY).isEmpty());
    }

    @Test
    void nothingIsWrittenWhenThereIsNothingToDo() {
        // Steady state should be quiet in the store as well as in the diff.
        FakeNode data = node(NODE);
        data.start();
        createIndex(1, 0);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 8; pass++) {
            supervisor.tick();
            data.tick();
            advance();
        }
        assertTrue(supervisor.lastDiffOf(POOL).isEmpty());

        long revision = store.get(Keys.nodeAssignment(NODE)).orElseThrow().revision();
        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            data.tick();
            advance();
        }

        assertEquals(revision, store.get(Keys.nodeAssignment(NODE)).orElseThrow().revision(),
            "a converged pool should not rewrite assignments");
    }

    @Test
    void aFleetActingTogetherStillPlacesEachShardOnce() {
        // Three supervisors, one of which holds the pool. The others must not write anything for it, and the
        // lease is what guarantees that rather than any agreement between them.
        FakeNode a = node(NODE);
        FakeNode b = node(NODE_B);
        a.start();
        b.start();
        createIndex(3, 0);

        var fleet = java.util.List.of(acting("s1"), acting("s2"), acting("s3"));
        for (int pass = 0; pass < 12; pass++) {
            fleet.forEach(Supervisor::tick);
            a.tick();
            b.tick();
            advance();
        }

        assertEquals(3, a.inventory().copyCount() + b.inventory().copyCount(),
            "three shards, three copies -- no duplicates from three supervisors");
    }

    // ---------------------------------------------------- search replicas (FINDINGS_FROM_THE_DATA_PLANE 1)

    private void createIndexWithSearchReplicas(int shards, int replicas, int searchReplicas) {
        store.put(Keys.indicesPrefix(ACME) + "my-index", IndexConfig.toDocument(
            "my-index", UUID, true,
            IndexConfig.settingsOf(shards, replicas, searchReplicas, Replication.SEGMENT)
        ));
    }

    @Test
    void aRequestedSearchReplicaIsActuallyPlaced() {
        // The regression this comes from, end to end: a tenant asked for one shard, no replicas and one search
        // replica; the supervisor planned one copy and projected the index green with zero unassigned shards.
        FakeNode a = node(NODE);
        FakeNode b = node(NODE_B);
        a.start();
        b.start();
        createIndexWithSearchReplicas(1, 0, 1);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 14; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            advance();
        }

        assertEquals(2, a.inventory().copyCount() + b.inventory().copyCount(),
            "a primary and the search replica that was asked for");
        assertEquals(1, rolesHeld(a, b).stream().filter(role -> role == ShardRole.SEARCH_REPLICA).count(),
            "and the second copy is a search replica: " + rolesHeld(a, b));

        IndexStatus status = IndexStatus.from(store.get(Keys.indexStatus(ACME, UUID)).orElseThrow().value());
        assertEquals(HealthStatus.GREEN, status.status(), "both copies are placed, so this is green");
        assertEquals(1, status.numberOfSearchReplicas(), "and the projection says what was asked for");
    }

    @Test
    void anIndexIsNotGreenWhileARequestedSearchReplicaIsUnplaced() {
        // The half that made this expensive to find. One node cannot hold both copies of a shard, so the search
        // replica stays unplaced -- and the old arithmetic left the index green with a copy missing, which is a
        // status that tells an operator to look somewhere else.
        FakeNode only = node(NODE);
        only.start();
        createIndexWithSearchReplicas(1, 0, 1);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 14; pass++) {
            supervisor.tick();
            only.tick();
            advance();
        }

        assertEquals(1, only.inventory().copyCount(), "one node, one copy");

        IndexStatus status = IndexStatus.from(store.get(Keys.indexStatus(ACME, UUID)).orElseThrow().value());
        assertEquals(1, status.unassignedShards(), "the search replica is desired and absent");
        assertEquals(HealthStatus.YELLOW, status.status(), "so this is not green");

        // And the diff says which kind of copy is missing, rather than leaving it to be inferred.
        assertEquals(1, supervisor.lastDiffOf(POOL).of(FindingKind.UNASSIGNED_SEARCH_REPLICA).size());
    }

    @Test
    void aSearchReplicaOnADocumentReplicationIndexIsReportedNotPlanned() {
        FakeNode a = node(NODE);
        FakeNode b = node(NODE_B);
        a.start();
        b.start();
        store.put(Keys.indicesPrefix(ACME) + "my-index", IndexConfig.toDocument(
            "my-index", UUID, true, IndexConfig.settingsOf(1, 0, 1, Replication.DOCUMENT)
        ));
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 12; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            advance();
        }

        assertEquals(1, a.inventory().copyCount() + b.inventory().copyCount(),
            "the primary only; a search-only copy has no remote store to recover from here");
        assertEquals(1, supervisor.lastDiffOf(POOL).of(FindingKind.DESIRED_COPY_IGNORED).size());

        String diff = new String(store.get(Keys.tenantDiff(ACME)).orElseThrow().value(),
            java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(diff.contains("DESIRED_COPY_IGNORED"), diff);
        assertTrue(diff.contains("document replication"), diff);
    }

    private java.util.List<ShardRole> rolesHeld(FakeNode... nodes) {
        java.util.List<ShardRole> roles = new java.util.ArrayList<>();
        for (FakeNode node : nodes) {
            var tenant = node.inventory().tenants().get(ACME);
            if (tenant == null) {
                continue;
            }
            var index = tenant.indices().get(UUID);
            if (index != null) {
                index.shards().values().forEach(copy -> roles.add(copy.role()));
            }
        }
        return roles;
    }

    // ------------------------------------------- coordinators (FINDINGS_FROM_THE_DATA_PLANE 4)

    private static final NodeId COORDINATOR = NodeId.of("nodeDDDDDDDDDDDDDDDDDD");

    private FakeNode coordinatorNode(NodeId id) {
        return new FakeNode(store, new Registration(
            id, "placeholder", id.value(), POOL, EnumSet.of(NodeRole.COORDINATOR),
            "10.0.0." + id.value().charAt(4), 9300, "3.7.0", remoteStoreNode("z1"), 5000, 0L
        ), clock, "eph-" + id.value());
    }

    private FakeNode bothRolesNode(NodeId id) {
        return new FakeNode(store, new Registration(
            id, "placeholder", id.value(), POOL, EnumSet.of(NodeRole.DATA, NodeRole.COORDINATOR),
            "10.0.0." + id.value().charAt(4), 9300, "3.7.0", remoteStoreNode("z1"), 5000, 0L
        ), clock, "eph-" + id.value());
    }

    private Assignment assignmentOf(NodeId node) {
        return store.get(Keys.nodeAssignment(node))
            .map(entry -> NodeJson.readAssignment(entry.value()))
            .orElse(null);
    }

    private void registerTenant(TenantId tenant) {
        store.put(Keys.poolBinding(tenant), PoolBinding.to(POOL).toBytes());
        store.put(Keys.tenantRegistration(tenant),
            ("{\"tenant\":\"" + tenant.value() + "\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void aCoordinatorIsToldWhichTenantsItServes() {
        // Nothing used to write this at all: assignments came only from placement, and a coordinator has nothing
        // to place, so it was observed, counted, found placeable for nothing, and told nothing for ever.
        TenantId globex = TenantId.of("globex");
        registerTenant(globex);
        FakeNode coordinator = coordinatorNode(COORDINATOR);
        coordinator.start();
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 8; pass++) {
            supervisor.tick();
            coordinator.tick();
            advance();
        }

        Assignment assignment = assignmentOf(COORDINATOR);
        assertNotNull(assignment, "a coordinator with bound tenants must be told about them");
        assertEquals(java.util.Set.of(ACME, globex), assignment.tenants().keySet());
        assignment.tenants().forEach((tenant, entry) -> {
            assertTrue(entry.coordinates(), tenant + " should be coordinated: " + entry.roles());
            assertFalse(entry.holdsData(), tenant + " holds no data on a coordinator-only node");
            assertTrue(entry.indices().isEmpty());
        });
    }

    @Test
    void aNodeCanBeToldItHoldsDataAndCoordinatesForOneTenant() {
        // The configuration that could not be expressed, and the natural one for a small deployment: OpenSearch
        // runs a coordinator on every node out of the box, which is wrong at scale and exactly right for three
        // nodes on a laptop.
        FakeNode both = bothRolesNode(NODE);
        FakeNode other = bothRolesNode(NODE_B);
        both.start();
        other.start();
        createIndex(2, 1);
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 14; pass++) {
            supervisor.tick();
            both.tick();
            other.tick();
            advance();
        }

        Assignment assignment = assignmentOf(NODE);
        Assignment.TenantAssignment entry = assignment.tenants().get(ACME);
        assertTrue(entry.holdsData(), entry.roles().toString());
        assertTrue(entry.coordinates(), entry.roles().toString());
        assertFalse(entry.indices().isEmpty(), "and it really does hold copies");
    }

    @Test
    void aTenantReboundAwayLosesItsCoordinatorEntry() {
        // The removal half, which matters more than it looks: a coordinator that keeps a rebound tenant keeps
        // routing queries to shards a different supervisor now owns.
        TenantId globex = TenantId.of("globex");
        registerTenant(globex);
        FakeNode coordinator = coordinatorNode(COORDINATOR);
        coordinator.start();
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 8; pass++) {
            supervisor.tick();
            coordinator.tick();
            advance();
        }
        assertTrue(assignmentOf(COORDINATOR).tenants().containsKey(globex));

        // Rebound to some other pool, which this supervisor does not own.
        store.put(Keys.poolBinding(globex), PoolBinding.to(PoolId.of("shared-b")).toBytes());
        for (int pass = 0; pass < 8; pass++) {
            supervisor.tick();
            coordinator.tick();
            advance();
        }

        Assignment assignment = assignmentOf(COORDINATOR);
        assertFalse(assignment.tenants().containsKey(globex),
            "a rebound tenant must be dropped, not left to be routed to: " + assignment.tenants().keySet());
        assertTrue(assignment.tenants().containsKey(ACME), "and the tenant still bound here is untouched");
    }

    @Test
    void aSettledCoordinatorIsNotRewrittenEveryPass() {
        // Every placeable node is now considered every pass, so a no-op write would bump the version and revision
        // of every coordinator for ever -- which the node reads as a change and liveness reads as activity.
        FakeNode coordinator = coordinatorNode(COORDINATOR);
        coordinator.start();
        Supervisor supervisor = acting("s1");

        for (int pass = 0; pass < 8; pass++) {
            supervisor.tick();
            coordinator.tick();
            advance();
        }
        long settled = assignmentOf(COORDINATOR).version();

        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            coordinator.tick();
            advance();
        }

        assertEquals(settled, assignmentOf(COORDINATOR).version(),
            "nothing changed, so nothing should have been written");
    }

    @Test
    void aCoordinatorIsToldEvenWhenThePassSpentItsWorkCeilingPlacingShards() {
        // Membership rather than placement: nothing is moving, so a coordinator should not have to wait for a
        // quiet pass to learn what it is for.
        FakeNode data = node(NODE);
        FakeNode coordinator = coordinatorNode(COORDINATOR);
        data.start();
        coordinator.start();
        createIndex(6, 0);
        // A ceiling of one placement per pass, so every pass is exhausted by shard work.
        Supervisor supervisor = new Supervisor(store, clock, "s1", Set.of(), 1.0, TTL,
            Balancer.Config.defaults(), new Planner.Policy(1, 1, 2), false);

        for (int pass = 0; pass < 6; pass++) {
            supervisor.tick();
            data.tick();
            coordinator.tick();
            advance();
        }

        assertTrue(data.inventory().copyCount() < 6, "the ceiling should still be biting");
        Assignment assignment = assignmentOf(COORDINATOR);
        assertNotNull(assignment, "yet the coordinator was told anyway");
        assertTrue(assignment.tenants().get(ACME).coordinates());
    }

    // ------------------------------- consumer-pointer resets (DESIGN.md §21, data plane findings 6 and 8)

    private void submitResumeTask(String taskId, int shard, String mode, String value) {
        String json = "{\"task_id\":\"" + taskId + "\",\"type\":\"resume-ingestion\",\"index\":\"my-index\","
            + "\"index_uuid\":\"" + UUID + "\",\"state\":\"pending\",\"submitted_at\":1000,\"sequence\":1,"
            + "\"details\":{\"reset_settings\":[{\"shard\":" + shard + ",\"mode\":\"" + mode + "\","
            + "\"value\":\"" + value + "\"}]}}";
        store.put(Keys.tasksPrefix(ACME) + taskId, json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private Assignment.IngestionReset resetIn(NodeId node) {
        Assignment assignment = assignmentOf(node);
        if (assignment == null) {
            return null;
        }
        var tenant = assignment.tenants().get(ACME);
        if (tenant == null || tenant.indices().get(UUID) == null) {
            return null;
        }
        return tenant.indices().get(UUID).ingestionReset();
    }

    @Test
    void aResetReachesOnlyTheNodeHoldingThatShardsPrimary() {
        FakeNode a = node(NODE);
        FakeNode b = node(NODE_B);
        a.start();
        b.start();
        // Two shards, one replica each, so both nodes hold a primary and a search replica.
        createIndex(2, 1);
        Supervisor supervisor = acting("s1");
        for (int pass = 0; pass < 14; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            advance();
        }

        submitResumeTask("resume-1", 0, "OFFSET", "1000");
        // The nodes stop applying, so the reset stays outstanding long enough to be looked at. With them ticking
        // the whole cycle -- hand over, acknowledge, withdraw -- finishes in about three passes.
        for (int pass = 0; pass < 4; pass++) {
            supervisor.tick();
            advance();
        }

        // Exactly one node gets it: whichever holds shard 0's primary. A search replica has no consumer, and the
        // data plane refuses a reset for a shard it was not made primary of rather than trusting this.
        Assignment.IngestionReset onA = resetIn(NODE);
        Assignment.IngestionReset onB = resetIn(NODE_B);
        assertTrue(onA == null ^ onB == null, "exactly one holder: A=" + onA + " B=" + onB);

        Assignment.IngestionReset reset = onA == null ? onB : onA;
        assertEquals("resume-1", reset.id(), "the task id is the identity a node keys idempotence on");
        assertEquals(java.util.Set.of(0), reset.shards().keySet(), "only the shard the task named");
        assertEquals(ResetMode.OFFSET, reset.shards().get(0).mode());
        assertEquals("1000", reset.shards().get(0).value());
    }

    @Test
    void aResetIsHandedOverAcknowledgedAndThenWithdrawn() {
        // The whole cycle, which is the thing worth asserting: the reset appears in the assignment, the node
        // applies it and reports the version carrying it, the task completes on that acknowledgement, and the
        // instruction is withdrawn. A first version of this test expected the reset to still be there after four
        // passes and was simply wrong about its own design -- the loop closes, which is the point.
        FakeNode a = node(NODE);
        a.start();
        createIndex(1, 0);
        Supervisor supervisor = acting("s1");
        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            a.tick();
            advance();
        }
        submitResumeTask("resume-cycle", 0, "Offset", "1000");

        boolean seen = false;
        String modeAsWritten = null;
        for (int pass = 0; pass < 12; pass++) {
            supervisor.tick();
            a.tick();
            advance();
            Assignment.IngestionReset reset = resetIn(NODE);
            if (reset != null) {
                seen = true;
                assertEquals("resume-cycle", reset.id());
                assertEquals(ResetMode.OFFSET, reset.shards().get(0).mode());
                modeAsWritten = new String(store.get(Keys.nodeAssignment(NODE)).orElseThrow().value(),
                    java.nio.charset.StandardCharsets.UTF_8);
            }
        }

        assertTrue(seen, "the reset must reach the node's assignment at some point");
        // Canonical spelling, whatever the client sent: the control plane stores the string verbatim after a
        // case-insensitive check, so "Offset" arrives and one form leaves.
        assertTrue(modeAsWritten.contains("\"mode\":\"offset\""), modeAsWritten);
        // And no phantom fields: a convenience method called isEmpty() once put "empty": false in here.
        assertFalse(modeAsWritten.contains("\"empty\""), modeAsWritten);

        assertEquals(TaskState.COMPLETED, taskStateOf("resume-cycle"),
            "the acknowledgement is what completes it");
        assertNull(resetIn(NODE), "and the instruction is withdrawn once it has been acted on");
    }

    @Test
    void aResetIsNotWithdrawnBeforeItIsAcknowledged() {
        // The failure the acknowledgement exists to prevent, driven by a node that never applies anything: the
        // reset must stay in the assignment and the task must stay open, for as long as nobody confirms it.
        FakeNode a = node(NODE);
        a.start();
        createIndex(1, 0);
        Supervisor supervisor = acting("s1");
        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            a.tick();
            advance();
        }

        submitResumeTask("resume-silent", 0, "OFFSET", "1000");
        // The node stops applying: it keeps heartbeating, so it stays live, but never acknowledges a new version.
        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            advance();
        }

        assertNotNull(resetIn(NODE), "still outstanding, so still written");
        assertNotEquals(TaskState.COMPLETED, taskStateOf("resume-silent"),
            "and nothing completed it on the strength of a version nobody applied");
    }

    private TaskState taskStateOf(String taskId) {
        return store.get(Keys.tasksPrefix(ACME) + taskId)
            .map(entry -> org.opensearch.supervisor.model.task.TaskRecord.from(entry.value()).state())
            .orElse(null);
    }

    @Test
    void anOutstandingResetIsNotRewrittenEveryPass() {
        // It is part of the tenants map, so the no-op guard covers it -- but a reset that churned the assignment
        // every pass would have the node re-read it constantly and look like a change each time.
        FakeNode a = node(NODE);
        a.start();
        createIndex(1, 0);
        Supervisor supervisor = acting("s1");
        for (int pass = 0; pass < 10; pass++) {
            supervisor.tick();
            a.tick();
            advance();
        }
        submitResumeTask("resume-stable", 0, "OFFSET", "1000");

        // Supervisor only: the node never acknowledges, so the reset stays outstanding and its stability is what
        // is under test rather than the speed of the handover.
        supervisor.tick();
        advance();
        long withReset = assignmentOf(NODE).version();
        assertNotNull(resetIn(NODE), "written on the first pass that sees the task");

        for (int pass = 0; pass < 8; pass++) {
            supervisor.tick();
            advance();
        }

        assertEquals(withReset, assignmentOf(NODE).version(), "an outstanding reset is stable, not rewritten");
        assertNotNull(resetIn(NODE));
    }


}
