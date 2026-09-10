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
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.plan.Planner;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
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
        store.put(Keys.poolDefinition(POOL), new PoolDefinition(POOL, PoolKind.SHARED, false, null, null, java.util.List.of()).toBytes());
        store.put(Keys.poolBinding(ACME), PoolBinding.to(POOL).toBytes());
        store.put(Keys.tenantRegistration(ACME),
            "{\"tenant\":\"acme\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private FakeNode zoned(NodeId id, String zone) {
        return new FakeNode(store, new Registration(
            id, "placeholder", id.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0." + id.value().charAt(4), 9300, "3.7.0", Map.of("zone", zone), 5000, 0L
        ), clock, "eph-" + id.value());
    }

    private FakeNode node(NodeId id) {
        return new FakeNode(store, new Registration(
            id, "placeholder", id.value(), POOL, EnumSet.of(NodeRole.DATA),
            // Distinct per node: the same-shard decider treats one address as one failure domain, so a fixture
            // sharing addresses would make every second copy unplaceable.
            "10.0.0." + id.value().charAt(4), 9300, "3.7.0", Map.of("zone", "z1"), 5000, 0L
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
        assertEquals(1, supervisor.lastDiffOf(POOL).of(FindingKind.UNASSIGNED_REPLICA).size());
    }

    @Test
    void copiesAreSpreadAcrossZonesWhenThePoolSaysTo() {
        // The rule that makes redundancy mean anything: three copies in one availability zone survive a disk,
        // not a zone, and the zone is usually why the tenant asked for three.
        store.put(Keys.poolDefinition(POOL), new org.opensearch.supervisor.model.pool.PoolDefinition(
            POOL, PoolKind.SHARED, false, null, null, java.util.List.of("zone")
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
}
