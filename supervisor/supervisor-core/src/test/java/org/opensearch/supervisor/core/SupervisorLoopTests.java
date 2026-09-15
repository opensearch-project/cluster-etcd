package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.fakenode.FakeNode;
import org.opensearch.supervisor.model.Json;
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
 * The two halves meeting: a real {@link FakeNode} writing the node contract, and the real supervisor
 * observing it. Nothing here hand-writes a node document, so the observer is reading what an implementation
 * of the contract actually produces rather than what this test thinks it should.
 *
 * <p>That distinction is {@code WORKING_NOTES.md}'s: tests and implementation share the author's
 * assumptions, so a misread field makes both wrong together and agreeing. Driving the other side's real
 * code is the cheapest available defence.
 *
 * <p>The loop is read-only, so the assignments here are written by the test standing in for a planner that
 * arrives at milestone 5.
 */
class SupervisorLoopTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final NodeId NODE = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private InMemoryKeyValueStore store;
    private MutableClock clock;
    private SupervisorLoop loop;
    private FakeNode node;

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

        store.put(Keys.poolDefinition(POOL),
            new PoolDefinition(POOL, PoolKind.SHARED, false, null, null, java.util.List.of(), false).toBytes());
        store.put(Keys.poolBinding(ACME), PoolBinding.to(POOL).toBytes());
        store.put(Keys.tenantRegistration(ACME),
            "{\"tenant\":\"acme\",\"registered_at\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        loop = new SupervisorLoop(store, clock, POOL, "supervisor-a1b2", Set.of());
        node = new FakeNode(store, new Registration(
            NODE, "placeholder", "data-1", POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of("zone", "z1"), 5000, 0L
        ), clock, "eph-1");
    }

    private void createIndex(int shards, int replicas) {
        store.put(Keys.indicesPrefix(ACME) + "my-index", IndexConfig.toDocument(
            "my-index", UUID, true, IndexConfig.settingsOf(shards, replicas, Replication.SEGMENT)
        ));
    }

    private void assign(Map<Integer, ShardRole> shards, long version) {
        store.put(Keys.nodeAssignment(NODE), NodeJson.write(new Assignment(
            version, POOL, 1, NODE, clock.millis(), Map.of(
                ACME, new Assignment.TenantAssignment(NodeRole.DATA, Map.of(
                    UUID, new Assignment.IndexAssignment("my-index", 3041, shards)
                ))
            )
        )));
    }

    // ---------------------------------------------------------------- the milestone's actual deliverable

    @Test
    void anIndexWithNoShardsAnywhereIsReportedAndNothingIsDoneAboutIt() {
        node.start();
        node.tick();
        createIndex(1, 0);

        Diff diff = loop.pass();

        assertEquals(1, diff.of(FindingKind.UNASSIGNED_PRIMARY).size());
        // Read-only. The node was told nothing, so it still holds nothing.
        assertTrue(store.get(Keys.nodeAssignment(NODE)).isEmpty(), "the loop must not write an assignment");
    }

    @Test
    void theDiffEmptiesOnceTheNodeHasConverged() {
        node.start();
        createIndex(1, 0);
        assign(Map.of(0, ShardRole.PRIMARY), 1);

        node.tick();
        // Initializing, but present: placing a second primary would be actively wrong, so this is already
        // not a finding. The role/state split is what makes that expressible.
        assertTrue(loop.pass().isEmpty(), "an initializing primary is still a primary");
        assertEquals(
            org.opensearch.supervisor.model.node.ShardState.INITIALIZING,
            node.inventory().tenants().get(ACME).indices().get(UUID).shards().get(0).state()
        );

        node.tick();
        Diff converged = loop.pass();

        assertTrue(converged.isEmpty(), () -> "expected nothing, got " + converged.findings());
        assertEquals(
            org.opensearch.supervisor.model.node.ShardState.STARTED,
            node.inventory().tenants().get(ACME).indices().get(UUID).shards().get(0).state()
        );
    }

    @Test
    void anOperatorCanReadWhatIsWrongOutOfTheStore() throws Exception {
        // R7.1, and the whole point of projecting rather than serving: this is readable when the supervisor
        // is not, with the same tooling as everything else.
        node.start();
        node.tick();
        createIndex(2, 1);

        loop.pass();

        var document = Json.mapper().readTree(store.get(Keys.tenantDiff(ACME)).orElseThrow().value());
        assertEquals(4, document.get("total").asInt(), "2 shards x (1 missing primary + 1 missing replica)");
        assertFalse(document.get("findings").get(0).get("because").asText().isBlank());

        var convergence = Json.mapper().readTree(store.get(Keys.poolConvergence(POOL)).orElseThrow().value());
        assertEquals(1, convergence.get("nodes").asInt());
        assertEquals(1, convergence.get("indices").asInt());
        assertEquals("supervisor-a1b2", convergence.get("holder").asText());
    }

    @Test
    void findingsAgeAcrossPasses() {
        // The alarm is that a finding is old, and that cannot be computed from a single pass.
        node.start();
        node.tick();
        createIndex(1, 0);

        assertEquals(1, loop.pass().of(FindingKind.UNASSIGNED_PRIMARY).get(0).passes());
        assertEquals(2, loop.pass().of(FindingKind.UNASSIGNED_PRIMARY).get(0).passes());
        assertEquals(3, loop.pass().of(FindingKind.UNASSIGNED_PRIMARY).get(0).passes());
    }

    @Test
    void aNodeGoingSilentIsNoticedAndItsShardsAreEventuallyMissing() {
        node.start();
        createIndex(1, 0);
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();
        assertTrue(loop.pass().isEmpty());

        // The node stops ticking. Its heartbeat key stops changing revision, which is the only signal.
        clock.advance(Duration.ofSeconds(35));
        Diff stale = loop.pass();
        assertEquals(1, stale.of(FindingKind.NODE_STALE).size());
        assertTrue(stale.of(FindingKind.UNASSIGNED_PRIMARY).isEmpty(), "a stale node's copies still count");

        clock.advance(Duration.ofSeconds(100));
        Diff lost = loop.pass();
        assertEquals(1, lost.of(FindingKind.NODE_LOST).size());
        assertEquals(1, lost.of(FindingKind.UNASSIGNED_PRIMARY).size(), "a lost node's copies do not");
    }

    @Test
    void aNodeThatComesBackClearsTheDiffAgain() {
        node.start();
        createIndex(1, 0);
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();
        // A baseline pass, so the tracker has seen this heartbeat's revision once. Without it the first
        // observation is always LIVE -- which is the startup grace period doing its job, not a bug.
        assertTrue(loop.pass().isEmpty());

        clock.advance(Duration.ofSeconds(200));
        assertFalse(loop.pass().isEmpty());

        clock.advance(Duration.ofSeconds(1));
        node.tick();

        assertTrue(loop.pass().isEmpty(), "one heartbeat should be enough to make it live again");
    }

    @Test
    void aDeletedIndexLeavesAnOrphanRatherThanAnInstruction() {
        node.start();
        createIndex(1, 0);
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();
        assertTrue(loop.pass().isEmpty());

        store.delete(Keys.indicesPrefix(ACME) + "my-index");
        Diff diff = loop.pass();

        assertEquals(1, diff.of(FindingKind.ORPHANED_COPY).size());
        // And the node still holds it, because nothing acted.
        assertEquals(1, node.inventory().copyCount());
    }

    @Test
    void aTenantBoundToAnotherPoolIsNotThisLoopsProblem() {
        node.start();
        node.tick();
        createIndex(1, 0);
        store.put(Keys.poolBinding(ACME), PoolBinding.to(PoolId.of("other")).toBytes());

        assertTrue(loop.pass().of(FindingKind.UNASSIGNED_PRIMARY).isEmpty());
    }

    @Test
    void aQuietPassRewritesNothing() {
        // In steady state the supervisor should be almost invisible in the store's write load. It observes
        // constantly and writes only when something it has already said changes.
        node.start();
        createIndex(1, 0);
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();
        loop.pass();

        long diffRevision = store.get(Keys.tenantDiff(ACME)).orElseThrow().revision();
        node.tick();
        loop.pass();

        assertEquals(diffRevision, store.get(Keys.tenantDiff(ACME)).orElseThrow().revision());
    }
}
