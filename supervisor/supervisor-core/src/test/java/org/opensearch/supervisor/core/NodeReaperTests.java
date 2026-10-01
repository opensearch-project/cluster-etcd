package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Finding;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.diff.Subject;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.settings.AllocationEnable;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.Replication;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deleting a departed node's documents, which {@code DESIGN.md} §20.2 declined to do until the node contract
 * gained a rule that made it safe.
 *
 * <p>Three of the four keys belong to the node rather than to this service, so the tests that matter are the ones
 * where it must keep its hands off: a node that has written since it was observed, and a node still holding a copy
 * the pool has not managed to replace.
 */
class NodeReaperTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final NodeId DEAD = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId LIVE = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    private static final Instant START = Instant.ofEpochMilli(1_000_000);

    private InMemoryKeyValueStore store;
    private LivenessTracker liveness;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        liveness = new LivenessTracker(START);
    }

    private static Registration registration(NodeId node) {
        return new Registration(
            node, "eph-" + node.value(), node.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0." + node.value().charAt(4), 9300, "3.7.0", Map.of(), 5000, 1L
        );
    }

    private static Heartbeat heartbeat(NodeId node) {
        return new Heartbeat(node, "eph-" + node.value(), 1L, Heartbeat.Applied.NONE, null, 0,
            new Heartbeat.Capacity(1000, 500, 10, 10, 0.1));
    }

    private static ShardInventory holding(NodeId node, ShardState state) {
        return new ShardInventory(node, "eph-" + node.value(), 1L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                UUID, new ShardInventory.IndexShards("my-index", 3041, Map.of(
                    0, ShardInventory.ShardCopy.of(ShardRole.PRIMARY, state)
                ))
            ))
        ));
    }

    private static Assignment assignment(NodeId node) {
        return new Assignment(1, POOL, 1, node, 1L, Map.of(
            ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                UUID, new Assignment.IndexAssignment("my-index", 3041, Map.of(0, ShardRole.PRIMARY))
            ))
        ));
    }

    /** Writes the four documents a node leaves behind, so that deleting them is observable. */
    private void writeDocumentsOf(NodeId node, ShardInventory inventory, Assignment held) {
        store.put(Keys.nodeRegistration(node), bytes("registration"));
        store.put(Keys.nodeHeartbeat(node), bytes("heartbeat"));
        if (inventory != null) {
            store.put(Keys.nodeShards(node), bytes("inventory"));
        }
        if (held != null) {
            store.put(Keys.nodeAssignment(node), bytes("assignment"));
        }
    }

    private static byte[] bytes(String what) {
        return ("{\"placeholder\":\"" + what + "\"}").getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Teaches the tracker the heartbeat revision the observation saw, which is the reap's CAS witness.
     *
     * <p>Read out of the store rather than made up, because a witness that never matches would make every test
     * here pass for the wrong reason -- the reap would always decline.
     */
    private void observedAt(NodeId node) {
        liveness.observe(node, store.get(Keys.nodeHeartbeat(node)).orElseThrow().revision(),
            registration(node), START);
    }

    private static Observation.NodeObservation dead(ShardInventory inventory, Assignment held) {
        return new Observation.NodeObservation(
            DEAD, registration(DEAD), heartbeat(DEAD), inventory, Liveness.LOST, POOL, held, 5
        );
    }

    private static Observation.NodeObservation live(ShardState state) {
        return new Observation.NodeObservation(
            LIVE, registration(LIVE), heartbeat(LIVE), holding(LIVE, state), Liveness.LIVE, POOL,
            assignment(LIVE), 9
        );
    }

    private static IndexConfig index() {
        return new IndexConfig("my-index", UUID, true, 1, 0, Replication.SEGMENT, false,
            AllocationEnable.UNKNOWN, 3041);
    }

    private static Observation observe(Map<String, IndexConfig> indices,
        Map<NodeId, Observation.NodeObservation> nodes) {
        return new Observation(100, START, POOL,
            Map.of(ACME, new Observation.DesiredState(indices)), nodes, List.of());
    }

    /** A {@code NODE_LOST} finding that has held for {@code passes}, without running the differ for real. */
    private static Diff lostFor(long passes) {
        return new Diff(100, POOL, List.of(new Finding(
            FindingKind.NODE_LOST, Subject.ofNode(DEAD), "silent past the lost threshold", 100 - passes + 1, passes
        )));
    }

    private boolean stored(String key) {
        return store.get(key).isPresent();
    }

    // ---------------------------------------------------------------- reaping

    @Test
    void anAgedLostNodeWhoseCopiesWereReplacedLosesAllFourDocuments() {
        writeDocumentsOf(DEAD, holding(DEAD, ShardState.STARTED), assignment(DEAD));
        observedAt(DEAD);
        Observation observation = observe(Map.of(UUID, index()), Map.of(
            DEAD, dead(holding(DEAD, ShardState.STARTED), assignment(DEAD)),
            LIVE, live(ShardState.STARTED)
        ));

        NodeReaper.Report report = new NodeReaper(store, liveness)
            .reap(lostFor(NodeReaper.REAP_AFTER_LOST_PASSES), observation);

        assertEquals(1, report.reaped().size(), "kept instead: " + report.kept());
        assertEquals(4, report.reaped().get(0).keys().size());
        assertFalse(stored(Keys.nodeRegistration(DEAD)));
        assertFalse(stored(Keys.nodeHeartbeat(DEAD)));
        assertFalse(stored(Keys.nodeShards(DEAD)));
        assertFalse(stored(Keys.nodeAssignment(DEAD)));
        assertTrue(stored(Keys.nodeRegistration(LIVE)) == false, "the live node was never written here");
    }

    @Test
    void aNodeHoldingNothingAtAllIsReaped() {
        // The graveyard case: a data plane restarted on the same port a dozen times leaves a registration and a
        // frozen heartbeat per incarnation, holding nothing and assigned nothing.
        writeDocumentsOf(DEAD, null, null);
        observedAt(DEAD);
        Observation observation = observe(Map.of(UUID, index()), Map.of(DEAD, dead(null, null)));

        NodeReaper.Report report = new NodeReaper(store, liveness)
            .reap(lostFor(NodeReaper.REAP_AFTER_LOST_PASSES), observation);

        assertEquals(1, report.reaped().size(), "kept instead: " + report.kept());
        assertFalse(stored(Keys.nodeRegistration(DEAD)));
    }

    @Test
    void aCopyOfADeletedIndexDoesNotKeepTheNodeAlive() {
        // Nobody wants the copy, so losing it costs nothing -- and holding a node's documents for ever on account
        // of a copy of an index that does not exist would make the graveyard permanent.
        writeDocumentsOf(DEAD, holding(DEAD, ShardState.STARTED), assignment(DEAD));
        observedAt(DEAD);
        Observation observation = observe(Map.of(), Map.of(
            DEAD, dead(holding(DEAD, ShardState.STARTED), assignment(DEAD))
        ));

        NodeReaper.Report report = new NodeReaper(store, liveness)
            .reap(lostFor(NodeReaper.REAP_AFTER_LOST_PASSES), observation);

        assertEquals(1, report.reaped().size(), "kept instead: " + report.kept());
    }

    // ---------------------------------------------------------------- keeping its hands off

    @Test
    void aNodeThatHasWrittenSinceItWasObservedKeepsEverything() {
        // The window §20.2 worried about, closed with a compare-and-set. The decision came from an observation, so
        // the revision worth checking is the one that observation saw -- re-reading the key first would prove only
        // that nothing happened in the intervening microsecond.
        writeDocumentsOf(DEAD, null, null);
        observedAt(DEAD);
        store.put(Keys.nodeHeartbeat(DEAD), bytes("heartbeat, and it is alive after all"));

        NodeReaper.Report report = new NodeReaper(store, liveness)
            .reap(lostFor(NodeReaper.REAP_AFTER_LOST_PASSES),
                observe(Map.of(), Map.of(DEAD, dead(null, null))));

        assertEquals(List.of(), report.reaped());
        assertEquals(1, report.kept().size());
        assertTrue(report.kept().get(0).because().contains("changed since it was observed"),
            report.kept().get(0).because());
        assertTrue(stored(Keys.nodeRegistration(DEAD)), "and nothing else was touched either");
        assertTrue(stored(Keys.nodeHeartbeat(DEAD)));
    }

    @Test
    void aNodeWithTheOnlyStartedCopyOfAShardIsKept() {
        // The destructive case, and the reason this is not just tidying. An absent assignment means "hold
        // nothing", so reaping a node whose copy the pool never replaced would turn the operator action of
        // bringing a machine back into the deletion of the last copy of a shard.
        writeDocumentsOf(DEAD, holding(DEAD, ShardState.STARTED), assignment(DEAD));
        observedAt(DEAD);
        Observation observation = observe(Map.of(UUID, index()), Map.of(
            DEAD, dead(holding(DEAD, ShardState.STARTED), assignment(DEAD))
        ));

        NodeReaper.Report report = new NodeReaper(store, liveness)
            .reap(lostFor(NodeReaper.REAP_AFTER_LOST_PASSES), observation);

        assertEquals(List.of(), report.reaped());
        assertTrue(report.kept().get(0).because().contains("only node with a started copy"),
            report.kept().get(0).because());
        assertTrue(stored(Keys.nodeRegistration(DEAD)));
    }

    @Test
    void aReplacementThatIsStillInitializingIsNotYetCover() {
        // Started, not merely present: a copy still reading its data has not finished being a copy. The same rule
        // SurplusReaper uses before removing anything.
        writeDocumentsOf(DEAD, holding(DEAD, ShardState.STARTED), assignment(DEAD));
        observedAt(DEAD);
        Observation observation = observe(Map.of(UUID, index()), Map.of(
            DEAD, dead(holding(DEAD, ShardState.STARTED), assignment(DEAD)),
            LIVE, live(ShardState.INITIALIZING)
        ));

        NodeReaper.Report report = new NodeReaper(store, liveness)
            .reap(lostFor(NodeReaper.REAP_AFTER_LOST_PASSES), observation);

        assertEquals(List.of(), report.reaped());
        assertTrue(stored(Keys.nodeRegistration(DEAD)));
    }

    @Test
    void aCopyTheNodeWasAssignedButNeverReportedStillCounts() {
        // Two records of what a node has, and either alone is a way to be wrong. A node assigned a copy that died
        // before reporting it has no inventory entry for it, and reaping on the inventory alone would drop it.
        writeDocumentsOf(DEAD, null, assignment(DEAD));
        observedAt(DEAD);
        Observation observation = observe(Map.of(UUID, index()), Map.of(
            DEAD, dead(null, assignment(DEAD))
        ));

        NodeReaper.Report report = new NodeReaper(store, liveness)
            .reap(lostFor(NodeReaper.REAP_AFTER_LOST_PASSES), observation);

        assertEquals(List.of(), report.reaped());
        assertTrue(report.kept().get(0).because().contains("only node with a started copy"),
            report.kept().get(0).because());
    }

    @Test
    void aFindingThatHasNotAgedIsNotActedOn() {
        // The protection for a node that does not implement the registration-rewrite rule: it is still
        // contract-conformant, so the ageing is what gives an operator time to notice.
        writeDocumentsOf(DEAD, null, null);
        observedAt(DEAD);

        NodeReaper.Report report = new NodeReaper(store, liveness)
            .reap(lostFor(NodeReaper.REAP_AFTER_LOST_PASSES - 1),
                observe(Map.of(), Map.of(DEAD, dead(null, null))));

        assertEquals(NodeReaper.Report.NOTHING, report);
        assertTrue(stored(Keys.nodeRegistration(DEAD)));
    }

    @Test
    void aNodeThatIsMerelyStaleIsNotReapedHoweverOldTheFinding() {
        // Belt and braces against a finding whose age outlived its truth: the verdict is re-read from the
        // observation rather than trusted from the diff.
        writeDocumentsOf(DEAD, null, null);
        observedAt(DEAD);
        Observation.NodeObservation recovered = new Observation.NodeObservation(
            DEAD, registration(DEAD), heartbeat(DEAD), null, Liveness.STALE, POOL, null, 5
        );

        NodeReaper.Report report = new NodeReaper(store, liveness)
            .reap(lostFor(200), observe(Map.of(), Map.of(DEAD, recovered)));

        assertEquals(NodeReaper.Report.NOTHING, report);
        assertTrue(stored(Keys.nodeRegistration(DEAD)));
    }

    @Test
    void aTenantNoLongerBoundToThisPoolIsNotThisPoolsJudgement() {
        // Its indices are absent because this supervisor no longer reads them, not because they stopped
        // existing -- and whoever owns the tenant now has placed what it needs.
        writeDocumentsOf(DEAD, holding(DEAD, ShardState.STARTED), assignment(DEAD));
        observedAt(DEAD);
        Observation observation = new Observation(100, START, POOL, Map.of(),
            Map.of(DEAD, dead(holding(DEAD, ShardState.STARTED), assignment(DEAD))), List.of());

        NodeReaper.Report report = new NodeReaper(store, liveness)
            .reap(lostFor(NodeReaper.REAP_AFTER_LOST_PASSES), observation);

        assertEquals(1, report.reaped().size(), "kept instead: " + report.kept());
    }
}
