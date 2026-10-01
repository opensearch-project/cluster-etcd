package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.fakenode.FakeNode;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.monitor.Alert;
import org.opensearch.supervisor.model.monitor.Rules;
import org.opensearch.supervisor.model.monitor.Sample;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.plan.Planner;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.projection.MemberStatus;
import org.opensearch.supervisor.model.status.Replication;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The monitoring path end to end: a real supervisor writes, {@link MonitorReader} reads, the rules judge.
 *
 * <p>Worth having separately from {@code RulesTests} because the rules are only as good as the documents, and the
 * documents were the half that had rotted — two of them did not exist at all until now. This is the test that
 * would have caught that, and the only one here that spans both.
 */
class MonitoringTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final NodeId NODE = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId NODE_B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

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
        store.put(Keys.poolDefinition(POOL), new PoolDefinition(
            POOL, PoolKind.SHARED, false, null, null, List.of("zone"), false).toBytes());
        store.put(Keys.poolBinding(ACME), PoolBinding.to(POOL).toBytes());
        store.put(Keys.tenantRegistration(ACME), "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));
    }

    private FakeNode node(NodeId id) {
        return new FakeNode(store, new Registration(
            id, "placeholder", id.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0." + id.value().charAt(4), 9300, "3.7.0",
            Map.of("zone", "z1",
                org.opensearch.supervisor.model.allocate.RemoteStoreDecider.SEGMENT_REPOSITORY_ATTRIBUTE,
                "segments"),
            5000, 0L
        ), clock, "eph-" + id.value());
    }

    private Supervisor supervisor() {
        return new Supervisor(store, clock, "s1", Set.of(), 1.0, Duration.ofSeconds(15),
            Balancer.Config.defaults(), new Planner.Policy(200, 5, 2), true,
            LivenessTracker.Config.defaults(), Duration.ofSeconds(2));
    }

    private void createIndex(int shards, int searchReplicas) {
        store.put(Keys.indicesPrefix(ACME) + "my-index", IndexConfig.toDocument(
            "my-index", UUID, true, IndexConfig.settingsOf(shards, 0, searchReplicas, Replication.SEGMENT)));
    }

    private Sample sample(Sample previous) {
        return new MonitorReader(store, clock).read(previous);
    }

    private static Alert only(List<Alert> alerts, String rule) {
        return alerts.stream().filter(alert -> alert.rule().equals(rule)).findFirst().orElseThrow(
            () -> new AssertionError("no " + rule + " in " + alerts));
    }

    private static boolean fired(List<Alert> alerts, String rule) {
        return alerts.stream().anyMatch(alert -> alert.rule().equals(rule));
    }

    // ---------------------------------------------------------------- the documents exist at all

    @Test
    void theMemberDocumentPublishesTheEffectiveLivenessThresholds() {
        // R3.3, which had no observable answer at all: the multipliers and the floor were configurable and the
        // per-node result -- the number an operator actually needs -- was computed nowhere anyone could read.
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        Supervisor supervisor = supervisor();
        supervisor.tick();
        clock.advance(Duration.ofSeconds(2));
        supervisor.tick();

        MemberStatus member = MemberStatus.from(
            store.get(Keys.memberStatus("s1")).orElseThrow().value());

        assertEquals("s1", member.supervisorId());
        assertEquals(2000, member.passIntervalMillis(), "the cadence PASS_SATURATED divides by");
        assertEquals(6, member.liveness().staleMultiplier());
        assertEquals(20, member.liveness().lostMultiplier());
        assertNotNull(member.storeBackend());

        // The part R3.3 is actually about: not the multipliers, but what they come to for a real node whose own
        // advertised interval is an input to the answer.
        MemberStatus.Thresholds effective = member.liveness().effectiveMillis().get(NODE.value());
        assertNotNull(effective, "the node's own thresholds are the observable value: " + member.liveness());
        assertEquals(30_000, effective.staleMillis(), "6 x the node's advertised 5s");
        assertEquals(100_000, effective.lostMillis(), "20 x the node's advertised 5s");
    }

    @Test
    void theMemberDocumentIsNotRewrittenEveryPass() {
        // It describes configuration, which almost never changes. A document rewritten every pass for a value
        // that never moves would make the supervisor the store's dominant writer for no information at all.
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        Supervisor supervisor = supervisor();
        for (int pass = 0; pass < 4; pass++) {
            supervisor.tick();
            data.tick();
            clock.advance(Duration.ofSeconds(2));
        }
        long revision = store.get(Keys.memberStatus("s1")).orElseThrow().revision();

        for (int pass = 0; pass < 6; pass++) {
            supervisor.tick();
            data.tick();
            clock.advance(Duration.ofSeconds(2));
        }

        assertEquals(revision, store.get(Keys.memberStatus("s1")).orElseThrow().revision(),
            "unchanged configuration must cost no writes");
    }

    @Test
    void theReapingDocumentRecordsWhatTheReaperWouldNotDelete() {
        // A pool worn down to its last node, whose copy nothing can replace. The reaper declines, for ever, and
        // declining produces no finding -- so before this document existed the state was completely silent, and
        // the reason the supervisor had for it existed only in the memory of the process that decided it.
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        createIndex(1, 0);
        Supervisor acting = new Supervisor(store, clock, "s1", Set.of(), 1.0, Duration.ofSeconds(15),
            Balancer.Config.defaults(), new Planner.Policy(200, 5, 2), false,
            LivenessTracker.Config.defaults(), Duration.ofSeconds(2));
        for (int pass = 0; pass < 8; pass++) {
            acting.tick();
            data.tick();
            clock.advance(Duration.ofSeconds(2));
        }

        // The node never speaks again, and there is nowhere else for its copy to go.
        for (int pass = 0; pass < 120; pass++) {
            acting.tick();
            clock.advance(Duration.ofSeconds(2));
        }

        var stored = store.get(Keys.poolReaping(POOL));
        assertTrue(stored.isPresent(), "a declined reap has to leave a record, or the state is invisible");
        var reaping = org.opensearch.supervisor.model.projection.ReapingStatus.from(stored.get().value());
        assertEquals(0, reaping.reapedTotal());
        assertEquals(1, reaping.keptTotal());
        assertTrue(reaping.kept().get(0).because().contains("only node with a started copy"),
            reaping.kept().get(0).because());

        // And it reaches an operator as an alert, which is the whole chain.
        Alert alert = only(Rules.evaluate(sample(null)), "NODE_REAP_BLOCKED");
        assertEquals(NODE.value(), alert.subject());
    }

    // ---------------------------------------------------------------- the rules, against real documents

    @Test
    void aConvergedPoolAlertsAboutNothing() {
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        createIndex(1, 0);
        Supervisor acting = new Supervisor(store, clock, "s1", Set.of(), 1.0, Duration.ofSeconds(15),
            Balancer.Config.defaults(), new Planner.Policy(200, 5, 2), false,
            LivenessTracker.Config.defaults(), Duration.ofSeconds(2));

        for (int pass = 0; pass < 8; pass++) {
            acting.tick();
            data.tick();
            clock.advance(Duration.ofSeconds(2));
        }

        List<Alert> alerts = Rules.evaluate(sample(null));
        assertEquals(List.of(), alerts, () -> "a healthy pool must be quiet, got " + alerts);
    }

    @Test
    void anUnplaceableCopyBecomesAnAlertCarryingTheDecidersOwnSentence() {
        // The end-to-end version of the claim this whole component rests on: the alert is only as good as the
        // `because` the supervisor wrote, and here that sentence travels from a decider, through the diff
        // projection, into the alert, unparaphrased.
        FakeNode a = node(NODE);
        FakeNode b = node(NODE_B);
        a.start();
        b.start();
        a.tick();
        b.tick();
        createIndex(1, 3);
        Supervisor acting = new Supervisor(store, clock, "s1", Set.of(), 1.0, Duration.ofSeconds(15),
            Balancer.Config.defaults(), new Planner.Policy(200, 5, 2), false,
            LivenessTracker.Config.defaults(), Duration.ofSeconds(2));

        for (int pass = 0; pass < 120; pass++) {
            acting.tick();
            a.tick();
            b.tick();
            clock.advance(Duration.ofSeconds(2));
        }

        List<Alert> alerts = Rules.evaluate(sample(null));
        Alert alert = only(alerts, "UNASSIGNED_SEARCH_REPLICA");
        assertTrue(alert.because().contains("passes:"), alert.because());
        assertTrue(alert.because().length() > 60,
            "the supervisor's own explanation has to reach the operator, not a rule name: " + alert.because());
    }

    @Test
    void aPoolNobodyLeasedIsTheDenominatorAlert() {
        // No supervisor runs at all here. Every status document is therefore absent -- which is exactly what a
        // quiet pool looks like -- so this can only be found by reading what an operator declared.
        store.put(Keys.poolDefinition(PoolId.of("lonely")), new PoolDefinition(
            PoolId.of("lonely"), PoolKind.SHARED, false, null, null, List.of(), false).toBytes());

        List<Alert> alerts = Rules.evaluate(sample(null));

        assertTrue(fired(alerts, "POOL_UNSUPERVISED"));
        assertTrue(fired(alerts, "NO_SUPERVISOR_MEMBERS"));
        assertEquals(2, alerts.stream().mapToInt(Alert::exitStatus).max().orElse(0),
            "a shell must be able to tell critical from warning without parsing anything");
    }

    @Test
    void aWedgedSupervisorIsVisibleFromOutsideAndFromNowhereElse() {
        // Two samples and a wall clock, which is the whole reason this is not part of the supervisor: a pass
        // cannot report its own absence, and the supervisor's own ages are counted in passes that stop when it
        // does. Here the supervisor simply never ticks again while time moves on.
        FakeNode data = node(NODE);
        data.start();
        data.tick();
        createIndex(1, 0);
        Supervisor acting = new Supervisor(store, clock, "s1", Set.of(), 1.0, Duration.ofSeconds(15),
            Balancer.Config.defaults(), new Planner.Policy(200, 5, 2), false,
            LivenessTracker.Config.defaults(), Duration.ofSeconds(2));
        for (int pass = 0; pass < 8; pass++) {
            acting.tick();
            data.tick();
            clock.advance(Duration.ofSeconds(2));
        }

        Sample first = sample(null);
        assertFalse(fired(Rules.evaluate(first), "POOL_NOT_ADVANCING"), "one sample cannot know");

        // The supervisor stops. Nothing else changes, and nothing raises a hand anywhere in the store.
        clock.advance(Duration.ofMinutes(5));
        Sample second = sample(first);

        Alert alert = only(Rules.evaluate(second), "POOL_NOT_ADVANCING");
        assertEquals(Alert.Severity.CRITICAL, alert.severity());
        assertTrue(alert.because().contains("has not moved"), alert.because());
    }
}
