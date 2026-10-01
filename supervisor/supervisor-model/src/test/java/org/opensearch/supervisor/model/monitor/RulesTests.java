package org.opensearch.supervisor.model.monitor;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.projection.MemberStatus;
import org.opensearch.supervisor.model.projection.Projections.Convergence;
import org.opensearch.supervisor.model.projection.Projections.HousekeepingStatus;
import org.opensearch.supervisor.model.projection.Projections.ProjectedFinding;
import org.opensearch.supervisor.model.projection.Projections.Tasks;
import org.opensearch.supervisor.model.projection.Projections.TenantDiff;
import org.opensearch.supervisor.model.projection.Projections.Trend;
import org.opensearch.supervisor.model.projection.ReapingStatus;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rules, from literals and with no store anywhere near them.
 *
 * <p>Which is the whole design: the rules live in a module that cannot see a {@code KeyValueStore}, so a test is
 * a hand-built {@link Sample} and an assertion — the same property that makes {@code Differ} and {@code Planner}
 * testable, inherited rather than re-argued.
 */
class RulesTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final long NOW = 1_000_000_000L;

    private static MemberStatus member(long passIntervalMillis, String backend) {
        return new MemberStatus("s1", NOW - 60_000, "dev", backend, List.of("shared-a"),
            new MemberStatus.Liveness(6, 20, 20_000, Map.of()),
            passIntervalMillis, 15_000, 200, 5, 2, true, false, NOW);
    }

    private static Convergence convergence(long pass, int findings, int delta, long passesSinceEmpty) {
        return new Convergence(POOL, 7, "s1", pass, NOW, 10, 3, 3, 2, 5.0,
            findings, Map.of(), NOW - 1000, passesSinceEmpty, new Trend(20, delta), Map.of(),
            Tasks.NONE, false, 0, 0, null);
    }

    /** The healthy shape: one pool, leased, a live member, a fresh housekeeping document, no findings. */
    private static Sample healthy() {
        return new Sample(NOW, null, List.of("shared-a"), Map.of("shared-a", "s1"),
            Map.of("shared-a", convergence(100, 0, 0, 0)), Map.of(),
            Map.of("s1", member(2000, "EtcdKeyValueStore")), Map.of(), List.of("s1"),
            housekeeping(List.of(), List.of()), true, List.of());
    }

    private static HousekeepingStatus housekeeping(List<String> unpooledTenants, List<String> unpooledNodes) {
        return new HousekeepingStatus(100, "s1", NOW, unpooledTenants, unpooledNodes, List.of(), List.of(),
            List.of(), List.of(), 1, 0, "registry", List.of());
    }

    private static Sample with(Sample base, java.util.function.Function<Sample, Sample> change) {
        return change.apply(base);
    }

    private static boolean fired(List<Alert> alerts, String rule) {
        return alerts.stream().anyMatch(alert -> alert.rule().equals(rule));
    }

    private static Alert only(List<Alert> alerts, String rule) {
        return alerts.stream().filter(alert -> alert.rule().equals(rule)).findFirst().orElseThrow(
            () -> new AssertionError("no " + rule + " in " + alerts));
    }

    // ---------------------------------------------------------------- the quiet case

    @Test
    void aHealthyPoolProducesNothing() {
        // The property worth having first, and the one an alerter most often fails: saying nothing when there is
        // nothing to say. A monitor that always fires is a monitor nobody reads.
        assertEquals(List.of(), Rules.evaluate(healthy()));
    }

    // ---------------------------------------------------------------- things a pass cannot see about itself

    @Test
    void aPoolWithADefinitionAndNoLeaseHolderIsCritical() {
        // The denominator rule. A pool nobody supervises produces no output at all, which is exactly what a quiet
        // pool produces -- so it can only be found by comparing what exists against what an operator declared.
        Sample sample = new Sample(NOW, null, List.of("shared-a", "orphaned"), Map.of("shared-a", "s1"),
            Map.of("shared-a", convergence(100, 0, 0, 0)), Map.of(),
            Map.of("s1", member(2000, "x")), Map.of(), List.of("s1"),
            housekeeping(List.of(), List.of()), true, List.of());

        Alert alert = only(Rules.evaluate(sample), "POOL_UNSUPERVISED");
        assertEquals(Alert.Severity.CRITICAL, alert.severity());
        assertEquals("orphaned", alert.subject());
    }

    @Test
    void aLeasedPoolWhosePassNumberHasNotMovedIsCritical() {
        // The alert this whole component exists for: revision-silence liveness applied to the supervisor instead
        // of to a node. Nothing inside a supervisor can fire it.
        Sample first = healthy();
        Sample second = new Sample(NOW + 120_000, first, first.poolsDefined(), first.leaseHolders(),
            Map.of("shared-a", convergence(100, 0, 0, 0)), Map.of(), first.members(), Map.of(),
            first.liveMembers(), first.housekeeping(), true, List.of());

        Alert alert = only(Rules.evaluate(second), "POOL_NOT_ADVANCING");
        assertEquals(Alert.Severity.CRITICAL, alert.severity());
        assertTrue(alert.because().contains("has not moved from 100"), alert.because());
    }

    @Test
    void aPoolThatIsAdvancingIsNotReported() {
        Sample first = healthy();
        Sample second = new Sample(NOW + 120_000, first, first.poolsDefined(), first.leaseHolders(),
            Map.of("shared-a", convergence(160, 0, 0, 0)), Map.of(), first.members(), Map.of(),
            first.liveMembers(), first.housekeeping(), true, List.of());

        assertFalse(fired(Rules.evaluate(second), "POOL_NOT_ADVANCING"));
    }

    @Test
    void twoSamplesTakenTooCloseTogetherProveNothing() {
        // Without a floor this would fire on any two reads inside one pass interval, which is most of them --
        // and it would fire hardest on a fast, healthy pool, which is the worst possible bias.
        Sample first = healthy();
        Sample second = new Sample(NOW + 1000, first, first.poolsDefined(), first.leaseHolders(),
            Map.of("shared-a", convergence(100, 0, 0, 0)), Map.of(), first.members(), Map.of(),
            first.liveMembers(), first.housekeeping(), true, List.of());

        assertFalse(fired(Rules.evaluate(second), "POOL_NOT_ADVANCING"));
    }

    @Test
    void aFirstRunCannotFireTheRulesThatNeedTwoObservations() {
        // Stated rather than worked around: one sample has no predecessor, so the rules that compare do not run.
        // The command says so in its output, because a quiet monitor and a blind one look identical.
        assertFalse(fired(Rules.evaluate(healthy()), "POOL_NOT_ADVANCING"));
        assertFalse(fired(Rules.evaluate(healthy()), "WATCH_DEAD"));
    }

    @Test
    void noLiveMembersAtAllIsCritical() {
        Sample sample = new Sample(NOW, null, List.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of(),
            List.of(), housekeeping(List.of(), List.of()), true, List.of());

        assertEquals(Alert.Severity.CRITICAL, only(Rules.evaluate(sample), "NO_SUPERVISOR_MEMBERS").severity());
    }

    @Test
    void aPassSpendingHalfItsIntervalIsReportedAgainstTheDeclaredCadence() {
        // The rule the member document had to exist for: a duration means nothing without the cadence it is
        // competing with, and the supervisor never knew its own -- the loop lives in the app.
        Convergence slow = new Convergence(POOL, 7, "s1", 100, NOW, 1500, 3, 3, 2, 5.0,
            0, Map.of(), NOW, 0, new Trend(20, 0), Map.of(), Tasks.NONE, false, 0, 0, null);
        Sample sample = new Sample(NOW, null, List.of("shared-a"), Map.of("shared-a", "s1"),
            Map.of("shared-a", slow), Map.of(), Map.of("s1", member(2000, "x")), Map.of(),
            List.of("s1"), housekeeping(List.of(), List.of()), true, List.of());

        assertTrue(only(Rules.evaluate(sample), "PASS_SATURATED").because().contains("1500ms against a declared"));
    }

    @Test
    void aPassAtItsWorkCeilingIsReportedEvenWhenItIsFast() {
        // Fast and truncated is a different failure from slow: the pool is converging at the rate of the ceiling.
        Convergence capped = new Convergence(POOL, 7, "s1", 100, NOW, 10, 3, 3, 2, 5.0,
            0, Map.of(), NOW, 0, new Trend(20, 0), Map.of(), Tasks.NONE, true, 0, 0, null);
        Sample sample = new Sample(NOW, null, List.of("shared-a"), Map.of("shared-a", "s1"),
            Map.of("shared-a", capped), Map.of(), Map.of("s1", member(2000, "x")), Map.of(),
            List.of("s1"), housekeeping(List.of(), List.of()), true, List.of());

        assertTrue(fired(Rules.evaluate(sample), "WORK_CEILING_HIT"));
    }

    @Test
    void aStableNonEmptyDiffIsTheAlarmRequirementsNames() {
        Sample sample = new Sample(NOW, null, List.of("shared-a"), Map.of("shared-a", "s1"),
            Map.of("shared-a", convergence(100, 12, 0, 300)), Map.of(),
            Map.of("s1", member(2000, "x")), Map.of(), List.of("s1"),
            housekeeping(List.of(), List.of()), true, List.of());

        assertTrue(only(Rules.evaluate(sample), "DIFF_STABLE_NON_EMPTY").because()
            .contains("quietly failing looks exactly like one with nothing to do"));
    }

    @Test
    void aShrinkingDiffIsRecoveryRatherThanAnAlarm() {
        Sample sample = new Sample(NOW, null, List.of("shared-a"), Map.of("shared-a", "s1"),
            Map.of("shared-a", convergence(100, 12, -4, 300)), Map.of(),
            Map.of("s1", member(2000, "x")), Map.of(), List.of("s1"),
            housekeeping(List.of(), List.of()), true, List.of());

        assertFalse(fired(Rules.evaluate(sample), "DIFF_STABLE_NON_EMPTY"));
    }

    // ---------------------------------------------------------------- thresholds on real findings

    private static Sample withFinding(String kind, long passes, String because, String unplaceable) {
        TenantDiff diff = new TenantDiff(ACME, POOL, 100, NOW, Map.of(kind, 1), false, 1, 1,
            List.of(new ProjectedFinding(kind, "warning", "acme/uzKx/1", because, unplaceable, 1, passes)));
        return new Sample(NOW, null, List.of("shared-a"), Map.of("shared-a", "s1"),
            Map.of("shared-a", convergence(100, 1, 0, 5)), Map.of("acme", diff),
            Map.of("s1", member(2000, "x")), Map.of(), List.of("s1"),
            housekeeping(List.of(), List.of()), true, List.of());
    }

    @Test
    void anAgedFindingBecomesAnAlertCarryingTheSupervisorsOwnSentence() {
        // The reason verbatim, never paraphrased. An alerter is one step removed from the reasoning that produced
        // the finding, so the moment it writes its own explanation there are two accounts and the more confident
        // one is further from the evidence.
        Alert alert = only(Rules.evaluate(withFinding("UNASSIGNED_PRIMARY", 40,
            "index [my-index] shard 1 has no primary", null)), "UNASSIGNED_PRIMARY");

        assertEquals(Alert.Severity.CRITICAL, alert.severity());
        assertEquals("acme/uzKx/1", alert.subject());
        assertTrue(alert.because().contains("index [my-index] shard 1 has no primary"), alert.because());
    }

    @Test
    void theRefusalIsPreferredBecauseItSaysWhatIsStoppingTheFix() {
        Alert alert = only(Rules.evaluate(withFinding("UNPLACEABLE", 40,
            "shard 1 wants a copy", "[disk] every candidate is above the high watermark")), "UNPLACEABLE");

        assertTrue(alert.because().contains("above the high watermark"),
            "the decider's sentence is the actionable half: " + alert.because());
    }

    @Test
    void aYoungFindingIsNotAnAlertAtAll() {
        assertEquals(List.of(), Rules.evaluate(withFinding("UNASSIGNED_PRIMARY", 3, "too new", null)));
    }

    @Test
    void aFindingThisBuildDoesNotKnowIsReportedAsSkewRatherThanIgnored() {
        List<Alert> alerts = Rules.evaluate(withFinding("SOMETHING_NEW", 500, "from a newer supervisor", null));

        Alert alert = only(alerts, "UNKNOWN_FINDING_KIND");
        assertEquals(Alert.Severity.INFO, alert.severity());
        assertTrue(alert.because().contains("going unjudged"), alert.because());
    }

    // ---------------------------------------------------------------- the silent states

    @Test
    void aNodeTheReaperKeepsDecliningIsReported() {
        // Declining to act produces no finding, so this state is otherwise completely silent -- and its commonest
        // cause is the one worth being told: a shard whose only started copy is on a machine nobody can reach.
        ReapingStatus reaping = new ReapingStatus(100, "s1", NOW, 0, 1, List.of(),
            List.of(new ReapingStatus.Kept("nodeAAA", "still the only node with a started copy of index uuid X")));
        Sample base = healthy();
        Sample sample = new Sample(NOW, null, base.poolsDefined(), base.leaseHolders(), base.convergence(),
            Map.of(), base.members(), Map.of("shared-a", reaping), base.liveMembers(), base.housekeeping(),
            true, List.of());

        Alert alert = only(Rules.evaluate(sample), "NODE_REAP_BLOCKED");
        assertEquals("nodeAAA", alert.subject());
        assertTrue(alert.because().contains("only node with a started copy"), alert.because());
    }

    @Test
    void anAbsentHousekeepingDocumentMeansTheWholeTierIsBlindRatherThanClean() {
        // The monitor-of-the-monitor rule. The housekeeping document is the only source for the unpooled-tenant
        // tier, so if its holder is dead those alerts go quiet -- and quiet must not be the same as clean.
        Sample sample = new Sample(NOW, null, List.of("shared-a"), Map.of("shared-a", "s1"),
            Map.of("shared-a", convergence(100, 0, 0, 0)), Map.of(),
            Map.of("s1", member(2000, "x")), Map.of(), List.of("s1"), null, false, List.of());

        assertTrue(only(Rules.evaluate(sample), "HOUSEKEEPING_ABSENT").because().contains("looks like idleness"));
    }

    @Test
    void anUnsupervisedTenantIsTheLoudestThingInTheHousekeepingTier() {
        Sample base = healthy();
        Sample sample = new Sample(NOW, null, base.poolsDefined(), base.leaseHolders(), base.convergence(),
            Map.of(), base.members(), Map.of(), base.liveMembers(),
            housekeeping(List.of("newco"), List.of()), true, List.of());

        Alert alert = only(Rules.evaluate(sample), "TENANT_UNSUPERVISED");
        assertEquals(Alert.Severity.CRITICAL, alert.severity());
        assertEquals("newco", alert.subject());
    }

    @Test
    void aTenantListFromConfigurationRatherThanTheRegistryIsWorthKnowing() {
        // An alert about another alert being blind: TENANT_UNSUPERVISED can only ever be as complete as the list
        // that produced it. The condition used to be written against a value that never occurs, so it fired on
        // every healthy cluster for ever.
        HousekeepingStatus fallback = new HousekeepingStatus(100, "s1", NOW, List.of(), List.of(), List.of(),
            List.of(), List.of(), List.of(), 1, 0, "configuration", List.of());
        Sample base = healthy();
        Sample sample = new Sample(NOW, null, base.poolsDefined(), base.leaseHolders(), base.convergence(),
            Map.of(), base.members(), Map.of(), base.liveMembers(), fallback, true, List.of());

        assertEquals(Alert.Severity.INFO, only(Rules.evaluate(sample), "REGISTRY_IS_FALLBACK").severity());
        assertFalse(fired(Rules.evaluate(healthy()), "REGISTRY_IS_FALLBACK"),
            "and it must not fire when the registry is the source");
    }

    // ---------------------------------------------------------------- output contract

    @Test
    void theWorstAlertDecidesTheExitStatus() {
        assertEquals(0, Rules.evaluate(healthy()).stream().mapToInt(Alert::exitStatus).max().orElse(0));

        List<Alert> warning = Rules.evaluate(withFinding("ORPHANED_COPY", 100, "held for ever", null));
        assertEquals(1, warning.stream().mapToInt(Alert::exitStatus).max().orElse(0));

        List<Alert> critical = Rules.evaluate(withFinding("UNASSIGNED_PRIMARY", 100, "no primary", null));
        assertEquals(2, critical.stream().mapToInt(Alert::exitStatus).max().orElse(0));
    }

    @Test
    void alertsAreOrderedWorstFirst() {
        TenantDiff diff = new TenantDiff(ACME, POOL, 100, NOW, Map.of(), false, 2, 2, List.of(
            new ProjectedFinding("ORPHANED_COPY", "warning", "acme/a/0", "orphan", null, 1, 100),
            new ProjectedFinding("UNASSIGNED_PRIMARY", "critical", "acme/b/0", "no primary", null, 1, 100)
        ));
        Sample base = healthy();
        Sample sample = new Sample(NOW, null, base.poolsDefined(), base.leaseHolders(), base.convergence(),
            Map.of("acme", diff), base.members(), Map.of(), base.liveMembers(), base.housekeeping(),
            true, List.of());

        List<Alert> alerts = Rules.evaluate(sample);
        assertEquals(Alert.Severity.CRITICAL, alerts.get(0).severity(),
            "a truncated terminal must show the worst thing first: " + alerts);
    }
}
