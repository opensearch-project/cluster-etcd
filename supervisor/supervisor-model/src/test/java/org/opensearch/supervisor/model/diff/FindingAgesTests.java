package org.opensearch.supervisor.model.diff;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Finding age is the only cross-pass memory the diff has, and it is what makes the alarm expressible: a
 * reconciler that is quietly failing looks exactly like one with nothing to do, so "twelve findings" is not
 * an answer and "twelve findings, unchanged for three hundred passes" is.
 */
class FindingAgesTests {

    private static final TenantId ACME = TenantId.of("acme");
    private static final Subject SUBJECT = Subject.ofShard(ACME, "uzKxLWZu4nPSrTknEcDpoA", 0);

    private static List<Finding> one(FindingKind kind, long pass) {
        return List.of(Finding.of(kind, SUBJECT, pass, "because"));
    }

    @Test
    void aFindingSeenOnceIsOnePassOld() {
        FindingAges ages = new FindingAges();
        Finding aged = ages.age(one(FindingKind.UNASSIGNED_PRIMARY, 10), 10).get(0);

        assertEquals(10, aged.firstObservedPass());
        assertEquals(1, aged.passes());
    }

    @Test
    void aFindingThatPersistsGrowsOlder() {
        FindingAges ages = new FindingAges();
        ages.age(one(FindingKind.UNASSIGNED_PRIMARY, 10), 10);
        ages.age(one(FindingKind.UNASSIGNED_PRIMARY, 11), 11);
        Finding aged = ages.age(one(FindingKind.UNASSIGNED_PRIMARY, 12), 12).get(0);

        assertEquals(10, aged.firstObservedPass());
        assertEquals(3, aged.passes());
    }

    @Test
    void aFindingThatClearsLosesItsHistory() {
        // The same problem recurring later is a new problem, not an apparently ancient one -- otherwise a
        // flapping shard would look like a permanently stuck one, which is a different diagnosis. This is
        // also what bounds the map.
        FindingAges ages = new FindingAges();
        ages.age(one(FindingKind.UNASSIGNED_PRIMARY, 10), 10);
        ages.age(List.of(), 11);
        assertEquals(0, ages.size());

        Finding recurred = ages.age(one(FindingKind.UNASSIGNED_PRIMARY, 12), 12).get(0);
        assertEquals(12, recurred.firstObservedPass());
        assertEquals(1, recurred.passes());
    }

    @Test
    void differentKindsOnTheSameSubjectAgeSeparately() {
        FindingAges ages = new FindingAges();
        ages.age(one(FindingKind.UNASSIGNED_PRIMARY, 10), 10);

        List<Finding> both = ages.age(List.of(
            Finding.of(FindingKind.UNASSIGNED_PRIMARY, SUBJECT, 11, "because"),
            Finding.of(FindingKind.UNASSIGNED_REPLICA, SUBJECT, 11, "because")
        ), 11);

        assertEquals(2, both.stream().filter(f -> f.kind() == FindingKind.UNASSIGNED_PRIMARY).findFirst().orElseThrow().passes());
        assertEquals(1, both.stream().filter(f -> f.kind() == FindingKind.UNASSIGNED_REPLICA).findFirst().orElseThrow().passes());
    }

    @Test
    void expectedTornStateEscalatesOnAgeRatherThanOnPresence() {
        // R2.5 promises the supervisor will see an index without its alias. Alarming the first time is how
        // a correct system earns a reputation for false alarms.
        assertEquals(Severity.INFO, FindingKind.TORN_DESIRED_STATE.severityAfter(1));
        assertEquals(Severity.INFO, FindingKind.TORN_DESIRED_STATE.severityAfter(FindingKind.ESCALATE_AFTER_PASSES - 1));
        assertEquals(Severity.WARNING, FindingKind.TORN_DESIRED_STATE.severityAfter(FindingKind.ESCALATE_AFTER_PASSES));
    }

    @Test
    void aFindingBornSeriousDoesNotWaitToBeTakenSeriously() {
        assertEquals(Severity.CRITICAL, FindingKind.UNASSIGNED_PRIMARY.severityAfter(1));
    }

    @Test
    void subjectKeysAreStableAcrossPasses() {
        // Age and oscillation detection both key on this. A subject whose rendering changed between passes
        // would reset its own age every pass and never look old, silently disabling the only alarm here.
        assertEquals(
            Subject.ofShard(ACME, "uu", 0).key(),
            Subject.ofShard(TenantId.of("acme"), "uu", 0).key()
        );
        assertEquals("acme/uu/0", Subject.ofShard(ACME, "uu", 0).key());
        assertEquals("acme/uu/0@n1", Subject.ofShardCopy(ACME, "uu", 0, NodeId.of("n1")).key());
        assertEquals("acme", Subject.ofTenant(ACME).key());
    }
}
