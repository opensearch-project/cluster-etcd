package org.opensearch.supervisor.model.monitor;

import org.opensearch.supervisor.model.diff.FindingKind;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * How long each kind of finding may stand before somebody is told.
 *
 * <p><b>The table is the artifact.</b> Most alerts are not new predicates — they are age thresholds on findings
 * the supervisor already raises, with a severity it already assigns — so the bulk of this library is a table
 * rather than a detector. That is deliberate and is what keeps it from becoming a second differ: a rule that
 * recomputed whether a shard is unassigned would be a second implementation of the thing most worth having only
 * one of.
 *
 * <p>Being able to argue about a number in one place is most of the value. The numbers below are a starting point.
 *
 * <p>The severities here are <b>not</b> {@code FindingKind}'s. A finding's severity says how bad the situation is;
 * an alert's says whether to wake someone, and they genuinely differ — {@code NODE_LOST} is `CRITICAL` as a
 * finding because a node's copies have stopped counting, and is a `WARNING` as an alert because the supervisor is
 * already replacing them and only its <em>persistence</em> needs a human.
 */
public final class Thresholds {

    /** A finding kind nobody should ever be woken for, however old. */
    public static final long NEVER = -1;

    private record Rule(long passes, Alert.Severity severity) {}

    private static final Map<FindingKind, Rule> RULES = rules();

    private Thresholds() {}

    private static Map<FindingKind, Rule> rules() {
        Map<FindingKind, Rule> rules = new LinkedHashMap<>();

        // Data is unreachable, not merely un-redundant.
        rules.put(FindingKind.UNASSIGNED_PRIMARY, new Rule(30, Alert.Severity.CRITICAL));
        // The actionable one: `unplaceable_because` names the decider that refused.
        rules.put(FindingKind.UNPLACEABLE, new Rule(30, Alert.Severity.CRITICAL));

        // Configuration errors, never races, so they do not need patience.
        rules.put(FindingKind.FOREIGN_COPY, new Rule(5, Alert.Severity.CRITICAL));
        rules.put(FindingKind.POOL_REPOSITORY_MISMATCH, new Rule(5, Alert.Severity.CRITICAL));
        rules.put(FindingKind.TENANT_UNPOOLED, new Rule(5, Alert.Severity.CRITICAL));
        rules.put(FindingKind.POOL_UNOWNED, new Rule(5, Alert.Severity.CRITICAL));

        // Loudest thing in the table, out of proportion to its WARNING severity as a finding. A node with a
        // heartbeat and no registration is placeable for nothing and does not recover on its own -- except on a
        // data plane implementing the registration-rewrite rule, and a supervisor may not assume one does.
        rules.put(FindingKind.NODE_UNREGISTERED, new Rule(2, Alert.Severity.CRITICAL));

        // The supervisor is already acting on these; it is persistence that needs a human.
        rules.put(FindingKind.NODE_LOST, new Rule(10, Alert.Severity.WARNING));
        rules.put(FindingKind.NODE_UNPOOLED, new Rule(10, Alert.Severity.WARNING));

        // Recovery is slow and legitimate, so the patience is long.
        rules.put(FindingKind.UNASSIGNED_REPLICA, new Rule(100, Alert.Severity.WARNING));
        rules.put(FindingKind.UNASSIGNED_SEARCH_REPLICA, new Rule(100, Alert.Severity.WARNING));

        // Past OrphanReaper's own thirty-pass patience, so this fires only when the removal is not happening.
        rules.put(FindingKind.ORPHANED_COPY, new Rule(60, Alert.Severity.WARNING));

        // One per deleted index is the signature of a copy nothing will let go of.
        rules.put(FindingKind.TASK_STUCK, new Rule(10, Alert.Severity.WARNING));

        rules.put(FindingKind.PROJECTION_STALE, new Rule(100, Alert.Severity.WARNING));

        // A version skew or a codec bug. Either way somebody has to look at bytes.
        rules.put(FindingKind.UNREADABLE_DOCUMENT, new Rule(5, Alert.Severity.WARNING));
        rules.put(FindingKind.UNRECOGNISED_STATE, new Rule(5, Alert.Severity.WARNING));

        // A request that will never be honoured. Not urgent, and it must not be silent.
        rules.put(FindingKind.DESIRED_COPY_IGNORED, new Rule(30, Alert.Severity.WARNING));

        // Most likely this service reaped a live node. The finding is already windowed on the age of the last
        // rewrite, so one pass is enough patience and a longer wait would just delay it.
        rules.put(FindingKind.NODE_REGISTRATION_REWRITTEN, new Rule(1, Alert.Severity.WARNING));

        // Mid-convergence states. Deliberately absent rather than set high: both are what a healthy pool looks
        // like while it works, and the pathology is them *oscillating*, which is a different detector.
        rules.put(FindingKind.SURPLUS_COPY, new Rule(NEVER, Alert.Severity.INFO));
        rules.put(FindingKind.PRIMARY_BEFORE_REPLICA, new Rule(NEVER, Alert.Severity.INFO));
        rules.put(FindingKind.NODE_STALE, new Rule(NEVER, Alert.Severity.INFO));

        // R2.5 promises the supervisor will see an index without its alias. Alerting on it is the classic mistake
        // this whole design is arranged against: the alarm is on age, and the differ already escalates this one.
        rules.put(FindingKind.TORN_DESIRED_STATE, new Rule(NEVER, Alert.Severity.INFO));

        return rules;
    }

    /**
     * The alert a finding of this kind and age deserves, if any.
     *
     * <p>An unknown kind — a supervisor newer than this build — returns empty rather than guessing. A monitor that
     * invented a threshold for a finding it had never heard of would be alerting on a definition it does not have.
     * The unknown kind is reported by {@link Rules} as its own alert instead, once, so it cannot pass silently.
     */
    public static Optional<Alert.Severity> firesAt(String kind, long passes) {
        FindingKind parsed;
        try {
            parsed = FindingKind.valueOf(kind);
        } catch (IllegalArgumentException unknown) {
            return Optional.empty();
        }
        Rule rule = RULES.get(parsed);
        if (rule == null || rule.passes() == NEVER || passes < rule.passes()) {
            return Optional.empty();
        }
        return Optional.of(rule.severity());
    }

    /** Whether this build has an opinion about a kind at all, which is what tells skew from silence. */
    public static boolean knows(String kind) {
        try {
            return RULES.containsKey(FindingKind.valueOf(kind));
        } catch (IllegalArgumentException unknown) {
            return false;
        }
    }

    /** Every kind this build knows, so a test can assert the table covers the enum. */
    public static Map<FindingKind, Long> declared() {
        Map<FindingKind, Long> declared = new LinkedHashMap<>();
        RULES.forEach((kind, rule) -> declared.put(kind, rule.passes()));
        return declared;
    }
}
