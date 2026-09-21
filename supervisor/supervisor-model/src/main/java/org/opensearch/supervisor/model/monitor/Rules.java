package org.opensearch.supervisor.model.monitor;

import org.opensearch.supervisor.model.projection.MemberStatus;
import org.opensearch.supervisor.model.projection.Projections.Convergence;
import org.opensearch.supervisor.model.projection.Projections.ProjectedFinding;
import org.opensearch.supervisor.model.projection.Projections.TenantDiff;
import org.opensearch.supervisor.model.projection.ReapingStatus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Every rule, as one pure function from a {@link Sample} to alerts.
 *
 * <p>Split into the two kinds §5 of {@code MONITORING.md} distinguishes, because they carry different risk:
 *
 * <ul>
 *   <li><b>Thresholds on findings the supervisor already raised</b>, which is most of them and is a table
 *       ({@link Thresholds}) rather than logic. Nothing here re-derives whether anything is wrong.
 *   <li><b>The few things a pass cannot see about itself</b> — a pool that has stopped advancing, a lease nobody
 *       holds, a diff that is stable and non-empty. These need a wall clock and a previous sample, which is exactly
 *       what the supervisor cannot have: its ages are in passes, and passes stop when it does.
 * </ul>
 *
 * <p>The second group is the justification for the component existing at all. The first group is why it is cheap.
 */
public final class Rules {

    /**
     * How far past the declared pass interval a pool may go without advancing before it counts as stuck.
     *
     * <p>Generous, because the alternative is worse: a pool legitimately takes longer than its interval when a
     * pass has real work in it, and `Runner` says so in as many words — it logs and starts the next pass
     * immediately rather than queueing. A tighter multiple would fire during exactly the recovery it is meant to
     * survive.
     */
    public static final int NOT_ADVANCING_INTERVALS = 10;

    /** Below this, "the pass number has not moved" is just two samples taken close together. */
    public static final Duration NOT_ADVANCING_FLOOR = Duration.ofSeconds(30);

    /** How stale the housekeeping document may be before its whole tier is assumed blind. */
    public static final Duration HOUSEKEEPING_STALE = Duration.ofMinutes(5);

    /** Passes of a stable, non-empty diff before it is the alarm R7.2 names rather than slow progress. */
    public static final long DIFF_STABLE_PASSES = 100;

    private Rules() {}

    public static List<Alert> evaluate(Sample sample) {
        List<Alert> alerts = new ArrayList<>();

        poolsUnsupervised(sample, alerts);
        poolsNotAdvancing(sample, alerts);
        noMembers(sample, alerts);
        passSaturated(sample, alerts);
        watchFailures(sample, alerts);
        diffStableAndNonEmpty(sample, alerts);
        findingThresholds(sample, alerts);
        reapBlocked(sample, alerts);
        housekeepingTier(sample, alerts);
        unreadable(sample, alerts);

        alerts.sort(Alert.BY_URGENCY);
        return List.copyOf(alerts);
    }

    // ------------------------------------------------- things a pass cannot see about itself

    /**
     * A pool an operator declared that nobody is supervising.
     *
     * <p>The denominator rule, and the one that needs `pools/definitions/` rather than any status document: a pool
     * with no convergence document is a pool that produces no output at all, which is indistinguishable from a
     * quiet one if you only read what supervisors write.
     */
    private static void poolsUnsupervised(Sample sample, List<Alert> alerts) {
        for (String pool : sample.poolsDefined()) {
            if (sample.leaseHolders().containsKey(pool) == false) {
                alerts.add(new Alert(Alert.Severity.CRITICAL, "POOL_UNSUPERVISED", pool,
                    "a pool definition exists and no live supervisor holds its lease, so nothing places shards,"
                        + " computes health or drains tasks for it — and none of that produces an error anywhere"));
            }
        }
    }

    /**
     * A lease held, and the pass number not moving.
     *
     * <p><b>The alert this whole component exists for.</b> It is revision-silence liveness — the mechanism this
     * system already trusts for nodes — applied one layer up, to the supervisor. Nothing inside a supervisor can
     * fire it: a wedged process, a pass that died between two steps and a pool with nothing to do all present as
     * documents that stop changing.
     */
    private static void poolsNotAdvancing(Sample sample, List<Alert> alerts) {
        long interval = sample.declaredPassIntervalMillis();
        long window = Math.max(NOT_ADVANCING_FLOOR.toMillis(), interval * NOT_ADVANCING_INTERVALS);
        if (sample.previous() == null || sample.sinceMillis() < window) {
            return;
        }
        sample.convergence().forEach((pool, now) -> {
            Convergence before = sample.previousConvergenceOf(pool);
            if (before == null || before.pass() != now.pass()) {
                return;
            }
            alerts.add(new Alert(Alert.Severity.CRITICAL, "POOL_NOT_ADVANCING", pool,
                "holder [" + now.holder() + "] still holds this pool and its pass number has not moved from "
                    + now.pass() + " in " + sample.sinceMillis() + "ms; a supervisor that has stopped looks"
                    + " exactly like one with nothing to do, which is why this is measured from outside"));
        });
    }

    private static void noMembers(Sample sample, List<Alert> alerts) {
        if (sample.liveMembers().isEmpty()) {
            alerts.add(new Alert(Alert.Severity.CRITICAL, "NO_SUPERVISOR_MEMBERS", "-",
                "no supervisor has a live member record, so no pool is owned and nothing in this system is"
                    + " reconciling anything"));
        }
    }

    /**
     * A pass spending most of its interval.
     *
     * <p>The capacity signal, and the reason the member document had to exist: a duration means nothing without
     * the cadence it competes with. Reads {@code work_ceiling_hit} as well, because the two failures look the same
     * from a duration alone — a pool at its ceiling is converging at the rate of the ceiling.
     */
    private static void passSaturated(Sample sample, List<Alert> alerts) {
        long interval = sample.declaredPassIntervalMillis();
        if (interval <= 0) {
            return;
        }
        sample.convergence().forEach((pool, convergence) -> {
            if (convergence.passDurationMillis() * 2 >= interval) {
                alerts.add(new Alert(Alert.Severity.WARNING, "PASS_SATURATED", pool,
                    "a pass took " + convergence.passDurationMillis() + "ms against a declared interval of "
                        + interval + "ms, so this pool is close to outgrowing its supervisor"
                        + (convergence.workCeilingHit() ? " and is also hitting its work ceiling" : "")));
            } else if (convergence.workCeilingHit()) {
                alerts.add(new Alert(Alert.Severity.WARNING, "WORK_CEILING_HIT", pool,
                    "this pass ran out of its budget before it ran out of work, so the pool is converging at the"
                        + " rate of the ceiling rather than at the rate of the cluster"));
            }
        });
    }

    /**
     * A watch that has stopped delivering.
     *
     * <p>Read against the backend, because the counter stays at zero by design on the backends that watch
     * natively — so a rising count is only meaningful where polling is how watching works, and a flat zero is not
     * evidence of health anywhere. {@code REQUIREMENTS.md}: a silently dead watch looks exactly like a quiet
     * keyspace.
     */
    private static void watchFailures(Sample sample, List<Alert> alerts) {
        sample.convergence().forEach((pool, now) -> {
            Convergence before = sample.previousConvergenceOf(pool);
            if (before == null || now.watchPollFailures() <= before.watchPollFailures()) {
                return;
            }
            String backend = sample.members().values().stream()
                .map(MemberStatus::storeBackend)
                .findFirst()
                .orElse("unknown");
            alerts.add(new Alert(Alert.Severity.WARNING, "WATCH_DEAD", pool,
                "watch poll failures rose from " + before.watchPollFailures() + " to " + now.watchPollFailures()
                    + " on backend [" + backend + "]; the pass loop still runs on its timer, so this costs"
                    + " latency rather than correctness — but a dead watch is invisible otherwise"));
        });
    }

    /**
     * A diff that is not getting better.
     *
     * <p>R7.2 stated as code: <em>"A diff that is stable and non-empty is the alarm."</em> The signal has existed
     * since the projections did — {@code Trend}'s own javadoc says zero with a non-empty diff is the alarm — and
     * nothing has ever evaluated it.
     */
    private static void diffStableAndNonEmpty(Sample sample, List<Alert> alerts) {
        sample.convergence().forEach((pool, convergence) -> {
            if (convergence.findingsTotal() == 0
                || convergence.trend() == null
                || convergence.trend().delta() < 0
                || convergence.passesSinceEmpty() < DIFF_STABLE_PASSES) {
                return;
            }
            alerts.add(new Alert(Alert.Severity.WARNING, "DIFF_STABLE_NON_EMPTY", pool,
                convergence.findingsTotal() + " findings, unchanged or growing across the last "
                    + convergence.trend().window() + " passes and non-empty for " + convergence.passesSinceEmpty()
                    + "; a reconciler that is quietly failing looks exactly like one with nothing to do"));
        });
    }

    // ------------------------------------------------- thresholds on what the supervisor already found

    /**
     * Every projected finding, against {@link Thresholds}.
     *
     * <p>Carries the supervisor's own {@code because} through untouched, and prefers {@code unplaceable_because}
     * where there is one — that field says what is *stopping* the fix, which is what an operator acts on.
     */
    private static void findingThresholds(Sample sample, List<Alert> alerts) {
        sample.diffs().forEach((tenant, diff) -> {
            for (ProjectedFinding finding : diff.findings()) {
                if (Thresholds.knows(finding.kind()) == false) {
                    alerts.add(new Alert(Alert.Severity.INFO, "UNKNOWN_FINDING_KIND", finding.subject(),
                        "the supervisor raised [" + finding.kind() + "], which this monitor build has no threshold"
                            + " for; it is newer than this binary and the finding is going unjudged"));
                    continue;
                }
                Thresholds.firesAt(finding.kind(), finding.passes()).ifPresent(severity ->
                    alerts.add(new Alert(severity, finding.kind(), finding.subject(),
                        finding.passes() + " passes: " + reasonOf(finding))));
            }
        });
    }

    private static String reasonOf(ProjectedFinding finding) {
        if (finding.unplaceableBecause() == null || finding.unplaceableBecause().isBlank()) {
            return finding.because();
        }
        return finding.because() + " — " + finding.unplaceableBecause();
    }

    /**
     * A node the reaper keeps declining to delete.
     *
     * <p>Declining to act produces no finding, so this state is otherwise completely silent — and the commonest
     * reason for it is the one worth being told: a shard whose only started copy is on a machine nobody can reach.
     */
    private static void reapBlocked(Sample sample, List<Alert> alerts) {
        sample.reaping().forEach((pool, reaping) -> {
            for (ReapingStatus.Kept kept : reaping.kept()) {
                alerts.add(new Alert(Alert.Severity.WARNING, "NODE_REAP_BLOCKED", kept.node(),
                    "in pool [" + pool + "]: " + kept.because()));
            }
        });
    }

    // ------------------------------------------------- the tier that belongs to nobody

    /**
     * Everything that belongs to no pool, plus the rule that notices this tier has gone blind.
     *
     * <p>{@code HOUSEKEEPING_ABSENT} is the monitor-of-the-monitor: the housekeeping document is the only source
     * for the rest of this method, so if its holder is dead these alerts go quiet rather than firing. **Quiet must
     * not be the same as clean.**
     */
    private static void housekeepingTier(Sample sample, List<Alert> alerts) {
        var housekeeping = sample.housekeeping();
        if (housekeeping == null
            || sample.housekeepingLeaseHeld() == false
            || sample.at() - housekeeping.observedAt() > HOUSEKEEPING_STALE.toMillis()) {
            alerts.add(new Alert(Alert.Severity.WARNING, "HOUSEKEEPING_ABSENT", "-",
                housekeeping == null
                    ? "no housekeeping document exists, so nothing is checking for tenants and nodes that belong"
                        + " to no pool — and every one of those looks like idleness"
                    : "the housekeeping document is " + (sample.at() - housekeeping.observedAt())
                        + "ms old or its lease is unheld, so this whole tier of checks is silent rather than clean"));
            if (housekeeping == null) {
                return;
            }
        }

        for (String tenant : housekeeping.unpooledTenants()) {
            alerts.add(new Alert(Alert.Severity.CRITICAL, "TENANT_UNSUPERVISED", tenant,
                "a registered tenant with no pool binding: no shards are placed, no health is computed and no"
                    + " tasks are reaped for it, and every one of those looks like idleness"));
        }
        for (String node : housekeeping.unpooledNodes()) {
            alerts.add(new Alert(Alert.Severity.WARNING, "NODE_UNPOOLED", node,
                "the node declares no pool and has no override, so no supervisor owns it and it will be given"
                    + " nothing"));
        }
        for (String pool : housekeeping.poolsWithoutDefinition()) {
            alerts.add(new Alert(Alert.Severity.WARNING, "POOL_WITHOUT_DEFINITION", pool,
                "something names this pool and no definition exists for it, which is a typo that presents as"
                    + " nodes quietly going unmanaged"));
        }
        for (String pool : housekeeping.duplicateDefaults()) {
            alerts.add(new Alert(Alert.Severity.WARNING, "DUPLICATE_DEFAULT_POOL", pool,
                "more than one pool claims to be the default, so which one an unbound tenant lands in is"
                    + " whichever was read first"));
        }
        if ("registry".equals(housekeeping.registrySource()) == false) {
            alerts.add(new Alert(Alert.Severity.INFO, "REGISTRY_IS_FALLBACK", "-",
                "the tenant list came from [" + housekeeping.registrySource() + "] rather than the registry, so"
                    + " TENANT_UNSUPERVISED can only ever be as complete as a configuration file"));
        }
    }

    private static void unreadable(Sample sample, List<Alert> alerts) {
        for (String key : sample.unreadable()) {
            alerts.add(new Alert(Alert.Severity.WARNING, "UNREADABLE_PROJECTION", key,
                "this monitor could not parse a document the supervisor wrote, which is a version skew or a codec"
                    + " bug — and it means some of the checks above ran on less than they should have"));
        }
    }
}
