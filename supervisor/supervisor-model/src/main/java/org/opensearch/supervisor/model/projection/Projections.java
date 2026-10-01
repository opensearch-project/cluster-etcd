package org.opensearch.supervisor.model.projection;

import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.plan.Plan;

import java.util.List;
import java.util.Map;

/**
 * The documents this service writes about itself, as data.
 *
 * <p>Here rather than nested inside the classes that write them, and the reason is structural. Anything that
 * <em>judges</em> these documents — whether a finding has gone on long enough to wake someone, whether a pool has
 * stopped advancing — must not be able to read a key it forgot to ask for, exactly as {@code Differ} and
 * {@code Planner} must not. {@code supervisor-model} cannot see a {@code KeyValueStore}, so putting the records
 * here is what lets the rule library inherit that guarantee instead of promising it.
 *
 * <p>It also closes the gap that let their schema rot: for a long time <b>nothing read them back</b>, so the only
 * specification was prose in {@code SUPERVISOR_KEYS.md} and it drifted in both directions. Each record now has a
 * {@code from(byte[])} beside its writer, which is what a reader needs and what a round trip pins.
 */
public final class Projections {

    private Projections() {}

    /** What a projected diff looks like on the wire. {@code supervisor/status/diff/<tenant-id>}. */
    public record TenantDiff(
        TenantId tenant,
        PoolId pool,
        long pass,
        long observedAt,
        Map<String, Integer> counts,
        boolean truncated,
        int shown,
        int total,
        List<ProjectedFinding> findings
    ) {
        public byte[] toBytes() {
            return Json.write(this, "tenant diff");
        }

        public static TenantDiff from(byte[] bytes) {
            return Json.read(bytes, TenantDiff.class, "tenant diff");
        }
    }

    /**
     * @param unplaceableBecause why this finding could not be acted on, when something tried and declined.
     *     Kept beside {@code because} rather than folded into it, because the two answer different questions:
     *     one says what is wrong, the other says what is stopping it being fixed, and an operator needs both.
     *     <p>The name is narrower than the meaning, and deliberately left alone: {@code unplaceable_because} is a
     *     published field, and it also carries {@code OrphanReaper}'s and {@code Displacer}'s reasons for
     *     declining to <em>remove</em> a copy. Any rule that refuses to act on a finding files its sentence here.
     */
    public record ProjectedFinding(
        String kind,
        String severity,
        String subject,
        String because,
        String unplaceableBecause,
        long firstObservedPass,
        long passes
    ) {}

    /**
     * Per-pool convergence: the answer to "is this getting better?", which one number cannot give.
     *
     * @param epoch the lease epoch this pass ran under, so a split brain is diagnosable afterwards from data the
     *     system already wrote
     * @param workCeilingHit whether this pass ran out of its budget before it ran out of work. The capacity signal
     *     that pairs with {@code passDurationMillis}: a pool always at its ceiling is converging at the rate of the
     *     ceiling rather than at the rate of the cluster.
     */
    public record Convergence(
        PoolId pool,
        long epoch,
        String holder,
        long pass,
        long passStartedAt,
        long passDurationMillis,
        int nodes,
        int placeableNodes,
        int indices,
        double weight,
        int findingsTotal,
        Map<String, Integer> findingsByKind,
        long lastEmptyDiffAt,
        long passesSinceEmpty,
        Trend trend,
        Map<String, TenantSummary> tenants,
        Tasks tasks,
        boolean workCeilingHit,
        int unreadableDocuments,
        long watchPollFailures,
        Plan.Balance balance
    ) {
        public byte[] toBytes() {
            return Json.write(this, "convergence");
        }

        public static Convergence from(byte[] bytes) {
            return Json.read(bytes, Convergence.class, "convergence");
        }
    }

    /**
     * What the queue did this pass.
     *
     * <p>Worth publishing because a queue that has stopped draining is invisible otherwise: the control plane
     * would keep reporting a growing pile through {@code cluster.pending_tasks} and nothing would say why.
     */
    public record Tasks(int claimed, int completed, int failed, int reaped, int lost) {
        public static final Tasks NONE = new Tasks(0, 0, 0, 0, 0);
    }

    /** Per tenant, capped at the worst few so this cannot grow with tenant count. */
    public record TenantSummary(int findings, long oldestPasses) {}

    /**
     * @param delta change in finding count across the window. Negative is recovery; zero with a non-empty
     *     diff is the alarm {@code REQUIREMENTS.md} names — a reconciler that is quietly failing looks
     *     exactly like one with nothing to do.
     */
    public record Trend(int window, int delta) {}

    /** {@code supervisor/status/housekeeping} — everything that belongs to no pool. */
    public record HousekeepingStatus(
        long pass,
        String holder,
        long observedAt,
        List<String> unpooledTenants,
        List<String> unpooledNodes,
        List<String> unownedPools,
        List<String> poolsWithoutDefinition,
        List<String> duplicateDefaults,
        List<String> overloadedDedicatedPools,
        int membersLive,
        int membersReaped,
        String registrySource,
        List<String> unreadable
    ) {
        public byte[] toBytes() {
            return Json.write(this, "housekeeping");
        }

        public static HousekeepingStatus from(byte[] bytes) {
            return Json.read(bytes, HousekeepingStatus.class, "housekeeping");
        }
    }
}
