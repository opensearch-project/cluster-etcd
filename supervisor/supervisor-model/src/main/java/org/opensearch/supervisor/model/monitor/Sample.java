package org.opensearch.supervisor.model.monitor;

import org.opensearch.supervisor.model.projection.MemberStatus;
import org.opensearch.supervisor.model.projection.Projections.Convergence;
import org.opensearch.supervisor.model.projection.Projections.HousekeepingStatus;
import org.opensearch.supervisor.model.projection.Projections.TenantDiff;
import org.opensearch.supervisor.model.projection.ReapingStatus;

import java.util.List;
import java.util.Map;

/**
 * Everything the rules get: one read of the store, parsed, plus the wall clock.
 *
 * <p>A record rather than a store handle, for the reason {@code Differ} takes an {@code Observation}: a rule that
 * could read a key would be a rule that could read a key it forgot to ask for, and this whole library's value is
 * that it is a pure function testable from two literals. {@code supervisor-model} cannot see a
 * {@code KeyValueStore}, so that is structural here rather than a promise.
 *
 * @param at wall clock, and the one thing the supervisor's own documents cannot supply. Ages inside the supervisor
 *     are counted in <em>passes</em>, which do not survive a restart and are not comparable across processes —
 *     {@code DESIGN.md} and {@code FindingAges} both say so. So "this pool has not advanced in four minutes" is a
 *     sentence only something outside the supervisor can form.
 * @param previous the last sample this monitor took, or null on a first run. The whole point of the second stage:
 *     a pass cannot report its own absence, so noticing that {@code convergence.pass} has not moved requires two
 *     observations and a clock. Absent is not an error — a first run simply cannot fire the rules that need it.
 * @param poolsDefined every pool an operator has declared, which is the denominator. A pool with no convergence
 *     document is invisible without it, and that is the most important thing this component detects.
 * @param leaseHolders pool id to the id of whoever holds its lease, or absent when nobody does
 */
public record Sample(
    long at,
    Sample previous,
    List<String> poolsDefined,
    Map<String, String> leaseHolders,
    Map<String, Convergence> convergence,
    Map<String, TenantDiff> diffs,
    Map<String, MemberStatus> members,
    Map<String, ReapingStatus> reaping,
    List<String> liveMembers,
    HousekeepingStatus housekeeping,
    boolean housekeepingLeaseHeld,
    List<String> unreadable
) {

    public Sample {
        poolsDefined = poolsDefined == null ? List.of() : List.copyOf(poolsDefined);
        leaseHolders = leaseHolders == null ? Map.of() : Map.copyOf(leaseHolders);
        convergence = convergence == null ? Map.of() : Map.copyOf(convergence);
        diffs = diffs == null ? Map.of() : Map.copyOf(diffs);
        members = members == null ? Map.of() : Map.copyOf(members);
        reaping = reaping == null ? Map.of() : Map.copyOf(reaping);
        liveMembers = liveMembers == null ? List.of() : List.copyOf(liveMembers);
        unreadable = unreadable == null ? List.of() : List.copyOf(unreadable);
    }

    /**
     * The previous sample's convergence for a pool, or null.
     *
     * <p>Chained one deep and no further on purpose. Keeping a history here would make the monitor stateful in the
     * way §1.1 says the <em>supervisor</em> must not be, and the one comparison the rules need — has this number
     * moved since last time — needs exactly one predecessor.
     */
    public Convergence previousConvergenceOf(String pool) {
        return previous == null ? null : previous.convergence().get(pool);
    }

    /** How long since the previous sample, or zero when there is none. */
    public long sinceMillis() {
        return previous == null ? 0 : at - previous.at();
    }

    /** The pass interval any member declared, for rules that need a cadence. Zero when nothing published one. */
    public long declaredPassIntervalMillis() {
        return members.values().stream()
            .mapToLong(MemberStatus::passIntervalMillis)
            .max()
            .orElse(0L);
    }
}
