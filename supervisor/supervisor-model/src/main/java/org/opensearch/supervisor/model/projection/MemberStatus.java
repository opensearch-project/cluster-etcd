package org.opensearch.supervisor.model.projection;

import org.opensearch.supervisor.model.Json;

import java.util.List;
import java.util.Map;

/**
 * {@code supervisor/status/members/<supervisor-id>} — what one supervisor process is <em>actually</em> running with.
 *
 * <p><b>R3.3 requires the effective liveness threshold to be observable</b>, and for a long time it was not: the
 * values were real, configurable, and published nowhere. `SUPERVISOR_KEYS.md` described this document in the
 * present tense while `Keys.memberStatus` was a key builder with no caller — which is the drift the schema test now
 * catches, and this is the half of it that was a missing feature rather than a stale sentence.
 *
 * <p>Per member rather than global, because two supervisors can be running different builds with different
 * settings, and that is itself the thing worth being able to see. "What is this process actually running with" is
 * the first question of every incident, and the second is "and is the one next to it running the same".
 *
 * <p>Distinct from {@code supervisor/members/<supervisor-id>}, which is the membership heartbeat — who is alive and
 * which pools they hold. That one is on the coordination path and is rewritten constantly; this one changes when a
 * process restarts with different configuration, which is almost never.
 *
 * @param effectiveLivenessMillis the thresholds as they apply to real nodes, which is the part R3.3 is about: the
 *     multipliers and the floor are inputs, and what an operator needs is the number of milliseconds a given node
 *     may be silent before its copies stop counting. Capped, because a 200-node pool must not produce a 200-entry
 *     document — and a map of every node here would scale with node count, which is the mistake the heartbeat
 *     document exists to avoid.
 */
public record MemberStatus(
    String supervisorId,
    long startedAt,
    String version,
    String storeBackend,
    List<String> poolsHeld,
    Liveness liveness,
    long passIntervalMillis,
    long leaseTtlMillis,
    int workCeilingPerPass,
    int perTenantFloor,
    long minimumPasses,
    boolean rebalanceEnabled,
    boolean readOnly,
    long observedAt
) {

    /** How many nodes' effective thresholds are published. Bounded on purpose; see the class note. */
    public static final int MAX_EFFECTIVE_NODES = 50;

    /**
     * @param staleMultiplier intervals of silence before a node is stale but its copies still count
     * @param lostMultiplier intervals before its copies are presumed gone
     * @param floorMillis the minimum <em>stale threshold</em>, applied by raising the interval rather than by
     *     clamping each threshold — clamping both would collapse the gap between them and remove the tolerance for
     *     a GC pause that the whole ladder exists to provide
     * @param effectiveMillis per node: stale and lost in milliseconds, as this process would decide them now
     */
    public record Liveness(
        int staleMultiplier,
        int lostMultiplier,
        long floorMillis,
        Map<String, Thresholds> effectiveMillis
    ) {}

    public record Thresholds(long staleMillis, long lostMillis) {}

    public byte[] toBytes() {
        return Json.write(this, "member status");
    }

    public static MemberStatus from(byte[] bytes) {
        return Json.read(bytes, MemberStatus.class, "member status");
    }
}
