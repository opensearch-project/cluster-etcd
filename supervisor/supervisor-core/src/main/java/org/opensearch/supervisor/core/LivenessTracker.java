package org.opensearch.supervisor.core;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.status.Liveness;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/**
 * Decides whether a node is alive, from <b>when this supervisor last observed its heartbeat key change
 * revision</b> — not by subtracting the node's own timestamp from the local clock.
 *
 * <p>{@code NODE_CONTRACT.md} argues the refinement: two clocks that disagree by a minute would otherwise
 * either evict a healthy cluster or strand a dead node's shards, and skew is not rare. The node's
 * {@code timestamp} stays in the document, for showing an operator how far behind a node is and for
 * catching a node whose own clock has stopped while it keeps writing.
 *
 * <p>This works only because <b>a heartbeat's value changes on every write</b> — the timestamp guarantees
 * it — so the revision always advances. That is why the field is required rather than optional, and it is
 * the one coupling between this class and the document format.
 *
 * <p><b>Two thresholds.</b> {@code STALE} first, {@code LOST} later; the gap is the system's tolerance for
 * a GC pause or a network blip, and a stale node's copies still count. Each is derived from that node's own
 * advertised interval rather than from a global constant, so a node that heartbeats every 30 s is not
 * declared dead at 40 s.
 *
 * <p><b>The startup grace period is not optional.</b> "Last observed change" lives in memory, and R2.3
 * forbids depending on memory — so a restarted supervisor must grant every node a full staleness window
 * before declaring anything dead. Without it, a supervisor restart evicts the cluster.
 */
public final class LivenessTracker {

    /** Multiplied by the node's own heartbeat interval to get its stale threshold. */
    public static final int DEFAULT_STALE_MULTIPLIER = 6;

    /** And again, further out, for lost. */
    public static final int DEFAULT_LOST_MULTIPLIER = 20;

    /** No node is declared stale sooner than this, however fast it claims to heartbeat. */
    public static final Duration DEFAULT_FLOOR = Duration.ofSeconds(20);

    /** Assumed interval for a node with no registration to derive one from. */
    public static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(5);

    private final RevisionSilence silence;
    private final int staleMultiplier;
    private final int lostMultiplier;
    private final Duration floor;

    public LivenessTracker(Instant startedAt) {
        this(startedAt, DEFAULT_STALE_MULTIPLIER, DEFAULT_LOST_MULTIPLIER, DEFAULT_FLOOR);
    }

    public LivenessTracker(Instant startedAt, int staleMultiplier, int lostMultiplier, Duration floor) {
        this.silence = new RevisionSilence(Objects.requireNonNull(startedAt, "startedAt"));
        this.staleMultiplier = staleMultiplier;
        this.lostMultiplier = lostMultiplier;
        this.floor = Objects.requireNonNull(floor, "floor");
        if (staleMultiplier >= lostMultiplier) {
            throw new IllegalArgumentException(
                "stale multiplier " + staleMultiplier + " must be below lost multiplier " + lostMultiplier
                    + "; without a gap between them there is no tolerance for a GC pause"
            );
        }
    }

    /**
     * Records that a node's heartbeat key was seen at a revision, and answers how alive it looks.
     *
     * @param heartbeatRevision the revision of {@code nodes/heartbeat/<id>} as just read
     * @param registration that node's registration, or {@code null} if it has none yet — without it there
     *     is no advertised interval, so the floor is used
     */
    public Liveness observe(NodeId nodeId, long heartbeatRevision, Registration registration, Instant now) {
        Duration silent = silence.observe(nodeId.value(), heartbeatRevision, now);

        if (silent.compareTo(lostThreshold(registration)) >= 0) {
            return Liveness.LOST;
        }
        if (silent.compareTo(staleThreshold(registration)) >= 0) {
            return Liveness.STALE;
        }
        return Liveness.LIVE;
    }

    /** A node observed for the first time by a supervisor that has only just started. */
    public Liveness observeAbsent(NodeId nodeId, Registration registration, Instant now) {
        silence.forget(nodeId.value());
        return silence.sinceStart(now).compareTo(staleThreshold(registration)) >= 0
            ? Liveness.LOST
            : Liveness.UNKNOWN;
    }

    public Duration staleThreshold(Registration registration) {
        return thresholdFor(registration, staleMultiplier);
    }

    public Duration lostThreshold(Registration registration) {
        return thresholdFor(registration, lostMultiplier);
    }

    /**
     * The floor applies to the <b>interval</b>, not to each threshold, and that distinction is a bug fix
     * rather than a nicety.
     *
     * <p>Clamping each threshold separately collapses the two whenever the derived values both fall below
     * the floor — a node heartbeating every second, or a node with no registration at all, would go from
     * live to lost with no stale phase and therefore no tolerance for a GC pause. Its copies would stop
     * counting and the supervisor would start replacing shards that exist. Raising the interval instead
     * keeps the ratio between the thresholds intact whatever the node claims.
     */
    private Duration thresholdFor(Registration registration, int multiplier) {
        Duration declared = registration == null
            ? DEFAULT_INTERVAL
            : Duration.ofMillis(registration.heartbeatIntervalMillis());
        // Ceiling division, so that lowestUseful * staleMultiplier is never a hair under the floor.
        // Duration.dividedBy truncates: 20s / 6 * 6 is 19.999999998s, which would put the stale threshold
        // just below the floor it exists to enforce.
        Duration lowestUseful = Duration.ofNanos(Math.ceilDiv(floor.toNanos(), staleMultiplier));
        Duration effective = declared.compareTo(lowestUseful) < 0 ? lowestUseful : declared;
        return effective.multipliedBy(multiplier);
    }

    /** Forget nodes that are no longer in the pool, so this cannot grow without bound. */
    public void retain(Set<NodeId> nodes) {
        silence.retain(nodes.stream().map(NodeId::value).collect(java.util.stream.Collectors.toSet()));
    }

    public int tracked() {
        return silence.tracked();
    }
}
