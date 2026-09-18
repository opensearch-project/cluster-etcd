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

    /**
     * How patient this supervisor is about silence.
     *
     * <p>Configurable, with the defaults unchanged, because this is the most consequential timing in the service
     * and it was the only one an operator could not reach. A lease TTL, the planner's policy and the balancer's
     * hysteresis are all tunable; declaring a node lost is what starts rebuilding its shards elsewhere, and at a
     * five-second heartbeat that takes about a hundred seconds. Whether that is too long or too short is a
     * property of the deployment — how expensive a rebuild is, how often a node pauses, how much redundancy
     * there is to spare — and not something this code can know.
     *
     * <p>It is also a knob to be careful with in one direction only. Too <em>long</em> costs availability while a
     * dead node's shards go unrebuilt. Too <em>short</em> declares healthy nodes lost during an ordinary GC pause
     * and moves data that did not need moving, which is worse: it burns recovery bandwidth precisely when the
     * cluster is already struggling. The floor exists to make the second failure hard to reach by accident, so
     * lowering it is the change that deserves thought.
     *
     * @param staleMultiplier multiplied by a node's own heartbeat interval to get its stale threshold. Stale
     *     means "do not place new copies here", and a stale node's existing copies still count.
     * @param lostMultiplier the same, further out. Lost means its copies are presumed gone.
     * @param floor applied to the <b>interval</b> rather than to each threshold — see {@link #thresholdFor}
     */
    public record Config(int staleMultiplier, int lostMultiplier, Duration floor) {

        public Config {
            Objects.requireNonNull(floor, "floor");
            if (staleMultiplier >= lostMultiplier) {
                throw new IllegalArgumentException(
                    "stale multiplier " + staleMultiplier + " must be below lost multiplier " + lostMultiplier
                        + "; equal thresholds would remove the stale phase, and with it the tolerance for a"
                        + " GC pause that stops a healthy node being treated as dead"
                );
            }
            if (staleMultiplier < 2) {
                throw new IllegalArgumentException("a stale multiplier of " + staleMultiplier + " would declare a"
                    + " node stale after missing a single heartbeat, which every healthy node does sometimes");
            }
            if (floor.isNegative() || floor.isZero()) {
                throw new IllegalArgumentException("the liveness floor must be positive, not " + floor);
            }
        }

        public static Config defaults() {
            return new Config(DEFAULT_STALE_MULTIPLIER, DEFAULT_LOST_MULTIPLIER, DEFAULT_FLOOR);
        }
    }

    private final RevisionSilence silence;

    /**
     * When this process first saw each node at all, which is what the grace period for a node that has
     * registered but not yet heartbeated is measured from.
     *
     * <p>Measuring it from process start instead was a bug: a supervisor that had been up for an hour
     * declared every newly registered node lost on sight, before it had any chance to write a heartbeat.
     * Harmless for a node holding nothing, and a false alarm in the diff either way — and it would have got
     * worse the moment anything keyed off that verdict.
     */
    private final java.util.Map<NodeId, Instant> firstSeen = new java.util.HashMap<>();

    private final int staleMultiplier;
    private final int lostMultiplier;
    private final Duration floor;

    public LivenessTracker(Instant startedAt) {
        this(startedAt, Config.defaults());
    }

    public LivenessTracker(Instant startedAt, Config config) {
        this(startedAt, config.staleMultiplier(), config.lostMultiplier(), config.floor());
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
        firstSeen.putIfAbsent(nodeId, now);
        Duration silent = silence.observe(nodeId.value(), heartbeatRevision, now);

        if (silent.compareTo(lostThreshold(registration)) >= 0) {
            return Liveness.LOST;
        }
        if (silent.compareTo(staleThreshold(registration)) >= 0) {
            return Liveness.STALE;
        }
        return Liveness.LIVE;
    }

    /**
     * A node that has a registration but no heartbeat: starting up, or broken before it ever spoke.
     *
     * <p>{@code UNKNOWN} until it has been visible for a whole staleness threshold, then {@code LOST}. The
     * wait is measured from when <em>this process first saw the node</em>, not from when the process started
     * — otherwise a long-running supervisor condemns every new node the instant it appears.
     */
    public Liveness observeAbsent(NodeId nodeId, Registration registration, Instant now) {
        silence.forget(nodeId.value());
        Instant since = firstSeen.computeIfAbsent(nodeId, node -> now);
        Duration visible = Duration.between(since, now);
        return visible.compareTo(staleThreshold(registration)) >= 0 ? Liveness.LOST : Liveness.UNKNOWN;
    }

    public Duration staleThreshold(Registration registration) {
        return thresholdFor(registration, staleMultiplier);
    }

    /**
     * The heartbeat revision this process last saw for a node, or null if it has seen none.
     *
     * <p>The witness a reap compare-and-sets against. This tracker is the only thing that remembers it, which
     * is not an accident: it is the same number the liveness verdict was computed from, so deleting the key
     * under it means "nothing has written here since I concluded this node was gone". A node that came back
     * between the observation and the delete fails the exchange and keeps every one of its documents.
     */
    public Long heartbeatRevisionOf(NodeId nodeId) {
        return silence.revisionOf(nodeId.value());
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

    /** Forget nodes that have gone entirely, including their grace-period start. */
    public void retainAllKnown(Set<NodeId> nodes) {
        firstSeen.keySet().retainAll(nodes);
    }

    public int tracked() {
        return silence.tracked();
    }
}
