package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.status.Liveness;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Liveness is the decision this service is most likely to get catastrophically wrong, because both
 * directions of error are outages: too eager and it evicts a healthy cluster, too slow and a dead node's
 * shards are never recovered.
 */
class LivenessTrackerTests {

    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");
    private static final Instant START = Instant.ofEpochMilli(1_000_000);

    private LivenessTracker tracker;

    @BeforeEach
    void setUp() {
        tracker = new LivenessTracker(START);
    }

    private static Registration registration(long intervalMillis) {
        return new Registration(
            NODE, "eph", "n", PoolId.of("shared-a"), EnumSet.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of(), intervalMillis, 1L
        );
    }

    private Liveness at(long revision, Duration afterStart, Registration registration) {
        return tracker.observe(NODE, revision, registration, START.plus(afterStart));
    }

    @Test
    void aHeartbeatWhoseRevisionMovesIsAlive() {
        assertEquals(Liveness.LIVE, at(1, Duration.ZERO, registration(5000)));
        assertEquals(Liveness.LIVE, at(2, Duration.ofSeconds(5), registration(5000)));
        assertEquals(Liveness.LIVE, at(3, Duration.ofSeconds(10), registration(5000)));
    }

    @Test
    void anUnchangedRevisionEventuallyGoesStaleThenLost() {
        // 5s interval, default multipliers of 6 and 20, floor of 20s: stale at 30s, lost at 100s.
        Registration node = registration(5000);
        assertEquals(Liveness.LIVE, at(1, Duration.ZERO, node));

        assertEquals(Liveness.LIVE, at(1, Duration.ofSeconds(29), node));
        assertEquals(Liveness.STALE, at(1, Duration.ofSeconds(30), node));
        assertEquals(Liveness.STALE, at(1, Duration.ofSeconds(99), node));
        assertEquals(Liveness.LOST, at(1, Duration.ofSeconds(100), node));
    }

    @Test
    void thresholdsFollowTheNodesOwnAdvertisedInterval() {
        // A node that heartbeats every 30s is not dead at 40s. Deriving this from a global constant is how
        // a slow-heartbeating node gets evicted for behaving exactly as it said it would.
        Registration slow = registration(30_000);
        assertEquals(Duration.ofSeconds(180), tracker.staleThreshold(slow));
        assertEquals(Duration.ofSeconds(600), tracker.lostThreshold(slow));

        assertEquals(Liveness.LIVE, at(1, Duration.ZERO, slow));
        assertEquals(Liveness.LIVE, at(1, Duration.ofSeconds(40), slow));
    }

    @Test
    void theFloorProtectsAFastHeartbeatingNodeFromATinyThreshold() {
        // 6 x 1s would be a 6-second threshold, which one slow GC would trip.
        Registration fast = registration(1000);
        assertTrue(
            tracker.staleThreshold(fast).compareTo(LivenessTracker.DEFAULT_FLOOR) >= 0,
            "stale threshold " + tracker.staleThreshold(fast) + " fell below the floor"
        );
        // At least the floor, and not wildly above it: the floor is a minimum, not a new default.
        assertTrue(tracker.staleThreshold(fast).compareTo(LivenessTracker.DEFAULT_FLOOR.plusSeconds(1)) < 0);
    }

    @Test
    void theFloorNeverCollapsesTheGapBetweenStaleAndLost() {
        // The floor applies to the interval rather than to each threshold. Clamping each separately makes
        // stale and lost equal for any node whose derived thresholds both fall below the floor -- so a
        // fast-heartbeating node would go live to lost with no stale phase, its copies would stop counting,
        // and the supervisor would start replacing shards that exist over one GC pause.
        for (long interval : new long[] { 100, 1000, 3000, 5000, 30_000 }) {
            Registration node = registration(interval);
            assertTrue(
                tracker.lostThreshold(node).compareTo(tracker.staleThreshold(node)) > 0,
                "interval " + interval + "ms collapsed stale and lost to " + tracker.staleThreshold(node)
            );
        }
        assertTrue(tracker.lostThreshold(null).compareTo(tracker.staleThreshold(null)) > 0);
    }

    @Test
    void aRestartedSupervisorGrantsAFullWindowBeforeDeclaringAnythingDead() {
        // The startup grace period, and it is not optional. "Last observed change" lives in memory, so a
        // supervisor that starts up and immediately measures silence from a node's own timestamp -- or
        // from zero -- would evict the entire cluster on restart.
        LivenessTracker restarted = new LivenessTracker(START);
        Registration node = registration(5000);

        // First sighting of an already-old heartbeat: alive, because we have never seen it change.
        assertEquals(Liveness.LIVE, restarted.observe(NODE, 99, node, START));
        // And it only ages from when this process started, not from whenever the node last wrote.
        assertEquals(Liveness.LIVE, restarted.observe(NODE, 99, node, START.plusSeconds(29)));
        assertEquals(Liveness.STALE, restarted.observe(NODE, 99, node, START.plusSeconds(30)));
    }

    @Test
    void aNodeThatComesBackIsAliveAgainImmediately() {
        Registration node = registration(5000);
        at(1, Duration.ZERO, node);
        assertEquals(Liveness.LOST, at(1, Duration.ofSeconds(200), node));

        assertEquals(Liveness.LIVE, at(2, Duration.ofSeconds(201), node), "a new revision is a live node");
    }

    @Test
    void aNodeWithNoRegistrationGetsTheDefaultIntervalRatherThanTheFloor() {
        // A node whose registration has not landed yet is far more likely to be a torn read than a broken
        // node, so it must not be treated more harshly than a registered one.
        assertEquals(Duration.ofSeconds(30), tracker.staleThreshold(null));
        assertEquals(Duration.ofSeconds(100), tracker.lostThreshold(null));

        assertEquals(Liveness.LIVE, at(1, Duration.ZERO, null));
        assertEquals(Liveness.LIVE, at(1, Duration.ofSeconds(29), null));
        assertEquals(Liveness.STALE, at(1, Duration.ofSeconds(30), null));
        assertEquals(Liveness.LOST, at(1, Duration.ofSeconds(100), null));
    }

    @Test
    void staleMustComeBeforeLost() {
        // Without a gap there is no tolerance for a GC pause, and the two thresholds collapse into one
        // eviction. Rejected at construction rather than discovered in an incident.
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new LivenessTracker(START, 20, 6, Duration.ofSeconds(20))
        );
    }

    @Test
    void aNewlyRegisteredNodeIsNotCondemnedByALongRunningSupervisor() {
        // The grace period for a node with a registration and no heartbeat is measured from when this
        // process first saw *that node*, not from when the process started. Measuring it from process start
        // meant a supervisor up for an hour declared every new node lost on sight, before it had any chance
        // to write its first heartbeat.
        LivenessTracker old = new LivenessTracker(START);
        Instant muchLater = START.plus(Duration.ofHours(1));

        assertEquals(Liveness.UNKNOWN, old.observeAbsent(NODE, registration(5000), muchLater));
        assertEquals(
            Liveness.UNKNOWN,
            old.observeAbsent(NODE, registration(5000), muchLater.plusSeconds(29)),
            "still inside its own grace period"
        );
        assertEquals(
            Liveness.LOST,
            old.observeAbsent(NODE, registration(5000), muchLater.plusSeconds(31)),
            "and lost once it has been visible for a threshold without speaking"
        );
    }

    @Test
    void forgettingNodesKeepsTheMapBounded() {
        at(1, Duration.ZERO, registration(5000));
        assertEquals(1, tracker.tracked());

        tracker.retain(Set.of());
        assertEquals(0, tracker.tracked());
    }
}
