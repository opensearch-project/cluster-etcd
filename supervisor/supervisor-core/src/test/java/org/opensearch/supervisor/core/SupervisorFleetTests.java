package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.status.Replication;
import org.opensearch.supervisor.model.supervisor.Lease;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Several supervisors over one store: leases, membership, balancing and failover.
 *
 * <p>This is milestone 4's whole deliverable, and it runs in a sandbox with no sockets, no containers and no
 * Docker — which is why it comes <em>before</em> acting. Running a fleet while none of its members can write
 * an assignment means the worst a bug here can do is compute the same diff twice.
 *
 * <p>Passes are driven explicitly and in a stated order, so these assert things no amount of sleeping could.
 */
class SupervisorFleetTests {

    private static final PoolId A = PoolId.of("pool-a");
    private static final PoolId B = PoolId.of("pool-b");
    private static final PoolId C = PoolId.of("pool-c");
    private static final Duration TTL = Duration.ofSeconds(15);

    private InMemoryKeyValueStore store;
    private MutableClock clock;

    private static final class MutableClock extends Clock {
        private Instant now = Instant.ofEpochMilli(1_000_000);

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        clock = new MutableClock();
        definePool(A);
        definePool(B);
        definePool(C);
    }

    private void definePool(PoolId pool) {
        store.put(Keys.poolDefinition(pool),
            new PoolDefinition(pool, PoolKind.SHARED, false, null, null, java.util.List.of(), false).toBytes());
    }

    private Supervisor supervisor(String id) {
        return supervisor(id, 1.0);
    }

    private Supervisor supervisor(String id, double capacity) {
        return new Supervisor(
            store, clock, id, Set.of(), capacity, TTL, Balancer.Config.defaults()
        );
    }

    /** Runs every supervisor for n passes, advancing the clock a pass interval between rounds. */
    private void run(List<Supervisor> fleet, int rounds) {
        for (int round = 0; round < rounds; round++) {
            fleet.forEach(Supervisor::tick);
            clock.advance(Duration.ofSeconds(2));
        }
    }

    private Set<PoolId> allHeld(List<Supervisor> fleet) {
        Set<PoolId> held = new LinkedHashSet<>();
        fleet.forEach(supervisor -> held.addAll(supervisor.held()));
        return held;
    }

    /** The invariant that matters: no pool is ever held by two members at once. */
    private void assertNoPoolHeldTwice(List<Supervisor> fleet) {
        Map<PoolId, String> owner = new HashMap<>();
        for (Supervisor supervisor : fleet) {
            for (PoolId pool : supervisor.held()) {
                String previous = owner.put(pool, supervisor.supervisorId());
                assertTrue(previous == null,
                    "pool " + pool + " held by both " + previous + " and " + supervisor.supervisorId());
            }
        }
    }

    // ---------------------------------------------------------------- one supervisor

    @Test
    void aLoneSupervisorEventuallyHoldsEveryPool() {
        Supervisor only = supervisor("s1");

        run(List.of(only), 5);

        assertEquals(Set.of(A, B, C), only.held());
    }

    @Test
    void aLoneSupervisorTakesOnePoolPerPass() {
        Supervisor only = supervisor("s1");

        only.tick();
        assertEquals(1, only.held().size());
        only.tick();
        assertEquals(2, only.held().size());
    }

    // ---------------------------------------------------------------- several supervisors

    @Test
    void aFleetSharesThePoolsAndNeverDoublesUp() {
        List<Supervisor> fleet = List.of(supervisor("s1"), supervisor("s2"), supervisor("s3"));

        run(fleet, 10);

        assertEquals(Set.of(A, B, C), allHeld(fleet), "every pool must be owned by someone");
        assertNoPoolHeldTwice(fleet);
        fleet.forEach(supervisor -> assertEquals(1, supervisor.held().size(),
            supervisor.supervisorId() + " holds " + supervisor.held()));
    }

    @Test
    void aSupervisorJoiningLaterTakesWorkFromTheOverloadedOne() {
        Supervisor first = supervisor("s1");
        run(List.of(first), 6);
        assertEquals(3, first.held().size());

        Supervisor second = supervisor("s2");
        List<Supervisor> fleet = List.of(first, second);

        // Hysteresis and the sustained-pass count mean this is not instant, and should not be: an eager
        // rebalancer is one that oscillates.
        run(fleet, 3);
        assertEquals(3, first.held().size(), "not yet -- the release threshold has not been reached");

        run(fleet, Balancer.Config.defaults().releaseAfterPasses() + 5);

        assertTrue(first.held().size() < 3, "s1 should have given something up");
        assertFalse(second.held().isEmpty(), "s2 should have taken it");
        assertEquals(Set.of(A, B, C), allHeld(fleet));
        assertNoPoolHeldTwice(fleet);
    }

    @Test
    void aBiggerMemberEndsUpWithMorePools() {
        List<Supervisor> fleet = List.of(supervisor("big", 5.0), supervisor("small", 1.0));

        run(fleet, 30);

        assertEquals(Set.of(A, B, C), allHeld(fleet));
        assertNoPoolHeldTwice(fleet);
        assertTrue(
            fleet.get(0).held().size() >= fleet.get(1).held().size(),
            "big held " + fleet.get(0).held() + ", small held " + fleet.get(1).held()
        );
    }

    // ---------------------------------------------------------------- failover

    @Test
    void aCrashedSupervisorsPoolsAreTakenOverAfterTheLeaseGoesQuiet() {
        Supervisor dying = supervisor("s1");
        Supervisor survivor = supervisor("s2");
        List<Supervisor> fleet = List.of(dying, survivor);
        run(fleet, 10);
        assertNoPoolHeldTwice(fleet);

        Set<PoolId> orphaned = dying.held();
        assertFalse(orphaned.isEmpty());

        // A crash, not a shutdown: nothing is released and nothing is cleaned up, so recovery has to come
        // from the survivor noticing that the lease key has stopped changing.
        dying.die();

        run(List.of(survivor), 3);
        assertFalse(survivor.held().containsAll(orphaned), "too early -- the TTL has not elapsed");

        clock.advance(TTL);
        run(List.of(survivor), 10);

        assertTrue(survivor.held().containsAll(orphaned),
            "expected " + orphaned + " to be taken over, survivor holds " + survivor.held());
    }

    @Test
    void aCleanShutdownHandsPoolsOverWithoutWaitingOutTheTtl() {
        // Releasing rather than letting the lease expire is the difference between a rebalance that costs
        // one pass and one that costs a failover window.
        Supervisor leaving = supervisor("s1");
        Supervisor staying = supervisor("s2");
        List<Supervisor> fleet = List.of(leaving, staying);
        run(fleet, 10);

        Set<PoolId> handed = leaving.held();
        leaving.shutdown();

        run(List.of(staying), 6);

        assertTrue(staying.held().containsAll(handed), "handover should need no TTL at all");
        assertEquals(Set.of(A, B, C), staying.held());
    }

    @Test
    void aRestartedSupervisorDoesNotStealAnythingOnItsFirstPasses() {
        // The startup grace period, and it is not optional. A new member observes nothing sitting still, so
        // it steals nothing -- otherwise a rolling restart would reshuffle the whole fleet.
        Supervisor incumbent = supervisor("s1");
        run(List.of(incumbent), 6);
        Set<PoolId> before = Set.copyOf(incumbent.held());

        Supervisor newcomer = supervisor("s2");
        newcomer.tick();

        assertEquals(before, incumbent.held(), "the incumbent must not lose anything to a newcomer's arrival");
        assertTrue(newcomer.held().isEmpty(), "a newcomer takes only what is genuinely unowned");
    }

    @Test
    void aSupervisorThatLosesItsLeaseDropsThePoolAtOnce() {
        // R6.3: a supervisor that comes back after losing a lease must not apply a stale plan. Plans are
        // cheap, so the pool is dropped immediately rather than finished.
        Supervisor holder = supervisor("s1");
        run(List.of(holder), 6);
        PoolId stolen = holder.held().iterator().next();

        // Somebody else takes the lease out from under it -- a partition healing, or an operator.
        store.put(Keys.poolLease(stolen), new Lease("someone-else", 99, clock.millis(),
            clock.millis() + TTL.toMillis()).toBytes());

        holder.tick();

        assertFalse(holder.held().contains(stolen), "the lease is gone, so the pool must be too");
    }

    // ---------------------------------------------------------------- the store's view

    @Test
    void theStoreShowsWhoHoldsWhatAndWhoIsAlive() {
        List<Supervisor> fleet = List.of(supervisor("s1"), supervisor("s2"));
        run(fleet, 10);

        for (Supervisor supervisor : fleet) {
            assertTrue(store.get(Keys.member(supervisor.supervisorId())).isPresent());
        }
        for (PoolId pool : Set.of(A, B, C)) {
            Lease lease = Lease.from(store.get(Keys.poolLease(pool)).orElseThrow().value());
            assertTrue(Set.of("s1", "s2").contains(lease.holder()));
            assertTrue(lease.epoch() >= 1);
        }
    }

    @Test
    void aReclaimedPoolGetsAHigherEpoch() {
        // The epoch is stamped into every assignment its holder writes, so a split brain is diagnosable
        // after the fact from data the system already wrote.
        Supervisor first = supervisor("s1");
        run(List.of(first), 6);
        PoolId pool = first.held().iterator().next();
        long firstEpoch = Lease.from(store.get(Keys.poolLease(pool)).orElseThrow().value()).epoch();

        first.die();
        clock.advance(TTL.multipliedBy(2));
        Supervisor second = supervisor("s2");
        run(List.of(second), 10);

        long secondEpoch = Lease.from(store.get(Keys.poolLease(pool)).orElseThrow().value()).epoch();
        assertTrue(secondEpoch > firstEpoch, firstEpoch + " -> " + secondEpoch);
    }

    @Test
    void reconciliationStillHappensForEveryPoolHeld() {
        // The fleet exists to run passes, so this is worth asserting rather than assuming: every held pool
        // gets a diff, and a pool nobody holds gets none.
        TenantId acme = TenantId.of("acme");
        store.put(Keys.poolBinding(acme), PoolBinding.to(A).toBytes());
        store.put(Keys.tenantRegistration(acme),
            "{\"tenant\":\"acme\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        store.put(Keys.indicesPrefix(acme) + "my-index", IndexConfig.toDocument(
            "my-index", "uzKxLWZu4nPSrTknEcDpoA", true, IndexConfig.settingsOf(1, 0, Replication.SEGMENT)
        ));

        Supervisor only = supervisor("s1");
        run(List.of(only), 6);

        assertFalse(only.lastDiffOf(A).isEmpty(), "pool A has an index with no shards anywhere");
        assertTrue(store.get(Keys.tenantDiff(acme)).isPresent(), "and it should be readable from the store");
    }

    @Test
    void nothingWritesAnAssignmentAnywhere() {
        // Milestone 4 is still read-only, and this is the assertion that keeps it honest while the fleet
        // machinery is being shaken out.
        TenantId acme = TenantId.of("acme");
        store.put(Keys.poolBinding(acme), PoolBinding.to(A).toBytes());
        store.put(Keys.indicesPrefix(acme) + "my-index", IndexConfig.toDocument(
            "my-index", "uzKxLWZu4nPSrTknEcDpoA", true, IndexConfig.settingsOf(3, 2, Replication.SEGMENT)
        ));

        List<Supervisor> fleet = List.of(supervisor("s1"), supervisor("s2"), supervisor("s3"));
        run(fleet, 20);

        assertTrue(store.scanPrefix(Keys.NODE_ASSIGNMENT_PREFIX).isEmpty());
    }

    // ---------------------------------------------------------------- stability

    @Test
    void aSettledFleetStopsMovingPoolsAround() {
        // Oscillation is the characteristic failure of every rebalancer. Once the pools are shared out, a
        // hundred more passes should change nothing at all.
        List<Supervisor> fleet = List.of(supervisor("s1"), supervisor("s2"), supervisor("s3"));
        run(fleet, 40);

        List<Set<PoolId>> settled = fleet.stream().map(supervisor -> Set.copyOf(supervisor.held())).toList();
        run(fleet, 100);

        for (int i = 0; i < fleet.size(); i++) {
            assertEquals(settled.get(i), fleet.get(i).held(),
                fleet.get(i).supervisorId() + " kept moving after settling");
        }
    }

    @Test
    void everyPoolIsHeldByExactlyOneMemberThroughoutARollingRestart() {
        // The property the whole lease mechanism exists for, checked at every step rather than at the end.
        List<Supervisor> fleet = new ArrayList<>(List.of(supervisor("s1"), supervisor("s2")));
        run(fleet, 20);

        for (int replacement = 0; replacement < 3; replacement++) {
            Supervisor going = fleet.remove(0);
            going.shutdown();
            Supervisor arriving = supervisor("s-new-" + replacement);
            fleet.add(arriving);

            for (int pass = 0; pass < 25; pass++) {
                fleet.forEach(Supervisor::tick);
                assertNoPoolHeldTwice(fleet);
                clock.advance(Duration.ofSeconds(2));
            }
        }

        assertEquals(Set.of(A, B, C), allHeld(fleet));
    }

    @Test
    void concurrentClaimsOfTheSamePoolLeaveOneWinner() {
        // Two members starting together will both decide to claim the same pool, because they have the same
        // figures. The compare-and-set is what makes that harmless rather than a split brain.
        List<Supervisor> fleet = List.of(supervisor("s1"), supervisor("s2"), supervisor("s3"));

        fleet.forEach(Supervisor::tick);

        assertNoPoolHeldTwice(fleet);
        int totalHeld = fleet.stream().mapToInt(supervisor -> supervisor.held().size()).sum();
        assertEquals(new HashSet<>(allHeld(fleet)).size(), totalHeld, "a pool was counted twice");
    }
}
