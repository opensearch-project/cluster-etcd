package org.opensearch.supervisor.integ;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.core.Supervisor;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.supervisor.Lease;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Several supervisors, real threads, one real store.
 *
 * <p>The gap this closes is specific. {@code SupervisorFleetTests} covers sharing and failover thoroughly, but it
 * does so by calling {@code tick()} on each supervisor in turn from one thread with a clock it advances by hand —
 * an arrangement in which two supervisors can never actually interleave, which is the only situation a lease is
 * for. Here they run concurrently against etcd's compare-and-set and a clock nobody controls.
 *
 * <p>The invariant under test throughout: <b>no pool is ever owned by two supervisors at once.</b> Everything
 * else in the design leans on it, because §2 makes every other key single-writer by construction and leaves this
 * as the one place contention exists.
 */
@EnabledIf("org.opensearch.supervisor.integ.EtcdTestServer#available")
class EtcdLeaseIT extends EtcdIntegBase {

    private static final List<PoolId> POOLS = List.of(
        PoolId.of("pool-a"), PoolId.of("pool-b"), PoolId.of("pool-c"), PoolId.of("pool-d"));

    /** A supervisor on its own thread, ticking until told to stop. */
    private final class Member {
        final String id;
        final Supervisor supervisor;
        final Thread thread;
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        Member(String id, KeyValueStore against) {
            this.id = id;
            this.supervisor = supervisor(id, against);
            this.thread = new Thread(() -> {
                while (stop.get() == false) {
                    try {
                        supervisor.tick();
                    } catch (Throwable thrown) {
                        failure.set(thrown);
                    }
                    sleep(Duration.ofMillis(300));
                }
            }, "supervisor-" + id);
        }

        void start() {
            thread.start();
        }

        /** A clean stop: releases leases, so a peer can claim immediately. */
        void shutdown() {
            stop.set(true);
            join();
            supervisor.shutdown();
        }

        /** An unclean stop: nothing is released, so recovery must come from observed silence. */
        void die() {
            stop.set(true);
            join();
            supervisor.die();
        }

        private void join() {
            try {
                thread.join(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void definePools() {
        POOLS.forEach(this::definePool);
    }

    /** Who the store says holds each pool, right now. */
    private Map<PoolId, String> holders() {
        Map<PoolId, String> holders = new LinkedHashMap<>();
        for (PoolId pool : POOLS) {
            store.get(Keys.poolLease(pool)).ifPresent(versioned -> {
                try {
                    holders.put(pool, Lease.from(versioned.value()).holder());
                } catch (RuntimeException unreadable) {
                    holders.put(pool, "<unreadable>");
                }
            });
        }
        return holders;
    }

    @Test
    void twoSupervisorsShareThePoolsAndNeitherEverHoldsOneTheOtherDoes() throws Exception {
        definePools();
        Member one = new Member("s1", store);
        Member two = new Member("s2", open());

        one.start();
        two.start();
        try {
            eventually("every pool to be claimed by somebody", () ->
                assertEquals(POOLS.size(), holders().size(), "holders=" + holders()));

            eventually("the pools to be shared rather than hoarded", Duration.ofSeconds(90), () -> {
                Set<PoolId> byOne = new LinkedHashSet<>(one.supervisor.held());
                Set<PoolId> byTwo = new LinkedHashSet<>(two.supervisor.held());
                assertTrue(byOne.isEmpty() == false && byTwo.isEmpty() == false,
                    "s1=" + byOne + " s2=" + byTwo);
            });

            // The invariant. Sampled repeatedly rather than once, because a violation would be brief.
            for (int sample = 0; sample < 40; sample++) {
                Set<PoolId> byOne = new LinkedHashSet<>(one.supervisor.held());
                Set<PoolId> byTwo = new LinkedHashSet<>(two.supervisor.held());
                Set<PoolId> both = new LinkedHashSet<>(byOne);
                both.retainAll(byTwo);
                assertTrue(both.isEmpty(), "both supervisors believed they held " + both
                    + " at once, which is the one thing the lease exists to prevent");
                sleep(Duration.ofMillis(150));
            }
        } finally {
            one.shutdown();
            two.shutdown();
        }
        assertNull(one.failure.get(), () -> "s1 threw: " + one.failure.get());
        assertNull(two.failure.get(), () -> "s2 threw: " + two.failure.get());
    }

    @Test
    void aCleanShutdownHandsPoolsOverWithoutWaitingForTheLeaseToExpire() throws Exception {
        // The reason Supervisor.shutdown() releases explicitly: it turns a rolling restart from a series of
        // TTL-length gaps into something invisible.
        definePools();
        Member one = new Member("s1", store);
        one.start();
        try {
            eventually("s1 to hold every pool", () ->
                assertEquals(POOLS.size(), one.supervisor.held().size(), "held=" + one.supervisor.held()));
        } finally {
            one.shutdown();
        }

        // Released, so nothing is holding them and a successor need not wait out the TTL.
        assertTrue(holders().isEmpty(), "a clean shutdown should leave no lease behind: " + holders());

        Member two = new Member("s2", open());
        two.start();
        try {
            eventually("s2 to pick the pools up promptly", LEASE_TTL.multipliedBy(2), () ->
                assertEquals(POOLS.size(), two.supervisor.held().size(), "held=" + two.supervisor.held()));
        } finally {
            two.shutdown();
        }
        assertNull(two.failure.get(), () -> "s2 threw: " + two.failure.get());
    }

    @Test
    void anAbruptDeathIsRecoveredFromObservedSilence() throws Exception {
        // The interesting failure, and the one the lease mechanism exists for: nothing is released, so the
        // survivor has to notice that a key has stopped changing. Against a real clock, for the first time.
        definePools();
        Member doomed = new Member("s1", store);
        doomed.start();
        try {
            eventually("s1 to hold every pool", () ->
                assertEquals(POOLS.size(), doomed.supervisor.held().size(), "held=" + doomed.supervisor.held()));
        } finally {
            doomed.die();
        }
        Map<PoolId, String> stranded = holders();
        assertEquals(POOLS.size(), stranded.size(), "the leases should still be there, held by a dead process");
        assertTrue(stranded.values().stream().allMatch("s1"::equals), stranded.toString());

        Member survivor = new Member("s2", open());
        survivor.start();
        try {
            // A challenger measures silence from when *it* started watching, not from the lease's own expiresAt
            // -- two clocks that disagree would otherwise let it steal a healthy lease. So the wait is a full TTL
            // after s2's first observation, not from s1's death.
            eventually("s2 to take over the stranded pools", LEASE_TTL.multipliedBy(6), () ->
                assertEquals(POOLS.size(), survivor.supervisor.held().size(),
                    "held=" + survivor.supervisor.held() + " holders=" + holders()));

            // And the epoch advanced, which is what makes a split brain diagnosable afterwards from data the
            // system already wrote.
            for (PoolId pool : POOLS) {
                Lease lease = Lease.from(store.get(Keys.poolLease(pool)).orElseThrow().value());
                assertEquals("s2", lease.holder());
                assertTrue(lease.epoch() > 1,
                    "an acquisition must advance the epoch: " + pool + " is at " + lease.epoch());
            }
        } finally {
            survivor.shutdown();
        }
        assertNull(survivor.failure.get(), () -> "s2 threw: " + survivor.failure.get());
    }

    @Test
    void manySupervisorsStartingAtOnceDoNotDoubleClaimAnything() throws Exception {
        // The thundering herd: five processes starting simultaneously is what a deployment rollout looks like,
        // and it is the moment every lease is uncontested and therefore every supervisor wants all of them.
        definePools();
        int members = 5;
        CountDownLatch go = new CountDownLatch(1);
        List<Member> all = new java.util.ArrayList<>();
        for (int i = 0; i < members; i++) {
            Member member = new Member("s" + i, open());
            all.add(member);
        }

        List<Thread> starters = new java.util.ArrayList<>();
        for (Member member : all) {
            Thread starter = new Thread(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                member.start();
            });
            starters.add(starter);
            starter.start();
        }
        go.countDown();
        for (Thread starter : starters) {
            starter.join(TimeUnit.SECONDS.toMillis(30));
        }

        try {
            eventually("every pool to be claimed", () ->
                assertEquals(POOLS.size(), holders().size(), "holders=" + holders()));

            for (int sample = 0; sample < 60; sample++) {
                Map<PoolId, Set<String>> claimants = new LinkedHashMap<>();
                for (Member member : all) {
                    for (PoolId pool : member.supervisor.held()) {
                        claimants.computeIfAbsent(pool, ignored -> new LinkedHashSet<>()).add(member.id);
                    }
                }
                claimants.forEach((pool, who) -> assertTrue(who.size() <= 1,
                    "pool " + pool + " was claimed by " + who + " at the same time"));
                sleep(Duration.ofMillis(100));
            }
        } finally {
            for (Member member : all) {
                member.shutdown();
            }
        }
        for (Member member : all) {
            assertNull(member.failure.get(), () -> member.id + " threw: " + member.failure.get());
        }
    }
}
