package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.supervisor.Lease;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Leases directly, and the chores nobody else can see. */
class LeaseAndHousekeepingTests {

    private static final PoolId A = PoolId.of("pool-a");
    private static final PoolId B = PoolId.of("pool-b");
    private static final TenantId ACME = TenantId.of("acme");
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
        store.put(Keys.poolDefinition(A), new PoolDefinition(A, PoolKind.SHARED, false, null, null, java.util.List.of(), false).toBytes());
    }

    private LeaseManager manager(String id) {
        return new LeaseManager(store, clock, id, TTL);
    }

    // ---------------------------------------------------------------- lease mechanics

    @Test
    void anAbsentLeaseIsClaimableAndTakingItIsCreateIfAbsent() {
        LeaseManager first = manager("s1");
        first.refresh();

        assertEquals(Set.of(A), first.claimable(Set.of(A)));
        assertTrue(first.tryAcquire(A));
        assertTrue(first.holds(A));
        assertEquals(1, first.epochOf(A), "a first acquisition starts at epoch 1");
    }

    @Test
    void twoSupervisorsRacingLeaveExactlyOneHolder() {
        // Both see an absent lease, both try. The compare-and-set is what makes this the ordinary outcome
        // of starting together rather than a split brain.
        LeaseManager first = manager("s1");
        LeaseManager second = manager("s2");
        first.refresh();
        second.refresh();

        boolean firstWon = first.tryAcquire(A);
        boolean secondWon = second.tryAcquire(A);

        assertTrue(firstWon ^ secondWon, "exactly one, got " + firstWon + "/" + secondWon);
    }

    @Test
    void aHeldLeaseIsNotClaimableByAnyoneElseHoweverLongTheyWatch() {
        LeaseManager holder = manager("s1");
        LeaseManager challenger = manager("s2");
        holder.refresh();
        holder.tryAcquire(A);

        // The holder renews every pass, so the revision keeps moving and silence never accumulates.
        for (int pass = 0; pass < 20; pass++) {
            clock.advance(Duration.ofSeconds(2));
            holder.refresh();
            assertTrue(holder.renew(A));
            challenger.refresh();
            assertTrue(challenger.claimable(Set.of(A)).isEmpty(), "stolen on pass " + pass);
        }
    }

    @Test
    void aLeaseNobodyRenewsBecomesClaimableAfterTheTtl() {
        LeaseManager holder = manager("s1");
        holder.refresh();
        holder.tryAcquire(A);

        LeaseManager challenger = manager("s2");
        challenger.refresh();
        assertTrue(challenger.claimable(Set.of(A)).isEmpty(), "not yet -- nothing has been observed sitting still");

        clock.advance(TTL);
        challenger.refresh();

        assertEquals(Set.of(A), challenger.claimable(Set.of(A)));
        assertTrue(challenger.tryAcquire(A));
        assertEquals(2, challenger.epochOf(A), "taking over advances the epoch");
    }

    @Test
    void expiryIsJudgedFromObservedSilenceNotFromTheHoldersTimestamp() {
        // A holder whose clock is wildly ahead writes an expiry far in the future; one whose clock is behind
        // writes one already past. Neither should change when a challenger may take over, because a
        // challenger that trusted those fields could steal a healthy lease or leave a dead one held.
        store.put(Keys.poolLease(A), new Lease("s1", 1, 0, 1).toBytes());

        LeaseManager challenger = manager("s2");
        challenger.refresh();
        assertTrue(challenger.claimable(Set.of(A)).isEmpty(), "an already-past expiry must not be enough");

        clock.advance(TTL);
        challenger.refresh();
        assertEquals(Set.of(A), challenger.claimable(Set.of(A)));
    }

    @Test
    void losingTheLeaseIsVisibleOnTheNextRenewal() {
        LeaseManager holder = manager("s1");
        holder.refresh();
        holder.tryAcquire(A);

        // Somebody writes the key out from under it.
        store.put(Keys.poolLease(A), new Lease("s2", 9, clock.millis(), clock.millis() + 1000).toBytes());

        holder.refresh();
        assertFalse(holder.holds(A));
        assertFalse(holder.renew(A), "renewal must fail rather than clobber the new holder");
    }

    @Test
    void releasingMakesItImmediatelyAvailableWithNoTtlToWaitOut() {
        LeaseManager holder = manager("s1");
        holder.refresh();
        holder.tryAcquire(A);

        assertTrue(holder.release(A));
        assertFalse(holder.holds(A));

        LeaseManager next = manager("s2");
        next.refresh();
        assertEquals(Set.of(A), next.claimable(Set.of(A)));
    }

    @Test
    void anUnreadableLeaseDoesNotMakeAPoolPermanentlyUnclaimable() {
        store.put(Keys.poolLease(A), "{ not json".getBytes(StandardCharsets.UTF_8));

        LeaseManager manager = manager("s1");
        manager.refresh();

        assertEquals(Set.of(A), manager.claimable(Set.of(A)));
        assertTrue(manager.tryAcquire(A));
    }

    @Test
    void theDefaultTtlIsAtLeastAFewPassesAndNeverAbsurdlyShort() {
        assertEquals(Duration.ofSeconds(15), LeaseManager.defaultTtl(Duration.ofSeconds(2)));
        assertEquals(Duration.ofSeconds(30), LeaseManager.defaultTtl(Duration.ofSeconds(10)));
    }

    // ---------------------------------------------------------------- housekeeping

    private Supervisor supervisor(String id) {
        return new Supervisor(store, clock, id, Set.of(), 1.0, TTL, Balancer.Config.defaults());
    }

    private Housekeeping.Report reportOf(Supervisor supervisor) {
        return supervisor.housekeeping().lastReport();
    }

    @Test
    void aTenantNobodyHasBoundToAPoolIsReported() {
        // The failure this class exists for. Nothing else in the system says anything at all: no shards are
        // placed, no health computed, no tasks reaped -- which is indistinguishable from an idle tenant.
        store.put(Keys.tenantRegistration(ACME), "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));

        Supervisor only = supervisor("s1");
        only.tick();

        assertEquals(List.of("acme"), reportOf(only).unpooledTenants());
    }

    @Test
    void aBoundTenantIsNotReported() {
        store.put(Keys.tenantRegistration(ACME), "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));
        store.put(Keys.poolBinding(ACME), PoolBinding.to(A).toBytes());

        Supervisor only = supervisor("s1");
        only.tick();

        assertTrue(reportOf(only).unpooledTenants().isEmpty());
    }

    @Test
    void aNodeInNoPoolIsReported() {
        // Invisible to every pool loop at once, by construction: a loop only ever looks at its own nodes.
        NodeId orphan = NodeId.of("orphanNodeAAAAAAAAAAA");
        store.put(Keys.nodeRegistration(orphan), NodeJson.write(new Registration(
            orphan, "eph", "orphan", null, EnumSet.of(NodeRole.DATA),
            "10.0.0.9", 9300, "3.7.0", Map.of(), 5000, 1L
        )));

        Supervisor only = supervisor("s1");
        only.tick();

        assertEquals(List.of(orphan.value()), reportOf(only).unpooledNodes());
    }

    @Test
    void aNodeNamingAPoolThatDoesNotExistIsAlsoUnpooled() {
        // An operator's typo should strand one node visibly rather than invent a pool.
        NodeId typo = NodeId.of("typoNodeAAAAAAAAAAAAA");
        store.put(Keys.nodeRegistration(typo), NodeJson.write(new Registration(
            typo, "eph", "typo", PoolId.of("pool-that-is-not-defined"), EnumSet.of(NodeRole.DATA),
            "10.0.0.9", 9300, "3.7.0", Map.of(), 5000, 1L
        )));

        Supervisor only = supervisor("s1");
        only.tick();

        assertEquals(List.of(typo.value()), reportOf(only).unpooledNodes());
    }

    @Test
    void aPoolNobodyHoldsIsReported() {
        store.put(Keys.poolDefinition(B), new PoolDefinition(B, PoolKind.SHARED, false, null, null, java.util.List.of(), false).toBytes());

        Supervisor only = supervisor("s1");
        only.tick();

        // One pool claimed on the first pass, so the other is briefly unowned -- which is exactly the
        // transient state an operator should be able to see rather than guess at.
        assertFalse(reportOf(only).unownedPools().isEmpty());

        for (int pass = 0; pass < 5; pass++) {
            clock.advance(Duration.ofSeconds(2));
            only.tick();
        }
        assertTrue(reportOf(only).unownedPools().isEmpty(), "everything should be owned once it settles");
    }

    @Test
    void twoPoolsClaimingToBeDefaultAreBothIgnoredAndReported() {
        // Choosing between them by scan order is a coin flip dressed up as configuration.
        store.put(Keys.poolDefinition(A), new PoolDefinition(A, PoolKind.SHARED, true, null, null, java.util.List.of(), false).toBytes());
        store.put(Keys.poolDefinition(B), new PoolDefinition(B, PoolKind.SHARED, true, null, null, java.util.List.of(), false).toBytes());
        store.put(Keys.tenantRegistration(ACME), "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));

        Supervisor only = supervisor("s1");
        only.tick();

        assertFalse(reportOf(only).duplicateDefaults().isEmpty());
        assertEquals(List.of("acme"), reportOf(only).unpooledTenants(), "and the tenant is left unbound");
    }

    @Test
    void aSecondTenantOnADedicatedPoolIsReported() {
        store.put(Keys.poolDefinition(A), new PoolDefinition(A, PoolKind.DEDICATED, false, null, null, java.util.List.of(), false).toBytes());
        store.put(Keys.poolBinding(ACME), PoolBinding.to(A).toBytes());
        store.put(Keys.poolBinding(TenantId.of("globex")), PoolBinding.to(A).toBytes());

        Supervisor only = supervisor("s1");
        only.tick();

        assertFalse(reportOf(only).overloadedDedicatedPools().isEmpty());
    }

    @Test
    void onlyOneMemberDoesTheChores() {
        List<Supervisor> fleet = List.of(supervisor("s1"), supervisor("s2"), supervisor("s3"));

        for (int pass = 0; pass < 5; pass++) {
            fleet.forEach(Supervisor::tick);
            long holders = fleet.stream().filter(supervisor -> supervisor.housekeeping().holdsLease()).count();
            assertEquals(1, holders, "pass " + pass + " had " + holders + " housekeepers");
            clock.advance(Duration.ofSeconds(2));
        }
    }

    @Test
    void theChoresMoveToAnotherMemberWhenTheHousekeeperDies() {
        Supervisor first = supervisor("s1");
        Supervisor second = supervisor("s2");
        first.tick();
        second.tick();
        assertTrue(first.housekeeping().holdsLease());

        first.die();
        clock.advance(TTL.plusSeconds(1));
        second.tick();
        second.tick();

        assertTrue(second.housekeeping().holdsLease());
    }

    @Test
    void theReportSaysWhereItsTenantListCameFrom() {
        // A fallback that does not say it is a fallback is how the missing tenant registry gets forgotten.
        Supervisor configured = new Supervisor(
            store, clock, "s1", Set.of(ACME), 1.0, TTL, Balancer.Config.defaults()
        );
        configured.tick();
        assertEquals("configuration", reportOf(configured).registrySource());

        store.put(Keys.tenantRegistration(ACME), "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));
        clock.advance(Duration.ofSeconds(2));
        configured.tick();
        assertEquals("registry", reportOf(configured).registrySource());
    }

    @Test
    void theReportIsReadableOutOfTheStore() throws Exception {
        store.put(Keys.tenantRegistration(ACME), "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));
        Supervisor only = supervisor("s1");
        only.tick();

        var document = org.opensearch.supervisor.model.Json.mapper()
            .readTree(store.get(Keys.HOUSEKEEPING_STATUS).orElseThrow().value());

        assertEquals("s1", document.get("holder").asText());
        assertEquals("acme", document.get("unpooled_tenants").get(0).asText());
        assertEquals(1, document.get("members_live").asInt());
    }

    /**
     * A store that hands back bytes nobody can parse, at the revisions it really has.
     *
     * <p>Fault injection rather than a condition the system produces on its own, and the distinction matters:
     * an unreadable document <em>at a revision this process wrote itself</em> means the process cannot read its
     * own writing, which in practice means a classpath skew or a store returning something other than what it
     * stored. Overwriting the key cannot simulate it, because that changes the revision and the staleness check
     * catches it first — which is why this needs a decorator.
     */
    private static final class CorruptingStore implements org.opensearch.controlplane.store.KeyValueStore {
        private final org.opensearch.controlplane.store.KeyValueStore delegate;
        private final java.util.Set<String> corrupt = new java.util.HashSet<>();

        CorruptingStore(org.opensearch.controlplane.store.KeyValueStore delegate) {
            this.delegate = delegate;
        }

        void corrupt(String key) {
            corrupt.add(key);
        }

        private Versioned mangle(String key, Versioned versioned) {
            if (corrupt.contains(key) == false) {
                return versioned;
            }
            // Same revision, unparseable bytes.
            return new Versioned("{\"not\":\"a lease\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                versioned.revision());
        }

        @Override
        public java.util.Optional<Versioned> get(String key) {
            return delegate.get(key).map(versioned -> mangle(key, versioned));
        }

        @Override
        public java.util.NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
            java.util.NavigableMap<String, Versioned> scanned = new java.util.TreeMap<>();
            delegate.scanPrefix(keyPrefix).forEach((key, versioned) -> scanned.put(key, mangle(key, versioned)));
            return scanned;
        }

        @Override
        public long put(String key, byte[] value) {
            return delegate.put(key, value);
        }

        @Override
        public long put(String key, byte[] value, long expectedRevision) {
            return delegate.put(key, value, expectedRevision);
        }

        @Override
        public boolean delete(String key) {
            return delegate.delete(key);
        }

        @Override
        public boolean delete(String key, long expectedRevision) {
            return delegate.delete(key, expectedRevision);
        }

        @Override
        public void close() {
            // InMemoryKeyValueStore has nothing to close, and narrowing the throws clause is what the
            // interface allows.
        }
    }

    @Test
    void aLeaseThatCannotBeReadAtMyOwnRevisionIsNotHeldRatherThanFatal() {
        // Found by accident, running the real process against a real store with a mismatched Jackson on the
        // classpath: every lease came back unreadable. The classpath was mine to fix, but the response was not
        // proportionate. refresh() deliberately treats an unreadable lease as no lease, so that a corrupt write
        // cannot make a pool permanently unclaimable -- and holdsKey was the one of five call sites that
        // dereferenced it anyway, which turned that into a NullPointerException on every pass for ever, since
        // the bad bytes are still there on the next one.
        CorruptingStore corrupting = new CorruptingStore(store);
        LeaseManager manager = new LeaseManager(corrupting, clock, "s1", TTL);
        manager.refresh();
        assertTrue(manager.tryAcquire(A), "acquired normally");

        corrupting.corrupt(Keys.poolLease(A));
        manager.refresh();

        assertFalse(manager.holds(A), "unreadable means not held, not an exception");

        // And the ordinary recovery path still works: the key looks silent, the TTL expires, and it becomes
        // claimable, at which point whoever claims it overwrites the bad document.
        clock.advance(TTL.plusSeconds(1));
        manager.refresh();
        assertTrue(manager.claimable(java.util.Set.of(A)).contains(A));
    }
}
