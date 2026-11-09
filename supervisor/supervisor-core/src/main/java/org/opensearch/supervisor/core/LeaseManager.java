package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.OutcomeUnknownException;
import org.opensearch.controlplane.store.RevisionConflictException;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.supervisor.Lease;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Acquires, renews and releases the one lock in this system.
 *
 * <p>Compare-and-set on the lease key's own revision, never a global lock — R6.2, and the reason it matters
 * is what happens when it fails: a stale supervisor loses individual writes rather than corrupting shared
 * state. Losing a CAS is not an error here; it is the mechanism working.
 *
 * <p><b>Expiry is judged from observed revision silence, not from the {@code expires_at} field.</b> A holder
 * renews every pass, so its lease key's revision advances every pass; a challenger decides a lease is
 * claimable only after it has itself watched that revision sit still for longer than the TTL. Reading the
 * holder's own timestamp instead would let clock skew steal a healthy lease. It also gives the startup grace
 * period for free: a supervisor that has just started has observed nothing sit still, so it steals nothing.
 *
 * <p><b>A write whose answer was lost is settled by reading the key back</b>, never treated as lost. Treating it as
 * lost gives up a lease this process may well hold: an applied renewal leaves the key at a revision the next
 * renewal does not expect, so that one conflicts with this process's own write, and an applied acquisition leaves a
 * pool with a fresh lease that nobody renews. Comparing bytes is sound here, as it would not be for most keys,
 * because a lease names its holder and the millisecond it was written: no other process's write leaves the same
 * bytes. This process's previous write can, within the same millisecond, and finding that means the same thing.
 */
public final class LeaseManager {

    /** Failover time is this. Too short and a GC pause hands a pool away; too long and one goes unmanaged. */
    public static Duration defaultTtl(Duration passInterval) {
        Duration threePasses = passInterval.multipliedBy(3);
        Duration floor = Duration.ofSeconds(15);
        return threePasses.compareTo(floor) > 0 ? threePasses : floor;
    }

    private final KeyValueStore store;
    private final Clock clock;
    private final String supervisorId;
    private final Duration ttl;
    private final RevisionSilence silence;

    /** Revision of my last successful write per key, which is what every renewal compare-and-sets against. */
    private final Map<String, Long> myRevisions = new HashMap<>();

    private Map<String, Observed> observed = Map.of();

    /**
     * Bytes of each write whose answer was lost, kept until the next {@link #refresh} reads the key. A write can
     * apply after the read-back that followed it, or the read-back can fail too; either way the next refresh, which
     * reads every lease anyway, sees whether these bytes are there.
     */
    private final Map<String, byte[]> unsettled = new HashMap<>();

    private record Observed(Lease lease, long revision, Duration silentFor) {}

    public LeaseManager(KeyValueStore store, Clock clock, String supervisorId, Duration ttl) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.supervisorId = Objects.requireNonNull(supervisorId, "supervisorId");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.silence = new RevisionSilence(clock.instant());
    }

    /**
     * Reads every pool lease and updates how long each has sat unchanged. Must be called once per pass
     * before anything else here, because silence only accumulates when someone is watching.
     */
    public void refresh() {
        Map<String, Observed> current = new LinkedHashMap<>();
        Map<String, KeyValueStore.Versioned> scanned = new LinkedHashMap<>(store.scanPrefix(Keys.POOL_LEASES_PREFIX));
        // One extra read for the housekeeping lease, which lives outside the pool prefix because it belongs
        // to no pool. Cheaper than widening the scan to everything under supervisor/.
        store.get(Keys.HOUSEKEEPING_LEASE).ifPresent(versioned -> scanned.put(Keys.HOUSEKEEPING_LEASE, versioned));

        for (var entry : scanned.entrySet()) {
            Lease lease;
            try {
                lease = Lease.from(entry.getValue().value());
            } catch (RuntimeException e) {
                // An unreadable lease is treated as no lease at all, so a corrupt write cannot make a pool
                // permanently unclaimable. Silence still accumulates against the key.
                lease = null;
            }
            Duration silent = silence.observe(entry.getKey(), entry.getValue().revision(), clock.instant());
            current.put(entry.getKey(), new Observed(lease, entry.getValue().revision(), silent));
            byte[] pending = unsettled.get(entry.getKey());
            if (pending != null && Arrays.equals(pending, entry.getValue().value())) {
                myRevisions.put(entry.getKey(), entry.getValue().revision());
            }
        }
        silence.retain(scanned.keySet());
        unsettled.clear();
        observed = current;
    }

    /** Pools whose lease is absent, unreadable, or has sat unchanged for longer than the TTL. */
    public Set<PoolId> claimable(Set<PoolId> pools) {
        Set<PoolId> claimable = new LinkedHashSet<>();
        for (PoolId pool : pools) {
            if (isClaimable(Keys.poolLease(pool))) {
                claimable.add(pool);
            }
        }
        return claimable;
    }

    /** Absent, unreadable, or sat unchanged for longer than the TTL. */
    public boolean isClaimable(String key) {
        Observed current = observed.get(key);
        return current == null || current.lease() == null || current.silentFor().compareTo(ttl) >= 0;
    }

    /** Pools this process currently believes it holds, having written their lease and not lost a CAS. */
    public Set<PoolId> held(Set<PoolId> pools) {
        Set<PoolId> held = new LinkedHashSet<>();
        for (PoolId pool : pools) {
            if (holds(pool)) {
                held.add(pool);
            }
        }
        return held;
    }

    public boolean holds(PoolId pool) {
        return holdsKey(Keys.poolLease(pool));
    }

    public boolean holdsKey(String key) {
        Long mine = myRevisions.get(key);
        if (mine == null) {
            return false;
        }
        Observed current = observed.get(key);
        if (current == null || current.revision() != mine) {
            // Someone else has written since I did, so the lease is theirs and my bookkeeping is stale.
            return false;
        }
        if (current.lease() == null) {
            // Unreadable at my own revision, which should not happen and must not be fatal when it does.
            // refresh() promises that an unreadable lease counts as no lease so a corrupt write cannot make a
            // pool permanently unclaimable, and this was the one of five call sites that dereferenced it anyway
            // -- turning that promise into a NullPointerException on every pass, for ever, since the bad bytes
            // are still there next time. Answering "I do not hold it" instead lets the ordinary path take over:
            // the key goes silent, the TTL expires, and somebody claims it and overwrites the bad document.
            return false;
        }
        return supervisorId.equals(current.lease().holder());
    }

    /**
     * Takes a lease, or does not.
     *
     * @return true if it is now held. False means someone else won the race, which is the ordinary outcome
     *     of two supervisors starting together and is not worth logging as a failure.
     */
    public boolean tryAcquire(PoolId pool) {
        return tryAcquireKey(Keys.poolLease(pool));
    }

    public boolean tryAcquireKey(String key) {
        Observed current = observed.get(key);
        long now = clock.millis();
        long epoch = current == null || current.lease() == null ? 1 : current.lease().epoch() + 1;
        Lease lease = new Lease(supervisorId, epoch, now, now + ttl.toMillis());
        byte[] bytes = lease.toBytes();

        try {
            long revision = current == null
                ? store.put(key, bytes, KeyValueStore.ABSENT)
                : store.put(key, bytes, current.revision());
            return wrote(key, lease, revision);
        } catch (RevisionConflictException e) {
            myRevisions.remove(key);
            return false;
        } catch (OutcomeUnknownException e) {
            return settle(key, lease, bytes);
        }
    }

    /**
     * Extends a lease this process holds.
     *
     * @return false if it has been lost — which means dropping the pool immediately, without finishing
     *     whatever was in progress. R6.3: plans are cheap, so recompute rather than resume.
     */
    public boolean renew(PoolId pool) {
        return renewKey(Keys.poolLease(pool));
    }

    public boolean renewKey(String key) {
        Long mine = myRevisions.get(key);
        if (mine == null) {
            return false;
        }
        Observed current = observed.get(key);
        Lease renewed = (current == null || current.lease() == null
            ? new Lease(supervisorId, 1, clock.millis(), 0)
            : current.lease()).renewedAt(clock.millis(), ttl.toMillis());
        byte[] bytes = renewed.toBytes();

        try {
            return wrote(key, renewed, store.put(key, bytes, mine));
        } catch (RevisionConflictException e) {
            myRevisions.remove(key);
            return false;
        } catch (OutcomeUnknownException e) {
            return settle(key, renewed, bytes);
        }
    }

    private boolean wrote(String key, Lease lease, long revision) {
        myRevisions.put(key, revision);
        observed = withObserved(key, new Observed(lease, revision, Duration.ZERO));
        return true;
    }

    /**
     * Whether a write whose answer was lost left this process holding the lease, read off the key.
     *
     * <p>These bytes there: the write applied, and its revision is the one to renew against. The key still at the
     * revision this process last wrote: the write did not apply, and the lease is still held, unrenewed for a pass,
     * which the TTL allows for. Anything else is somebody else's write. If the read fails too, the failure propagates
     * like any other store failure, and the next {@link #refresh} settles the question.
     */
    private boolean settle(String key, Lease lease, byte[] written) {
        unsettled.put(key, written);
        Optional<KeyValueStore.Versioned> now = store.get(key);
        if (now.isPresent() && Arrays.equals(now.get().value(), written)) {
            return wrote(key, lease, now.get().revision());
        }
        Long mine = myRevisions.get(key);
        if (now.isPresent() && mine != null && now.get().revision() == mine) {
            return true;
        }
        myRevisions.remove(key);
        return false;
    }

    /**
     * Gives up a lease deliberately, so another member can take it without waiting out the TTL.
     *
     * <p>Deleting rather than letting it expire is the difference between a rebalance that costs one pass
     * and one that costs a failover window.
     */
    public boolean release(PoolId pool) {
        return releaseKey(Keys.poolLease(pool));
    }

    public boolean releaseKey(String key) {
        Long mine = myRevisions.remove(key);
        if (mine == null) {
            return false;
        }
        try {
            store.delete(key, mine);
            silence.forget(key);
            observed = withoutObserved(key);
            return true;
        } catch (RevisionConflictException e) {
            // Already someone else's. Nothing to give up.
            return false;
        } catch (OutcomeUnknownException e) {
            // Given up either way: deleted, or left to expire, since nothing here renews it any more.
            return true;
        }
    }

    public Optional<Lease> leaseOf(PoolId pool) {
        Observed current = observed.get(Keys.poolLease(pool));
        return current == null ? Optional.empty() : Optional.ofNullable(current.lease());
    }

    /** The epoch stamped into assignments written under this lease, for diagnosing a split brain later. */
    public long epochOf(PoolId pool) {
        return leaseOf(pool).map(Lease::epoch).orElse(0L);
    }

    public String supervisorId() {
        return supervisorId;
    }

    public Duration ttl() {
        return ttl;
    }

    private Map<String, Observed> withObserved(String key, Observed value) {
        Map<String, Observed> next = new LinkedHashMap<>(observed);
        next.put(key, value);
        return next;
    }

    private Map<String, Observed> withoutObserved(String key) {
        Map<String, Observed> next = new LinkedHashMap<>(observed);
        next.remove(key);
        return next;
    }
}
