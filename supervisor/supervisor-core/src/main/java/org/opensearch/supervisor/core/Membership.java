package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.supervisor.MemberRecord;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Who else is running. The whole of the membership protocol: records that are renewed, and stop being
 * renewed.
 *
 * <p>No gossip, no consensus, no failure detector beyond {@link RevisionSilence}. That is enough because
 * membership only feeds the balancer, which is advisory — a member wrongly believed dead costs a wasted
 * compare-and-set, not a split brain, because the lease is what actually decides.
 */
public final class Membership {

    private final KeyValueStore store;
    private final Clock clock;
    private final String supervisorId;
    private final double capacityWeight;
    private final Duration ttl;
    private final RevisionSilence silence;
    private final long startedAt;

    private Map<String, MemberRecord> live = Map.of();
    private List<String> unreadable = List.of();

    public Membership(
        KeyValueStore store,
        Clock clock,
        String supervisorId,
        double capacityWeight,
        Duration ttl
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.supervisorId = Objects.requireNonNull(supervisorId, "supervisorId");
        this.capacityWeight = capacityWeight;
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.silence = new RevisionSilence(clock.instant());
        this.startedAt = clock.millis();
    }

    /**
     * Publishes this member and reads the others.
     *
     * <p>Written unconditionally rather than with a compare-and-set: this key has exactly one writer by
     * construction, since it is named after the writer. Nothing to race with.
     */
    public void refresh(long pass, Set<PoolId> poolsHeld, double weightHeld) {
        store.put(Keys.member(supervisorId), new MemberRecord(
            supervisorId,
            startedAt,
            clock.millis() + ttl.toMillis(),
            pass,
            capacityWeight,
            new ArrayList<>(poolsHeld),
            weightHeld
        ).toBytes());

        Map<String, MemberRecord> found = new LinkedHashMap<>();
        List<String> bad = new ArrayList<>();
        var scanned = store.scanPrefix(Keys.MEMBERS_PREFIX);

        for (var entry : scanned.entrySet()) {
            Duration silent = silence.observe(entry.getKey(), entry.getValue().revision(), clock.instant());
            MemberRecord record;
            try {
                record = MemberRecord.from(entry.getValue().value());
            } catch (RuntimeException e) {
                bad.add(entry.getKey());
                continue;
            }
            // Silence, not the record's own expiry: same reasoning as everywhere else in this service. My
            // own record is always live -- I just wrote it, and its revision moved.
            if (record.supervisorId().equals(supervisorId) || silent.compareTo(ttl) < 0) {
                found.put(record.supervisorId(), record);
            }
        }
        silence.retain(scanned.keySet());
        live = found;
        unreadable = List.copyOf(bad);
    }

    public Map<String, MemberRecord> live() {
        return live;
    }

    public Map<String, Double> capacities() {
        Map<String, Double> capacities = new LinkedHashMap<>();
        live.forEach((id, record) -> capacities.put(id, record.capacityWeight()));
        return capacities;
    }

    public Map<String, Double> weightsHeld() {
        Map<String, Double> weights = new LinkedHashMap<>();
        live.forEach((id, record) -> weights.put(id, record.weightHeld()));
        return weights;
    }

    /** What each live member says it holds, which is how an unowned pool is spotted. */
    public Map<String, Set<PoolId>> poolsHeld() {
        Map<String, Set<PoolId>> held = new LinkedHashMap<>();
        live.forEach((id, record) -> held.put(id, Set.copyOf(record.poolsHeld())));
        return held;
    }

    /**
     * Member records long past their expiry, for the housekeeping holder to reap.
     *
     * @param graceMultiple how many TTLs past expiry before a record is deleted. Generously large on
     *     purpose: a reaped record that comes back is harmless, but reaping a member that was merely slow
     *     removes its capacity from the fair share and invites a pointless rebalance.
     */
    public List<String> reapable(int graceMultiple) {
        List<String> reapable = new ArrayList<>();
        long cutoff = clock.millis() - ttl.toMillis() * (long) graceMultiple;
        store.scanPrefix(Keys.MEMBERS_PREFIX).forEach((key, versioned) -> {
            try {
                MemberRecord record = MemberRecord.from(versioned.value());
                if (record.supervisorId().equals(supervisorId) == false && record.expiresAt() < cutoff) {
                    reapable.add(key);
                }
            } catch (RuntimeException e) {
                reapable.add(key);
            }
        });
        return reapable;
    }

    public List<String> unreadable() {
        return unreadable;
    }

    public String supervisorId() {
        return supervisorId;
    }
}
