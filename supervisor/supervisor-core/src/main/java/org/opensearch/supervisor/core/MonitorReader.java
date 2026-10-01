package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.monitor.Sample;
import org.opensearch.supervisor.model.projection.MemberStatus;
import org.opensearch.supervisor.model.projection.Projections.Convergence;
import org.opensearch.supervisor.model.projection.Projections.HousekeepingStatus;
import org.opensearch.supervisor.model.projection.Projections.TenantDiff;
import org.opensearch.supervisor.model.projection.ReapingStatus;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads one {@link Sample} out of the store, and nothing else.
 *
 * <p><b>The only class in the monitoring path that touches a store, which is the point.</b> The rules live in
 * {@code supervisor-model}, which cannot see a {@code KeyValueStore} at all, so "the monitor cannot write" is a
 * property of the module graph rather than a promise in a comment — and this one small class is the only thing
 * needing review for it.
 *
 * <p>Every read is a prefix scan or a single get, and a document that will not parse costs itself and nothing else:
 * it lands in {@code unreadable}, which is its own alert. Losing the whole sample because one key was written by a
 * newer build would be the same mistake as failing a heartbeat parse on an unknown field.
 */
public final class MonitorReader {

    private final KeyValueStore store;
    private final Clock clock;

    public MonitorReader(KeyValueStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    /** @param previous the last sample, or null. Rules needing two observations simply do not fire on a first run. */
    public Sample read(Sample previous) {
        List<String> unreadable = new ArrayList<>();

        List<String> pools = new ArrayList<>();
        for (String key : store.scanPrefix(Keys.POOL_DEFINITIONS_PREFIX).keySet()) {
            pools.add(key.substring(Keys.POOL_DEFINITIONS_PREFIX.length()));
        }

        Map<String, String> leaseHolders = new LinkedHashMap<>();
        for (var entry : store.scanPrefix(Keys.POOL_LEASES_PREFIX).entrySet()) {
            // The holder is read out of the lease document, and a lease that will not parse is treated as unheld
            // rather than as absent -- the conservative direction, since POOL_UNSUPERVISED is the louder alert.
            try {
                var lease = org.opensearch.supervisor.model.supervisor.Lease.from(entry.getValue().value());
                leaseHolders.put(poolOf(entry.getKey()), lease.holder());
            } catch (RuntimeException broken) {
                unreadable.add(entry.getKey());
            }
        }

        Map<String, Convergence> convergence = new LinkedHashMap<>();
        Map<String, ReapingStatus> reaping = new LinkedHashMap<>();
        for (var entry : store.scanPrefix("supervisor/status/pools/").entrySet()) {
            String key = entry.getKey();
            try {
                if (key.endsWith("/convergence")) {
                    Convergence parsed = Convergence.from(entry.getValue().value());
                    convergence.put(parsed.pool().value(), parsed);
                } else if (key.endsWith("/reaping")) {
                    reaping.put(poolFromStatusKey(key), ReapingStatus.from(entry.getValue().value()));
                }
            } catch (RuntimeException broken) {
                unreadable.add(key);
            }
        }

        Map<String, TenantDiff> diffs = new LinkedHashMap<>();
        for (var entry : store.scanPrefix("supervisor/status/diff/").entrySet()) {
            try {
                TenantDiff parsed = TenantDiff.from(entry.getValue().value());
                diffs.put(parsed.tenant().value(), parsed);
            } catch (RuntimeException broken) {
                unreadable.add(entry.getKey());
            }
        }

        Map<String, MemberStatus> members = new LinkedHashMap<>();
        for (var entry : store.scanPrefix("supervisor/status/members/").entrySet()) {
            try {
                MemberStatus parsed = MemberStatus.from(entry.getValue().value());
                members.put(parsed.supervisorId(), parsed);
            } catch (RuntimeException broken) {
                unreadable.add(entry.getKey());
            }
        }

        List<String> liveMembers = new ArrayList<>();
        for (String key : store.scanPrefix(Keys.MEMBERS_PREFIX).keySet()) {
            liveMembers.add(key.substring(Keys.MEMBERS_PREFIX.length()));
        }

        HousekeepingStatus housekeeping = null;
        var stored = store.get(Keys.HOUSEKEEPING_STATUS);
        if (stored.isPresent()) {
            try {
                housekeeping = HousekeepingStatus.from(stored.get().value());
            } catch (RuntimeException broken) {
                unreadable.add(Keys.HOUSEKEEPING_STATUS);
            }
        }

        return new Sample(
            clock.millis(), previous, pools, leaseHolders, convergence, diffs, members, reaping,
            liveMembers, housekeeping, store.get(Keys.HOUSEKEEPING_LEASE).isPresent(), unreadable
        );
    }

    private static String poolOf(String leaseKey) {
        String rest = leaseKey.substring(Keys.POOL_LEASES_PREFIX.length());
        int slash = rest.indexOf('/');
        return slash < 0 ? rest : rest.substring(0, slash);
    }

    private static String poolFromStatusKey(String key) {
        String rest = key.substring("supervisor/status/pools/".length());
        int slash = rest.indexOf('/');
        return slash < 0 ? rest : rest.substring(0, slash);
    }
}
