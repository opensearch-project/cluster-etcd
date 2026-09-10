package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.Registration;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The chores that belong to no pool.
 *
 * <p>Every one of these is a configuration mistake that is otherwise <b>completely silent</b>, which is why
 * they need a home at all. A tenant nobody has bound to a pool has no pool loop to notice it; a node in no
 * pool is skipped by every loop by construction; a pool nobody holds is, definitionally, watched by nobody.
 * All three look exactly like a healthy quiet system.
 *
 * <p>Given one small lease rather than a singleton service or duplicated across every member: one extra
 * uncontended compare-and-set per pass, and the work is milliseconds.
 */
public final class Housekeeping {

    /** Findings about the fleet itself, projected for an operator. */
    public record Report(
        long pass,
        String holder,
        long observedAt,
        List<String> unpooledTenants,
        List<String> unpooledNodes,
        List<String> unownedPools,
        List<String> poolsWithoutDefinition,
        List<String> duplicateDefaults,
        List<String> overloadedDedicatedPools,
        int membersLive,
        int membersReaped,
        String registrySource,
        List<String> unreadable
    ) {}

    private final KeyValueStore store;
    private final LeaseManager leases;
    private final TenantDiscovery tenants;
    private final String supervisorId;

    private Report lastReport;

    public Housekeeping(
        KeyValueStore store,
        LeaseManager leases,
        Set<TenantId> configuredTenants,
        String supervisorId
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.leases = Objects.requireNonNull(leases, "leases");
        this.tenants = new TenantDiscovery(store, configuredTenants);
        this.supervisorId = Objects.requireNonNull(supervisorId, "supervisorId");
    }

    /**
     * Runs the chores if this member holds the housekeeping lease, and tries to take it if nobody does.
     *
     * @return true if this member did the work
     */
    public boolean tick(long pass, Pools pools, Membership membership, long nowMillis) {
        if (leases.holdsKey(Keys.HOUSEKEEPING_LEASE) == false) {
            if (leases.isClaimable(Keys.HOUSEKEEPING_LEASE) == false
                || leases.tryAcquireKey(Keys.HOUSEKEEPING_LEASE) == false) {
                return false;
            }
        } else if (leases.renewKey(Keys.HOUSEKEEPING_LEASE) == false) {
            return false;
        }

        TenantDiscovery.Discovered discovered = tenants.discover();
        List<String> unreadable = new ArrayList<>(pools.unreadable());
        unreadable.addAll(membership.unreadable());
        discovered.malformed().forEach(bad -> unreadable.add(Keys.TENANTS_PREFIX + bad));

        // A tenant the control plane is serving that no pool is bound to. Nothing else in the system reports
        // this: no shards are placed, no health is computed, no tasks are reaped, and every one of those
        // looks like an idle tenant.
        List<String> unpooledTenants = pools.unboundOf(discovered.tenants())
            .stream()
            .map(TenantId::value)
            .toList();

        List<String> unpooledNodes = unpooledNodes(pools);

        Set<PoolId> unowned = Balancer.unowned(pools.all(), membership.poolsHeld());
        List<String> unownedPools = unowned.stream().map(PoolId::value).toList();

        List<String> reaped = new ArrayList<>();
        for (String key : membership.reapable(Supervisor.MEMBER_REAP_GRACE)) {
            if (store.delete(key)) {
                reaped.add(key);
            }
        }

        Report report = new Report(
            pass,
            supervisorId,
            nowMillis,
            unpooledTenants,
            unpooledNodes,
            unownedPools,
            poolsWithoutDefinition(pools, discovered),
            pools.problems(),
            pools.overloadedDedicatedPools(),
            membership.live().size(),
            reaped.size(),
            discovered.source().name().toLowerCase(java.util.Locale.ROOT),
            unreadable
        );
        store.put(Keys.HOUSEKEEPING_STATUS, Json.write(report, "housekeeping report"));
        lastReport = report;
        return true;
    }

    /**
     * Nodes in no pool. Checked here rather than in a pool's loop because a pool loop only ever sees its own
     * nodes — an unpooled node is invisible to all of them at once.
     */
    private List<String> unpooledNodes(Pools pools) {
        List<String> unpooled = new ArrayList<>();
        for (var entry : store.scanPrefix(Keys.NODE_REGISTRATION_PREFIX).entrySet()) {
            NodeId nodeId;
            Registration registration;
            try {
                nodeId = NodeId.of(Keys.idAfter(Keys.NODE_REGISTRATION_PREFIX, entry.getKey()));
                registration = NodeJson.readRegistration(entry.getValue().value());
            } catch (RuntimeException e) {
                continue;
            }
            if (pools.poolOf(nodeId, registration.pool()).isEmpty()) {
                unpooled.add(nodeId.value());
            }
        }
        return unpooled;
    }

    /** A tenant bound to a pool that has no definition — an operator's typo, stranding that tenant. */
    private List<String> poolsWithoutDefinition(Pools pools, TenantDiscovery.Discovered discovered) {
        Set<String> missing = new LinkedHashSet<>();
        for (TenantId tenant : discovered.tenants()) {
            pools.bindingOf(tenant).ifPresent(binding -> {
                if (pools.all().contains(binding.pool()) == false) {
                    missing.add(binding.pool().value());
                }
            });
        }
        return List.copyOf(missing);
    }

    public Report lastReport() {
        return lastReport;
    }

    public boolean holdsLease() {
        return leases.holdsKey(Keys.HOUSEKEEPING_LEASE);
    }
}
