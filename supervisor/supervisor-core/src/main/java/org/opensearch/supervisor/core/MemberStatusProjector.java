package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.projection.MemberStatus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Publishes what this supervisor process is actually running with.
 *
 * <p>**R3.3**: the effective liveness threshold has to be observable, and until now it was not — the multipliers,
 * the floor and the per-node result were all real, all configurable, and written nowhere. `SUPERVISOR_KEYS.md`
 * described the document in the present tense while nothing called {@code Keys.memberStatus}.
 *
 * <p>Written by whichever process it describes, about itself, which is why there is no lease on it: the key is
 * per supervisor id, so it has exactly one writer by construction — the same argument that makes a node's own
 * documents safe.
 *
 * <p>Rewritten only when its content changes, like every other projection here. It changes when a process starts,
 * when it gains or loses a pool, and when the set of nodes whose thresholds are published changes — so in a steady
 * deployment it is written once and then never again, which is the right cost for a document whose purpose is to
 * answer "what is this thing configured to do".
 */
final class MemberStatusProjector {

    private final KeyValueStore store;
    private final String supervisorId;
    private final MemberStatus.Liveness base;
    private final long passIntervalMillis;
    private final long leaseTtlMillis;
    private final int workCeilingPerPass;
    private final int perTenantFloor;
    private final long minimumPasses;
    private final boolean rebalanceEnabled;
    private final boolean readOnly;
    private final long startedAt;
    private final String storeBackend;

    private String lastWritten;

    MemberStatusProjector(
        KeyValueStore store,
        String supervisorId,
        LivenessTracker.Config liveness,
        long passIntervalMillis,
        Duration leaseTtl,
        org.opensearch.supervisor.model.plan.Planner.Policy policy,
        boolean readOnly,
        long startedAt
    ) {
        this.store = store;
        this.supervisorId = supervisorId;
        this.base = new MemberStatus.Liveness(
            liveness.staleMultiplier(), liveness.lostMultiplier(), liveness.floor().toMillis(), Map.of()
        );
        this.passIntervalMillis = passIntervalMillis;
        this.leaseTtlMillis = leaseTtl.toMillis();
        this.workCeilingPerPass = policy.workCeiling();
        this.perTenantFloor = policy.perTenantFloor();
        this.minimumPasses = policy.minimumPasses();
        this.rebalanceEnabled = policy.rebalance();
        this.readOnly = readOnly;
        this.startedAt = startedAt;
        // The store's own class name, which is the only honest answer to "which backend is this". Nothing in this
        // repository names a backend at compile time, so there is nothing else to read -- and WATCH_DEAD needs it,
        // because the poll counter stays zero by design on the backends that watch natively.
        this.storeBackend = store.getClass().getSimpleName();
    }

    /**
     * @param registrations every node this process can see, for the effective thresholds
     * @param poolsHeld the pools this process currently holds the lease on
     */
    void project(Map<NodeId, Registration> registrations, List<PoolId> poolsHeld, LivenessTracker tracker,
        long nowMillis) {
        MemberStatus status = new MemberStatus(
            supervisorId,
            startedAt,
            versionOf(),
            storeBackend,
            poolsHeld.stream().map(PoolId::value).sorted().toList(),
            new MemberStatus.Liveness(
                base.staleMultiplier(), base.lostMultiplier(), base.floorMillis(),
                effectiveFor(registrations, tracker)
            ),
            passIntervalMillis,
            leaseTtlMillis,
            workCeilingPerPass,
            perTenantFloor,
            minimumPasses,
            rebalanceEnabled,
            readOnly,
            nowMillis
        );

        byte[] bytes = status.toBytes();
        // observedAt moves every pass, so the digest has to exclude it or this document would be rewritten for
        // ever -- the same trap every other projection here avoids, and the reason `write` takes a content digest.
        String digest = org.opensearch.supervisor.model.Json.digest(
            org.opensearch.supervisor.model.Json.write(
                List.of(status.poolsHeld(), status.liveness(), status.storeBackend(), status.version(),
                    status.passIntervalMillis(), status.leaseTtlMillis(), status.workCeilingPerPass(),
                    status.readOnly()),
                "member status content"
            )
        );
        if (digest.equals(lastWritten)) {
            return;
        }
        store.put(Keys.memberStatus(supervisorId), bytes);
        lastWritten = digest;
    }

    /**
     * The thresholds as they apply to real nodes, which is the part R3.3 is about.
     *
     * <p>Multipliers and a floor are inputs. What an operator needs is how long <em>this</em> node may be silent,
     * which depends on the interval the node itself advertises — so the answer is per node and cannot be derived
     * from the configuration alone. That is precisely why publishing the configuration would not have satisfied
     * the requirement.
     *
     * <p>Capped, and the nodes kept are the ones whose thresholds are **furthest from the majority**, because a
     * cap that kept the first fifty by id would hide the one misconfigured node in a pool of two hundred — which
     * is the only thing anybody reads this for.
     */
    private Map<String, MemberStatus.Thresholds> effectiveFor(
        Map<NodeId, Registration> registrations,
        LivenessTracker tracker
    ) {
        Map<String, MemberStatus.Thresholds> all = new LinkedHashMap<>();
        registrations.forEach((nodeId, registration) -> all.put(nodeId.value(), new MemberStatus.Thresholds(
            tracker.staleThreshold(registration).toMillis(),
            tracker.lostThreshold(registration).toMillis()
        )));
        if (all.size() <= MemberStatus.MAX_EFFECTIVE_NODES) {
            return all;
        }

        long commonest = all.values().stream()
            .collect(java.util.stream.Collectors.groupingBy(
                MemberStatus.Thresholds::staleMillis, java.util.stream.Collectors.counting()))
            .entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(0L);

        List<Map.Entry<String, MemberStatus.Thresholds>> unusual = new ArrayList<>(all.entrySet());
        unusual.sort(Comparator
            .comparingLong((Map.Entry<String, MemberStatus.Thresholds> entry) ->
                Math.abs(entry.getValue().staleMillis() - commonest)).reversed()
            .thenComparing(Map.Entry::getKey));

        Map<String, MemberStatus.Thresholds> kept = new LinkedHashMap<>();
        unusual.stream().limit(MemberStatus.MAX_EFFECTIVE_NODES)
            .forEach(entry -> kept.put(entry.getKey(), entry.getValue()));
        return kept;
    }

    private static String versionOf() {
        String version = MemberStatusProjector.class.getPackage().getImplementationVersion();
        return version == null ? "dev" : version;
    }
}
