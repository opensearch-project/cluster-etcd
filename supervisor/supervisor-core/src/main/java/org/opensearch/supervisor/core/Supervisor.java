package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.plan.Planner;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * One supervisor process: renew membership, renew leases, take or give up one pool, then run a
 * reconciliation pass over each pool held.
 *
 * <p>Still <b>read-only</b>. Milestone 4 exists before milestone 5 on purpose — running several of these
 * against one store while none of them writes an assignment is the safest possible way to shake out leases
 * and failover, and it needs no data plane and no network at all.
 *
 * <p>{@link #tick()} is explicit rather than a thread, so a test can drive N supervisors in one JVM in a
 * deterministic order and assert what no amount of sleeping could.
 */
public final class Supervisor {

    /** How many TTLs past expiry before a dead member's record is reaped. */
    public static final int MEMBER_REAP_GRACE = 10;

    private final KeyValueStore store;
    private final Clock clock;
    private final String supervisorId;
    private final Set<TenantId> configuredTenants;
    private final LeaseManager leases;
    private final Membership membership;
    private final Balancer.Config balancerConfig;
    private final PoolWeights weights;
    private final Housekeeping housekeeping;

    /**
     * The cadence assumed when nobody declares one, matching {@code supervisor-app}'s own default.
     *
     * <p>A default rather than a required argument because most callers are tests that never publish anything an
     * operator reads. The app passes the real value, which matters: {@code PASS_SATURATED} compares a pass's
     * duration against this number, so a wrong one would make the alert wrong rather than absent.
     */
    public static final Duration DECLARED_PASS_INTERVAL = Duration.ofSeconds(2);

    private final MemberStatusProjector memberStatus;
    /** Thresholds only: the methods used are pure functions of a registration and the config, with no history. */
    private final LivenessTracker thresholds;
    private final Planner.Policy policy;
    private final LivenessTracker.Config liveness;
    private final boolean readOnly;

    private final Map<PoolId, SupervisorLoop> loops = new LinkedHashMap<>();
    private Set<PoolId> held = Set.of();
    private int passesOverShare;
    private long pass;
    private Balancer.Decision lastDecision = new Balancer.Decision.Hold("not started");

    public Supervisor(
        KeyValueStore store,
        Clock clock,
        String supervisorId,
        Set<TenantId> configuredTenants,
        double capacityWeight,
        Duration leaseTtl,
        Balancer.Config balancerConfig
    ) {
        this(store, clock, supervisorId, configuredTenants, capacityWeight, leaseTtl, balancerConfig,
            Planner.Policy.defaults(), false);
    }

    /**
     * @param readOnly observe, diff and project but never plan or write. What milestones 3 and 4 ran as, kept
     *     because it is the safest way to introduce this service to a cluster that already has data in it.
     */
    public Supervisor(
        KeyValueStore store,
        Clock clock,
        String supervisorId,
        Set<TenantId> configuredTenants,
        double capacityWeight,
        Duration leaseTtl,
        Balancer.Config balancerConfig,
        Planner.Policy policy,
        boolean readOnly
    ) {
        this(store, clock, supervisorId, configuredTenants, capacityWeight, leaseTtl, balancerConfig, policy,
            readOnly, LivenessTracker.Config.defaults());
    }

    /**
     * @param liveness how patient this supervisor is about a node's silence. See
     *     {@link LivenessTracker.Config} — the direction that deserves care is making it shorter.
     */
    public Supervisor(
        KeyValueStore store,
        Clock clock,
        String supervisorId,
        Set<TenantId> configuredTenants,
        double capacityWeight,
        Duration leaseTtl,
        Balancer.Config balancerConfig,
        Planner.Policy policy,
        boolean readOnly,
        LivenessTracker.Config liveness
    ) {
        this(store, clock, supervisorId, configuredTenants, capacityWeight, leaseTtl, balancerConfig, policy,
            readOnly, liveness, DECLARED_PASS_INTERVAL);
    }

    /**
     * @param passInterval how often whatever drives this expects to call {@link #tick()}. <b>Declared, not
     *     enforced</b> — the loop lives in {@code supervisor-app} and this class never sleeps. It is here because
     *     it is published in the member-status document, where it is the denominator for "is a pass taking too
     *     long": a duration means nothing without the cadence it is competing with.
     */
    public Supervisor(
        KeyValueStore store,
        Clock clock,
        String supervisorId,
        Set<TenantId> configuredTenants,
        double capacityWeight,
        Duration leaseTtl,
        Balancer.Config balancerConfig,
        Planner.Policy policy,
        boolean readOnly,
        LivenessTracker.Config liveness,
        Duration passInterval
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.supervisorId = Objects.requireNonNull(supervisorId, "supervisorId");
        this.configuredTenants = Set.copyOf(configuredTenants);
        this.leases = new LeaseManager(store, clock, supervisorId, leaseTtl);
        this.membership = new Membership(store, clock, supervisorId, capacityWeight, leaseTtl);
        this.balancerConfig = Objects.requireNonNull(balancerConfig, "balancerConfig");
        this.weights = new PoolWeights(store);
        this.housekeeping = new Housekeeping(store, leases, this.configuredTenants, supervisorId);
        this.policy = Objects.requireNonNull(policy, "policy");
        this.liveness = Objects.requireNonNull(liveness, "liveness");
        this.thresholds = new LivenessTracker(clock.instant(), liveness);
        this.memberStatus = new MemberStatusProjector(
            store, supervisorId, liveness, passInterval.toMillis(), leaseTtl, this.policy, readOnly,
            clock.millis()
        );
        this.readOnly = readOnly;
    }

    /** One turn of the crank. */
    public void tick() {
        pass++;

        Pools pools = Pools.read(store);
        Set<PoolId> all = pools.all();

        leases.refresh();
        // Renew before anything else. A lease lost here means dropping the pool immediately rather than
        // finishing what was in progress: R6.3 says plans are cheap, so recompute rather than resume.
        Set<PoolId> stillHeld = new LinkedHashSet<>();
        for (PoolId pool : held) {
            if (all.contains(pool) && leases.renew(pool)) {
                stillHeld.add(pool);
            } else {
                loops.remove(pool);
            }
        }
        held = stillHeld;

        Map<PoolId, Double> poolWeights = weights.read(all, balancerConfig.defaultPoolWeight());
        membership.refresh(pass, held, weightOf(poolWeights));

        Balancer.View view = new Balancer.View(
            poolWeights,
            leases.claimable(all),
            membership.capacities(),
            Balancer.weightsHeldFrom(membership.weightsHeld(), supervisorId, weightOf(poolWeights)),
            supervisorId,
            held
        );

        double fairShare = Balancer.fairShareOf(view, balancerConfig);
        passesOverShare = weightOf(poolWeights) > fairShare * (1 + balancerConfig.hysteresis())
            ? passesOverShare + 1
            : 0;

        lastDecision = Balancer.decide(view, passesOverShare, balancerConfig);
        applyDecision(lastDecision);

        // Chores that belong to no pool, done by whichever member holds the one extra lease.
        housekeeping.tick(pass, pools, membership, clock.millis());

        // Only now, with ownership settled, does any reconciliation happen.
        java.util.Map<org.opensearch.supervisor.model.NodeId, org.opensearch.supervisor.model.node.Registration>
            seen = new LinkedHashMap<>();
        for (PoolId pool : held) {
            SupervisorLoop loop = loops.computeIfAbsent(pool, id -> readOnly
                ? new SupervisorLoop(store, clock, id, supervisorId, configuredTenants, liveness)
                // The lease check is a supplier closing over the manager, so it is evaluated at the moment
                // of the write rather than at the moment the loop was built.
                : new SupervisorLoop(store, clock, id, supervisorId, configuredTenants,
                    () -> leases.holds(id), policy, leases.epochOf(id), liveness)
            );
            loop.pass();
            seen.putAll(loop.lastRegistrations());
        }

        // What this process is running with, published after the pools are settled so `pools_held` is the answer
        // for the pass that just ran rather than the one before it. R3.3, which had no observable answer until now.
        memberStatus.project(seen, List.copyOf(held), thresholds, clock.millis());
    }

    private void applyDecision(Balancer.Decision decision) {
        if (decision instanceof Balancer.Decision.Claim claim) {
            if (leases.tryAcquire(claim.pool())) {
                Set<PoolId> next = new LinkedHashSet<>(held);
                next.add(claim.pool());
                held = next;
            }
            // Losing the race is the ordinary outcome of two members starting together, and not a failure.
        } else if (decision instanceof Balancer.Decision.Release release) {
            if (leases.release(release.pool())) {
                Set<PoolId> next = new LinkedHashSet<>(held);
                next.remove(release.pool());
                held = next;
                loops.remove(release.pool());
                passesOverShare = 0;
            }
        }
    }

    /**
     * Summed from the same total map the balancer divides, so the two cannot disagree about a denominator.
     * See {@link PoolWeights#read} for what happened when they did.
     */
    private double weightOf(Map<PoolId, Double> poolWeights) {
        return held.stream()
            .mapToDouble(pool -> poolWeights.getOrDefault(pool, balancerConfig.defaultPoolWeight()))
            .sum();
    }

    /** Stops renewing and gives up every lease, so a clean shutdown costs no failover window. */
    public void shutdown() {
        for (PoolId pool : Set.copyOf(held)) {
            leases.release(pool);
        }
        leases.releaseKey(Keys.HOUSEKEEPING_LEASE);
        store.delete(Keys.member(supervisorId));
        held = Set.of();
        loops.clear();
    }

    /**
     * Simulates a process that stopped without releasing anything — a crash, a kill, a partition.
     *
     * <p>The interesting failure, and the one the whole lease mechanism exists for: nothing is cleaned up,
     * so recovery has to come from other members noticing silence.
     */
    public void die() {
        held = Set.of();
        loops.clear();
    }

    public Set<PoolId> held() {
        return held;
    }

    /** Whether this process observes and reports but never writes anything a data node will act on. */
    public boolean isReadOnly() {
        return readOnly;
    }

    public Balancer.Decision lastDecision() {
        return lastDecision;
    }

    public long pass() {
        return pass;
    }

    public String supervisorId() {
        return supervisorId;
    }

    public Membership membership() {
        return membership;
    }

    public LeaseManager leases() {
        return leases;
    }

    public Diff lastDiffOf(PoolId pool) {
        SupervisorLoop loop = loops.get(pool);
        return loop == null ? null : loop.lastDiff();
    }

    public List<String> reapableMembers() {
        return membership.reapable(MEMBER_REAP_GRACE);
    }

    public Housekeeping housekeeping() {
        return housekeeping;
    }
}
