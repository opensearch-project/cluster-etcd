package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.Watches;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Differ;
import org.opensearch.supervisor.model.diff.FindingAges;
import org.opensearch.supervisor.model.observe.Observation;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/**
 * One pool's reconciliation pass: observe, diff, project. <b>Acts on nothing.</b>
 *
 * <p>That is milestone 3's whole point and {@code REQUIREMENTS.md} §10 argues it better than this class
 * can: a reconciler that observes correctly and acts on nothing is safe and immediately useful, while one
 * that acts before it observes correctly is neither. Placement arrives at milestone 5, behind the lease
 * checks and the work ceiling.
 *
 * <p>{@link #pass()} is explicit rather than a thread, for the same reason {@code FakeNode.tick()} is: a
 * pass that only advances when asked is a pass a test can reason about, and the balancer tests at milestone
 * 4 will need to drive several of these in one JVM.
 *
 * <p>Note what is <em>not</em> here. No plan survives a pass, and nothing is carried between passes except
 * the caches — the revision cache, the liveness history, and the finding ages — each of which is safe to
 * lose. R2.3 is satisfied by having nothing worth keeping.
 */
public final class SupervisorLoop {

    private final KeyValueStore store;
    private final Clock clock;
    private final PoolId pool;
    private final Observer observer;
    private final Projector projector;
    private final FindingAges ages = new FindingAges();
    private final TenantDiscovery tenants;

    private Diff lastDiff;

    public SupervisorLoop(
        KeyValueStore store,
        Clock clock,
        PoolId pool,
        String supervisorId,
        Set<TenantId> configuredTenants
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.pool = Objects.requireNonNull(pool, "pool");
        this.observer = new Observer(store, clock, new LivenessTracker(clock.instant()));
        this.projector = new Projector(store, supervisorId);
        this.tenants = new TenantDiscovery(store, configuredTenants);
    }

    /**
     * One pass. Reads the store, computes what is wrong, and writes that where an operator can see it.
     *
     * @return the diff, so a caller — a test, or a future planner — can act on it without re-reading
     */
    public Diff pass() {
        long startedAt = clock.millis();

        Pools pools = Pools.read(store);
        Set<TenantId> known = tenants.discover().tenants();
        Observation observation = observer.observe(pool, pools, known);

        Diff diff = Differ.diff(observation, ages);

        // Projection happens before anything would act, and happens in read-only mode. The observability
        // surface reports what was seen, not what was done, so it has to be right before there is anything
        // to be wrong about.
        projector.project(diff, observation, clock.millis() - startedAt, watchPollFailures());

        lastDiff = diff;
        return diff;
    }

    /**
     * {@code Watches.pollFailures()} counts polls that threw, and is only non-zero on the filesystem store
     * — both remote backends watch natively. Published regardless, because a silently dead watch looks
     * exactly like a quiet keyspace, which is this mechanism's most likely failure.
     */
    private long watchPollFailures() {
        return Watches.pollFailures();
    }

    public Diff lastDiff() {
        return lastDiff;
    }

    public PoolId pool() {
        return pool;
    }

    /** How long to wait before the next pass, absent a watch waking it sooner. */
    public static Duration defaultPassInterval() {
        return Duration.ofSeconds(2);
    }
}
