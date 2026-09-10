package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.Watches;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Differ;
import org.opensearch.supervisor.model.diff.FindingAges;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.plan.Plan;
import org.opensearch.supervisor.model.plan.Planner;
import org.opensearch.supervisor.model.task.TaskRecord;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * One pool's reconciliation pass: observe, diff, project, and — only if asked — plan and act.
 *
 * <p><b>Read-only unless a lease check is supplied.</b> Constructed without one it behaves exactly as it did
 * through milestones 3 and 4, which is what let leases and failover be shaken out by a fleet that could not
 * touch anything. {@code REQUIREMENTS.md} §10 argues that order better than this class can.
 *
 * <p>Acting is deliberately narrow: {@link Planner} issues one kind of instruction, for one finding, and only
 * ever adds a copy. Nothing removes anything yet, so the worst a wrong decision can do is create a surplus
 * copy — a self-clearing finding rather than lost data.
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
    private final Applier applier;
    private final Planner.Policy policy;
    private final BooleanSupplier stillHoldsLease;
    private final long epoch;
    private final TaskQueue tasks;
    private final StatusProjector status;

    private Diff lastDiff;
    private Plan lastPlan;
    private Applier.Outcome lastOutcome;
    private TaskQueue.Outcome lastTaskOutcome;
    private int lastStatusWrites;

    /** Read-only: observes, diffs and projects, and writes nothing a node will act on. */
    public SupervisorLoop(
        KeyValueStore store,
        Clock clock,
        PoolId pool,
        String supervisorId,
        Set<TenantId> configuredTenants
    ) {
        this(store, clock, pool, supervisorId, configuredTenants, null, null, 0);
    }

    /**
     * Acting.
     *
     * @param stillHoldsLease evaluated as late as possible, immediately before each write and again between
     *     writes. Passed as a supplier rather than a boolean precisely so that the caller cannot accidentally
     *     evaluate it once at the top of a pass and hand over a stale answer.
     * @param epoch the lease epoch, stamped into every assignment written so that a split brain is
     *     diagnosable afterwards from data the system already wrote
     */
    public SupervisorLoop(
        KeyValueStore store,
        Clock clock,
        PoolId pool,
        String supervisorId,
        Set<TenantId> configuredTenants,
        BooleanSupplier stillHoldsLease,
        Planner.Policy policy,
        long epoch
    ) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.pool = Objects.requireNonNull(pool, "pool");
        this.observer = new Observer(store, clock, new LivenessTracker(clock.instant()));
        this.projector = new Projector(store, supervisorId);
        this.tenants = new TenantDiscovery(store, configuredTenants);
        this.applier = new Applier(store, clock);
        this.stillHoldsLease = stillHoldsLease;
        this.policy = policy == null ? Planner.Policy.defaults() : policy;
        this.epoch = epoch;
        this.tasks = new TaskQueue(store, clock, supervisorId, TaskQueue.Policy.defaults());
        this.status = new StatusProjector(store);
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
        // The queue drains after this, so these are the *previous* pass's figures. One pass of lag is the
        // price of tasks being a hint rather than a driver: draining first would let a task decide what gets
        // reconciled, which R2.1 forbids. The alternative -- projecting twice per pass -- costs a write.
        projector.project(diff, observation, clock.millis() - startedAt, watchPollFailures(), taskFigures());

        lastDiff = diff;
        lastPlan = null;
        lastOutcome = null;

        if (stillHoldsLease != null) {
            Plan plan = Planner.plan(observation, diff, epoch, policy);
            lastPlan = plan;
            lastOutcome = applier.apply(plan, observation, stillHoldsLease);

            // After reconciling, never before. A task is a hint about where to look, so draining the queue
            // must not be what decides what gets done -- R2.1. Running it here rather than first also means
            // a task is judged against the state this pass has just brought about.
            if (stillHoldsLease.getAsBoolean()) {
                lastTaskOutcome = tasks.drain(observation, epoch);

                // The projections the control plane reads. Computed from the observation this pass began
                // with, so they describe what was seen rather than what has just been asked for -- which is
                // the invariant docrep peer resolution depends on.
                lastStatusWrites = status.project(observation);
            }
        }
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

    public Plan lastPlan() {
        return lastPlan;
    }

    public Applier.Outcome lastOutcome() {
        return lastOutcome;
    }

    public TaskQueue.Outcome lastTaskOutcome() {
        return lastTaskOutcome;
    }

    /** How many projection keys the last pass wrote; zero in steady state. */
    public int lastStatusWrites() {
        return lastStatusWrites;
    }

    private Projector.Tasks taskFigures() {
        if (lastTaskOutcome == null) {
            return Projector.Tasks.NONE;
        }
        return new Projector.Tasks(
            lastTaskOutcome.claimed(), lastTaskOutcome.completed(), lastTaskOutcome.failed(),
            lastTaskOutcome.reaped(), lastTaskOutcome.lost()
        );
    }

    public boolean isReadOnly() {
        return stillHoldsLease == null;
    }

    public PoolId pool() {
        return pool;
    }

    /** How long to wait before the next pass, absent a watch waking it sooner. */
    public static Duration defaultPassInterval() {
        return Duration.ofSeconds(2);
    }
}
