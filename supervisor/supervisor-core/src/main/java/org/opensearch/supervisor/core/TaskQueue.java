package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.task.TaskCompletion;
import org.opensearch.supervisor.model.task.TaskRecord;
import org.opensearch.supervisor.model.task.TaskState;
import org.opensearch.supervisor.model.task.TaskType;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Drains the control plane's work queue: claim, evaluate, complete or fail, and eventually reap.
 *
 * <p>The control plane writes one record per index write and never revisits it, so
 * {@code cluster.pending_tasks} reports a pile that grows without bound. This is the other half of that
 * handoff — and {@code cluster.pending_tasks} starts telling the truth as soon as claiming works, because it
 * reports only {@code pending}.
 *
 * <p><b>None of this is required for correctness.</b> R2.1 and R4.4: with the queue empty the supervisor
 * converges from the index documents alone, and a task that is lost, duplicated, or arrives before the
 * document it refers to changes nothing. That is why this runs after reconciliation rather than driving it,
 * and why every operation here is safe to do twice.
 */
public final class TaskQueue {

    /**
     * @param maxAttempts how many times a task may be taken up before it is given up on. R4.2 wants repeated
     *     failure to become visible rather than looping silently.
     * @param claimLease how long a claim is good for. Generous, because renewing costs a write per claimed
     *     task per pass and nothing here is harmed by a task being worked twice.
     * @param retention how long a completed record is kept before deletion. The only reason it is written at
     *     all rather than deleted outright: an operator needs a moment to see that something finished.
     * @param ceiling records touched per pass, so a backlog of ten thousand tasks makes progress rather than
     *     turning one pass into a very long write.
     */
    public record Policy(int maxAttempts, Duration claimLease, Duration retention, int ceiling) {
        public static Policy defaults() {
            return new Policy(5, Duration.ofMinutes(1), Duration.ofHours(1), 200);
        }
    }

    /** What one pass did, for the convergence document and for tests. */
    public record Outcome(int claimed, int completed, int failed, int reaped, int lost, List<Stuck> stuck) {
        public boolean didAnything() {
            return claimed + completed + failed + reaped > 0;
        }
    }

    /** A task worth telling an operator about. */
    public record Stuck(TenantId tenant, String taskId, String because) {}

    private final KeyValueStore store;
    private final Clock clock;
    private final String supervisorId;
    private final Policy policy;

    public TaskQueue(KeyValueStore store, Clock clock, String supervisorId, Policy policy) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.supervisorId = Objects.requireNonNull(supervisorId, "supervisorId");
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    /**
     * One pass over the queue for every tenant in this observation.
     *
     * @param epoch the lease epoch, stamped into a claim so that a claim made by a supervisor which had
     *     already lost the pool is identifiable afterwards
     */
    public Outcome drain(Observation observation, long epoch) {
        int claimed = 0;
        int completed = 0;
        int failed = 0;
        int reaped = 0;
        int lost = 0;
        int touched = 0;
        List<Stuck> stuck = new ArrayList<>();

        long now = clock.millis();

        for (TenantId tenant : observation.tenants().keySet()) {
            String prefix = Keys.tasksPrefix(tenant);
            List<Entry> entries = read(prefix, tenant, stuck);
            entries.sort((a, b) -> TaskRecord.STABLE_ORDER.compare(a.record(), b.record()));

            for (Entry entry : entries) {
                if (touched >= policy.ceiling()) {
                    break;
                }
                TaskRecord task = entry.record();

                if (task.state().isTerminal()) {
                    if (task.state() == TaskState.FAILED) {
                        // Never reaped: a deleted failure is an invisible failure, and R4.2 asks for the
                        // opposite. Clearing these is an operator's decision.
                        stuck.add(new Stuck(tenant, task.taskId(),
                            "failed after " + task.attempts() + " attempts: " + task.lastError()));
                        continue;
                    }
                    if (now - task.terminalAt() >= policy.retention().toMillis()) {
                        touched++;
                        if (delete(entry)) {
                            reaped++;
                        } else {
                            lost++;
                        }
                    }
                    continue;
                }

                boolean mine = task.isHeldBy(supervisorId);
                boolean claimable = task.state() == TaskState.PENDING || task.isClaimExpired(now);
                if (mine == false && claimable == false) {
                    // Somebody else's live claim. Leaving it alone is the point of the lease.
                    continue;
                }

                TaskCompletion.Verdict verdict = TaskCompletion.evaluate(task, tenant, observation);

                if (verdict.satisfied()) {
                    touched++;
                    if (write(entry, task.completed(now))) {
                        completed++;
                    } else {
                        lost++;
                    }
                    continue;
                }

                if (mine && task.isClaimExpired(now) == false) {
                    // Our own live claim, not yet done. Nothing to write: re-evaluating next pass costs
                    // nothing, and renewing every pass would cost a write per claimed task for no benefit.
                    continue;
                }

                if (task.type() == TaskType.UNKNOWN) {
                    // Not claimed, not failed, not completed. Claiming it would take responsibility for work
                    // this build cannot name; the honest move is to say so and leave it for a newer one.
                    stuck.add(new Stuck(tenant, task.taskId(),
                        "task type is not recognised by this build; leaving it for a newer supervisor rather"
                            + " than reporting success or failure for work it cannot evaluate"));
                    continue;
                }

                // The claim has expired -- ours or somebody's. Whether that costs an attempt depends on
                // whether the work is advancing. A task still making progress is slow, not stuck, and
                // spending an attempt on it would eventually fail an index for being large. Only a task that
                // has stopped moving works its way toward being given up on, which is what R4.2 asks for.
                if (verdict.progress() > task.progress()) {
                    touched++;
                    if (write(entry, task.renewed(
                        supervisorId, epoch, now, policy.claimLease().toMillis(), verdict.progress()
                    ))) {
                        claimed++;
                    } else {
                        lost++;
                    }
                    continue;
                }

                if (task.attempts() >= policy.maxAttempts()) {
                    touched++;
                    String reason = "given up after " + task.attempts() + " attempts; last verdict: "
                        + verdict.because();
                    if (write(entry, task.failed(reason, now))) {
                        failed++;
                        stuck.add(new Stuck(tenant, task.taskId(), reason));
                    } else {
                        lost++;
                    }
                    continue;
                }

                touched++;
                if (write(entry, task.claimedBy(
                    supervisorId, epoch, now, policy.claimLease().toMillis(), verdict.progress()
                ))) {
                    claimed++;
                } else {
                    // Another supervisor claimed it first. The ordinary outcome of a race, and not a failure:
                    // whatever they do with it is work this one no longer has to.
                    lost++;
                }
            }
        }

        return new Outcome(claimed, completed, failed, reaped, lost, stuck);
    }

    private record Entry(String key, TaskRecord record, long revision) {}

    private List<Entry> read(String prefix, TenantId tenant, List<Stuck> stuck) {
        List<Entry> entries = new ArrayList<>();
        for (var scanned : store.scanPrefix(prefix).entrySet()) {
            try {
                entries.add(new Entry(
                    scanned.getKey(),
                    TaskRecord.from(scanned.getValue().value()),
                    scanned.getValue().revision()
                ));
            } catch (RuntimeException e) {
                // One unreadable record must not stop the queue draining. R2.5 again.
                stuck.add(new Stuck(tenant, Keys.idAfter(prefix, scanned.getKey()),
                    "record could not be parsed: " + e.getMessage()));
            }
        }
        return entries;
    }

    /**
     * Compare-and-set on the record's own revision — R4.1 and R6.2, a lease rather than a lock.
     *
     * <p>The holder can die, and then the claim simply expires and anybody may take it by winning the same
     * compare-and-set. Losing that race returns false, which is the mechanism working.
     */
    private boolean write(Entry entry, TaskRecord updated) {
        try {
            store.put(entry.key(), updated.toBytes(), entry.revision());
            return true;
        } catch (RevisionConflictException e) {
            return false;
        }
    }

    private boolean delete(Entry entry) {
        try {
            return store.delete(entry.key(), entry.revision());
        } catch (RevisionConflictException e) {
            return false;
        }
    }
}
