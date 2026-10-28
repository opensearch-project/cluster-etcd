package org.opensearch.supervisor.app;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Calls {@link Cycle#tick()} on a schedule, and decides what to do when a pass fails.
 *
 * <p>The failure policy is the only real content here, and it is short because the lease mechanism already
 * answers most of it.
 *
 * <p><b>A failing pass must not stop the loop.</b> Reconciliation is level-triggered, so a pass that threw
 * halfway through has left no partial state to unwind — the next pass observes whatever is actually there and
 * proceeds. Exiting instead would turn a transient store timeout into an outage of the management plane.
 *
 * <p><b>And a permanently failing pass needs no special handling either</b>, which is the part worth noticing.
 * {@code Supervisor.tick()} renews leases first and then gives up any pool whose pass throws, without releasing it,
 * so a pool whose passes all throw stops being renewed, its lease expires, and a peer takes it over. Housekeeping and
 * the other pools still run. A tick that fails while renewing has not renewed what it failed on. Either way the
 * wedged process stops being anybody's problem without having to detect that it is wedged. It keeps trying and keeps saying so, because a process that exits
 * loses the one thing it is still good for: reporting why it cannot work. So there is no failure count that
 * triggers an exit, deliberately. The escalating log is the whole response.
 *
 * <p><b>Fixed delay, not fixed rate.</b> A pass slower than the interval must not cause passes to queue: that
 * turns a slow store into an accelerating pile of concurrent work, which is how a struggling system becomes a
 * dead one. Overrun is logged instead, since {@code R2.6} cares about a pass that cannot finish in time.
 *
 * <p>Single-threaded on purpose. A supervisor is not thread-safe and has no reason to be — one pool has one
 * owner, and one process's passes are sequential by nature.
 */
public final class Runner {

    private static final Log log = Log.of("runner");

    /** After this many consecutive failures, say so at error rather than warn. */
    static final int ESCALATE_AFTER = 3;

    private final Cycle cycle;
    private final Options options;
    private final AtomicBoolean stopping = new AtomicBoolean();
    /** Counted down once {@link #run()} has released its leases, or given up trying. */
    private final CountDownLatch finished = new CountDownLatch(1);

    private long passes;
    private long failures;
    private long consecutiveFailures;
    private volatile Thread loopThread;
    /** When a requested stop must be over by, in {@link System#nanoTime()} terms; meaningful once stopping. */
    private volatile long stopDeadlineNanos;

    public Runner(Cycle cycle, Options options) {
        this.cycle = cycle;
        this.options = options;
    }

    /** Runs until {@link #stop()}, or until {@code --max-passes} is reached. Returns the exit code. */
    public int run() {
        loopThread = Thread.currentThread();
        try {
            return loop();
        } finally {
            finished.countDown();
        }
    }

    private int loop() {
        long intervalNanos = options.interval().toNanos();

        while (stopping.get() == false) {
            long startedAt = System.nanoTime();
            passes++;
            onePass();
            long elapsedNanos = System.nanoTime() - startedAt;

            if (options.maxPasses() != Options.FOREVER && passes >= options.maxPasses()) {
                log.info("reached the configured pass limit", "passes", passes, "failures", failures);
                break;
            }
            if (elapsedNanos > intervalNanos) {
                // Not throttled down in response. Deciding to reconcile less often because reconciling is slow
                // is a judgement about the cause, and the cause is usually the store rather than the pass.
                log.warn("pass took longer than the interval, so the next one starts immediately",
                    "pass", passes, "took_ms", elapsedNanos / 1_000_000, "interval_ms", intervalNanos / 1_000_000);
                continue;
            }
            sleep(intervalNanos - elapsedNanos);
        }

        return shutdown();
    }

    private void onePass() {
        try {
            cycle.tick();
            if (consecutiveFailures > 0) {
                log.info("passes are succeeding again", "after_failures", consecutiveFailures);
                consecutiveFailures = 0;
            }
            if (log.isEnabled(Log.Level.DEBUG)) {
                log.debug("pass complete", "state", cycle.describe());
            }
        } catch (Throwable thrown) {
            // Throwable rather than Exception. An OutOfMemoryError or a NoClassDefFoundError from a store jar
            // is exactly the kind of thing that should be reported by a process still able to report, and
            // letting it escape kills the loop without ever writing the line that explains it.
            failures++;
            consecutiveFailures++;
            String message = "reconciliation pass failed; the next pass will recompute from observed state";
            if (consecutiveFailures >= ESCALATE_AFTER) {
                log.error("reconciliation pass failed repeatedly; leases will expire and peers will take over",
                    thrown, "pass", passes, "consecutive", consecutiveFailures, "total", failures);
            } else {
                log.warn(message, thrown, "pass", passes, "consecutive", consecutiveFailures);
            }
        }
    }

    /**
     * Asks the loop to finish the pass it is in and stop.
     *
     * <p>Called from a shutdown hook, so on a different thread. The pass is not interrupted mid-way: a pass is
     * short, and interrupting one would abandon a store call that may be halfway through a write that the next
     * owner then has to reason about. Waiting for it costs at most one interval.
     */
    public synchronized void stop() {
        if (stopping.get() == false) {
            // The deadline before the flag, so whoever sees the flag also sees the deadline.
            stopDeadlineNanos = System.nanoTime() + options.shutdownBudget().toNanos();
            stopping.set(true);
            log.info("stopping after the current pass");
            LockSupport.unpark(loopThread);
        }
    }

    /**
     * Stops, and waits for the leases to be released, for at most the shutdown budget. For the shutdown hook.
     *
     * <p><b>The wait is the point.</b> The JVM halts as soon as every shutdown hook has returned, so a hook that only
     * asked for a stop returned at once and the process died while the loop was still finishing its pass, before it
     * released anything. Peers then waited out a whole lease TTL for pools that a clean stop hands over on their next
     * pass. The budget covers the whole stop, the pass in flight included, as {@code --shutdown-budget} says; a stop
     * that cannot finish inside it leaves the leases to expire, as before.
     *
     * @return whether the loop finished within the budget
     */
    public boolean stopAndAwait() {
        stop();
        if (loopThread == null) {
            // Not started, so nothing is held and nothing will be released.
            return true;
        }
        long remaining = stopDeadlineNanos - System.nanoTime();
        try {
            return finished.await(Math.max(0, remaining), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private int shutdown() {
        // Releasing leases explicitly is worth real effort: it turns failover from "wait for the TTL to expire"
        // into "a peer claims it on its next pass", which is the difference between a rolling restart being
        // invisible and a rolling restart being a series of gaps.
        Thread releasing = new Thread(() -> {
            try {
                cycle.shutdown();
            } catch (Throwable thrown) {
                log.error("releasing leases failed; peers will take over when the lease expires instead",
                    thrown, "lease_ttl_ms", options.leaseTtl().toMillis());
            }
        }, "supervisor-shutdown");
        releasing.setDaemon(true);
        releasing.start();

        // A stop that was requested has already spent part of its budget on the pass it interrupted, so the release
        // gets what is left, and the shutdown hook waiting on this gives up at the same moment rather than earlier.
        long budgetNanos = stopping.get()
            ? Math.max(0, stopDeadlineNanos - System.nanoTime())
            : options.shutdownBudget().toNanos();
        try {
            releasing.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(budgetNanos)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (releasing.isAlive()) {
            // A store that will not answer must not keep the process alive. The leases expire on their own.
            log.warn("gave up waiting for a clean shutdown; leases will expire instead",
                "budget_ms", options.shutdownBudget().toMillis(), "lease_ttl_ms", options.leaseTtl().toMillis());
        } else {
            log.info("released every lease", "passes", passes, "failures", failures);
        }

        // Non-zero only if the process never managed a single successful pass, which is the case a supervising
        // process should restart or alert on. Intermittent failures are the store's problem, not this one's.
        boolean neverWorked = passes > 0 && failures == passes;
        if (neverWorked) {
            log.error("every pass failed", "passes", passes);
        }
        return neverWorked ? 1 : 0;
    }

    /**
     * Waits out the rest of the interval, unless asked to stop.
     *
     * <p>The deadline is the authority rather than any single {@code parkNanos} call, because park may return
     * early and even spuriously — without re-checking, a spurious wake would silently shorten the interval.
     * But the stop flag has to be checked in the same loop, or {@code stop()}'s unpark would be undone by the
     * next park and shutdown would still wait out a full interval for nothing.
     */
    private void sleep(long nanos) {
        long deadline = System.nanoTime() + nanos;
        long remaining = nanos;
        while (remaining > 0 && stopping.get() == false) {
            LockSupport.parkNanos(remaining);
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            remaining = deadline - System.nanoTime();
        }
    }

    public long passes() {
        return passes;
    }

    public long failures() {
        return failures;
    }
}
