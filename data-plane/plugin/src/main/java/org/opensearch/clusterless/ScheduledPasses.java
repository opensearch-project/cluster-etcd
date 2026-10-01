/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The plugin's periodic work: publishing what this node holds, and reading what it should hold.
 *
 * <p>Separate from the passes themselves so that every interesting question about either can be asked of a
 * single pass rather than of a clock. Tests drive {@code publish()} and {@code poll()} directly.
 *
 * <h2>Its own thread pool, with two threads</h2>
 *
 * <p>Dedicated, so that a slow store cannot consume threads that queries need. <b>Two</b> rather than one,
 * which is the detail worth explaining: both passes block on store round trips, and on a single thread a slow
 * read of an index document would delay the heartbeat behind it. Liveness is computed from the heartbeat's key
 * changing, so that delay is not a latency problem — past the staleness threshold the supervisor declares the
 * node lost and rebuilds its shards elsewhere. Two threads keep the two clocks independent.
 *
 * <p>And no more than two, because there are exactly two periodic tasks. A pool that could run three passes at
 * once would let two copies of the same pass overlap, and both passes assume they are the only one running.
 *
 * <h2>Fixed delay, not fixed rate</h2>
 *
 * <p>D3.7 says the interval a node declares must be the interval it writes at, and the supervisor derives that
 * node's staleness threshold from the declared number. {@code scheduleWithFixedDelay} measures the gap
 * <em>between</em> passes, so the true period is the interval plus however long a pass takes — a few
 * milliseconds normally, and as long as a store round trip when the store is slow.
 *
 * <p>Fixed delay is still right. At a fixed rate, a store slow enough to make passes overlap produces a queue
 * that can never drain, each entry writing a heartbeat that was already stale when it was queued. The drift is
 * bounded by the default threshold being six times the interval, which leaves room for a pass to take five
 * times the interval before liveness is at risk — and a node in that state has a real problem that a scheduling
 * policy should not hide.
 */
public final class ScheduledPasses implements Closeable {

    private static final Logger logger = LogManager.getLogger(ScheduledPasses.class);

    /** Named for the pool rather than the plugin, because this is what shows up in the hot threads output. */
    public static final String THREAD_POOL_NAME = "clusterless_passes";

    private static final int THREADS = 2;

    private final ThreadPool threadPool;
    private final List<Scheduler.Cancellable> scheduled = new ArrayList<>();

    public ScheduledPasses(ThreadPool threadPool) {
        this.threadPool = Objects.requireNonNull(threadPool, "threadPool");
    }

    public static ExecutorBuilder<?> executorBuilder(Settings settings) {
        return new FixedExecutorBuilder(Objects.requireNonNull(settings), THREAD_POOL_NAME, THREADS, THREADS, THREAD_POOL_NAME);
    }

    /**
     * Runs a pass once immediately, then every interval.
     *
     * <p>The first pass is synchronous so that a node has registered and heartbeated by the time it finishes
     * starting, rather than one interval later — and so that a store it cannot reach at all shows up in the
     * startup log rather than in the first scheduled pass, when nobody is looking.
     */
    public synchronized void start(String what, Runnable pass, TimeValue interval) {
        pass.run();
        scheduled.add(threadPool.scheduleWithFixedDelay(pass, interval, THREAD_POOL_NAME));
        logger.info("clusterless: {} every {}", what, interval);
    }

    @Override
    public synchronized void close() {
        scheduled.forEach(Scheduler.Cancellable::cancel);
        scheduled.clear();
    }
}
