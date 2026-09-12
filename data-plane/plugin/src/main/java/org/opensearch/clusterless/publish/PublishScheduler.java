/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.common.settings.Settings;

import java.io.Closeable;
import java.util.Objects;

/**
 * Calls {@link NodePublisher#publish()} on a timer, and nothing else.
 *
 * <p>Separate from the publisher so that every interesting question about publishing can be asked of a
 * pass rather than of a clock. Tests drive {@code publish()} directly.
 *
 * <h2>Its own thread pool</h2>
 *
 * <p>One thread, dedicated. Publishing does a small amount of work and then blocks on a network round trip
 * to the store, and doing that on a search or management thread would let a slow store consume threads
 * that queries need. Sizing it at one is not a limit, it is the design: two passes overlapping would race
 * to decide whether the inventory had changed.
 *
 * <h2>Fixed delay, not fixed rate, and the small honesty problem in it</h2>
 *
 * <p>D3.7 says the interval a node declares must be the interval it actually writes at, because the
 * supervisor derives that node's staleness threshold from the declared number. {@code scheduleWithFixedDelay}
 * measures the gap <em>between</em> passes, so the true period is the interval plus however long a pass
 * takes — a few milliseconds normally, and as long as a store round trip when the store is slow.
 *
 * <p>Fixed delay is still the right choice. At a fixed rate, a store slow enough to make passes overlap
 * produces a queue of passes that can never drain, each one writing a heartbeat that was already stale
 * when it was queued. The drift is bounded by the default threshold being six times the interval, which
 * leaves room for a pass to take five times the interval before liveness is at risk — and a node in that
 * state has a real problem that a scheduling policy should not hide.
 */
public final class PublishScheduler implements Closeable {

    private static final Logger logger = LogManager.getLogger(PublishScheduler.class);

    /** Named for the pool, not for the plugin, because this is what shows up in the hot threads output. */
    public static final String THREAD_POOL_NAME = "clusterless_publish";

    private final NodePublisher publisher;
    private final ThreadPool threadPool;
    private final TimeValue interval;

    private Scheduler.Cancellable scheduled;

    public PublishScheduler(NodePublisher publisher, ThreadPool threadPool, TimeValue interval) {
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.threadPool = Objects.requireNonNull(threadPool, "threadPool");
        this.interval = Objects.requireNonNull(interval, "interval");
    }

    public static ExecutorBuilder<?> executorBuilder(Settings settings) {
        return new FixedExecutorBuilder(Objects.requireNonNull(settings), THREAD_POOL_NAME, 1, 1, THREAD_POOL_NAME);
    }

    /**
     * Publishes once immediately, then every interval.
     *
     * <p>The first pass is synchronous so that a node is registered and heartbeating by the time it
     * finishes starting, rather than one interval later. It also means a store that cannot be reached at
     * all is visible in the startup log rather than in the first scheduled pass, when nobody is looking.
     */
    public synchronized void start() {
        if (scheduled != null) {
            throw new IllegalStateException("already started");
        }
        publisher.publish();
        scheduled = threadPool.scheduleWithFixedDelay(publisher::publish, interval, THREAD_POOL_NAME);
        logger.info("clusterless: publishing every {}", interval);
    }

    @Override
    public synchronized void close() {
        if (scheduled != null) {
            scheduled.cancel();
            scheduled = null;
        }
    }
}
