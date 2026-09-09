/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Ways of getting a {@link Watch}, whatever the backend can do.
 *
 * <p>{@link #watchOrPoll} is the one to call. A consumer that uses it works identically against etcd,
 * ZooKeeper and the filesystem store, and gains native efficiency wherever the backend has it without any
 * code change.
 */
public final class Watches {

    private Watches() {}

    /**
     * A native watch if the backend has one, otherwise a polling watch.
     *
     * <p>The fallback is not a stopgap. It exists because <b>the interface's contract is the polling
     * contract</b> — coalesced, occasionally spurious, no payload — so a consumer written against polling
     * is already written correctly for every native implementation. Building it the other way round, native
     * first and a fallback bolted on later, is how a consumer ends up quietly depending on delivery
     * guarantees that only one backend offers.
     *
     * @param interval how often to poll when falling back; ignored when the backend watches natively
     */
    public static Watch watchOrPoll(KeyValueStore store, String keyPrefix, Duration interval, Runnable onChange) {
        Optional<Watch> native_ = store.watch(keyPrefix, onChange);
        return native_.orElseGet(() -> polling(store, keyPrefix, interval, onChange));
    }

    /**
     * A watch implemented by re-scanning the prefix and comparing revisions.
     *
     * <p>Detects additions, removals and modifications, because a modification moves a key's revision.
     * Costs one {@link KeyValueStore#scanPrefix} per interval, which is the honest price of not having
     * native support — and the same scan a level-triggered consumer performs anyway, so for a consumer that
     * would have polled regardless this is free.
     *
     * <p><b>It cannot see intermediate states.</b> A key written and deleted between two polls is never
     * reported, and a key written twice reports once. That is exactly the latitude {@link Watch} reserves,
     * which is why this fallback is a legitimate implementation rather than an approximation of one.
     *
     * <p>The first scan establishes a baseline and does <b>not</b> notify: a watch reports changes from when
     * it was registered, so a consumer that reads state and then registers does not immediately get told to
     * read it again.
     */
    public static Watch polling(KeyValueStore store, String keyPrefix, Duration interval, Runnable onChange) {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        Objects.requireNonNull(onChange, "onChange");
        if (interval.isPositive() == false) {
            throw new IllegalArgumentException("poll interval must be positive, got " + interval);
        }
        return new PollingWatch(store, keyPrefix, interval, onChange);
    }

    private static final class PollingWatch implements Watch, Runnable {

        private final KeyValueStore store;
        private final String keyPrefix;
        private final Runnable onChange;
        private final ScheduledExecutorService scheduler;
        private final AtomicBoolean closed = new AtomicBoolean();

        /** Key to revision as of the last scan. Only ever touched by the scheduler's single thread. */
        private Map<String, Long> lastSeen;

        PollingWatch(KeyValueStore store, String keyPrefix, Duration interval, Runnable onChange) {
            this.store = store;
            this.keyPrefix = keyPrefix;
            this.onChange = onChange;
            this.lastSeen = snapshot();

            // One daemon thread per watch. Wasteful if a consumer registers hundreds, and a consumer with
            // hundreds of prefixes should be watching one shorter prefix instead. Daemon so that a forgotten
            // watch cannot keep a JVM alive.
            this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "kv-poll-" + keyPrefix);
                thread.setDaemon(true);
                return thread;
            });
            scheduler.scheduleWithFixedDelay(
                this, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void run() {
            if (closed.get()) {
                return;
            }
            try {
                Map<String, Long> current = snapshot();
                boolean changed = current.equals(lastSeen) == false;
                lastSeen = current;
                if (changed) {
                    onChange.run();
                }
            } catch (RuntimeException e) {
                // Swallowed on purpose. scheduleWithFixedDelay cancels the task on any thrown exception,
                // so letting a transient store failure escape would silently stop the watch for good --
                // the worst outcome available, because the consumer keeps believing it is watching.
                // A consumer that must know about store failures learns it from its own reads.
                POLL_FAILURES.incrementAndGet();
            }
        }

        private Map<String, Long> snapshot() {
            Map<String, Long> revisions = new HashMap<>();
            store.scanPrefix(keyPrefix).forEach((key, versioned) -> revisions.put(key, versioned.revision()));
            return revisions;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                // Not awaited: close may be called from a notification callback, which runs on this very
                // scheduler, and awaiting termination there would deadlock.
                scheduler.shutdownNow();
            }
        }

        @Override
        public String toString() {
            return "PollingWatch[" + keyPrefix + "]";
        }
    }

    /**
     * Polls that threw, since the process started.
     *
     * <p>Exposed because a silently failing watch is indistinguishable from a quiet keyspace, and that is
     * the failure this whole class is most likely to produce. A consumer with metrics should publish it.
     */
    public static long pollFailures() {
        return POLL_FAILURES.get();
    }

    private static final AtomicLong POLL_FAILURES = new AtomicLong();
}
