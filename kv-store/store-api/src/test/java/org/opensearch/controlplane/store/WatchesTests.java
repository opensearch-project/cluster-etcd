/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The polling watch and the native-or-polling choice.
 *
 * <p>Uses short intervals and latches rather than sleeps, so the suite stays quick and does not flake on a
 * loaded machine. Where a test has to prove a <em>negative</em> — that nothing was notified — it waits a
 * bounded number of intervals and then asserts, which is the best available without a fake clock.
 */
class WatchesTests {

    private static final Duration FAST = Duration.ofMillis(20);

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------
    // polling
    // ------------------------------------------------------------------

    @Test
    void aWriteUnderThePrefixNotifies() throws Exception {
        KeyValueStore store = new InMemoryKeyValueStore();
        CountDownLatch notified = new CountDownLatch(1);

        try (Watch watch = Watches.polling(store, "indices/", FAST, notified::countDown)) {
            store.put("indices/a", bytes("one"));
            assertTrue(notified.await(5, TimeUnit.SECONDS), "a write under the prefix must notify");
            assertEquals("PollingWatch[indices/]", watch.toString());
        }
    }

    @Test
    void aModificationNotifies() throws Exception {
        KeyValueStore store = new InMemoryKeyValueStore();
        store.put("indices/a", bytes("one"));
        CountDownLatch notified = new CountDownLatch(1);

        try (Watch ignored = Watches.polling(store, "indices/", FAST, notified::countDown)) {
            // Same key, new value: only the revision distinguishes it, which is what the poll compares.
            store.put("indices/a", bytes("two"));
            assertTrue(notified.await(5, TimeUnit.SECONDS), "a modification must notify");
        }
    }

    @Test
    void aDeletionNotifies() throws Exception {
        KeyValueStore store = new InMemoryKeyValueStore();
        store.put("indices/a", bytes("one"));
        CountDownLatch notified = new CountDownLatch(1);

        try (Watch ignored = Watches.polling(store, "indices/", FAST, notified::countDown)) {
            store.delete("indices/a");
            assertTrue(notified.await(5, TimeUnit.SECONDS), "a deletion must notify");
        }
    }

    /** A watch reports changes from when it was registered, so existing keys are not a change. */
    @Test
    void theBaselineScanDoesNotNotify() throws Exception {
        KeyValueStore store = new InMemoryKeyValueStore();
        store.put("indices/a", bytes("one"));
        store.put("indices/b", bytes("two"));
        AtomicInteger calls = new AtomicInteger();

        try (Watch ignored = Watches.polling(store, "indices/", FAST, calls::incrementAndGet)) {
            Thread.sleep(FAST.toMillis() * 6);
            assertEquals(0, calls.get(),
                "state that predates the watch is not a change; a consumer reads then registers");
        }
    }

    /** Writes outside the prefix are not this watch's business. */
    @Test
    void aWriteOutsideThePrefixDoesNotNotify() throws Exception {
        KeyValueStore store = new InMemoryKeyValueStore();
        AtomicInteger calls = new AtomicInteger();

        try (Watch ignored = Watches.polling(store, "indices/", FAST, calls::incrementAndGet)) {
            store.put("aliases/a", bytes("one"));
            store.put("tasks/x", bytes("two"));
            Thread.sleep(FAST.toMillis() * 6);
            assertEquals(0, calls.get());
        }
    }

    @Test
    void closingStopsNotifications() throws Exception {
        KeyValueStore store = new InMemoryKeyValueStore();
        AtomicInteger calls = new AtomicInteger();

        Watch watch = Watches.polling(store, "indices/", FAST, calls::incrementAndGet);
        watch.close();
        store.put("indices/a", bytes("one"));
        Thread.sleep(FAST.toMillis() * 6);

        assertEquals(0, calls.get(), "a closed watch must not notify");
    }

    @Test
    void closeIsIdempotent() {
        Watch watch = Watches.polling(new InMemoryKeyValueStore(), "x/", FAST, () -> {});

        watch.close();
        watch.close();
    }

    /** Closing from inside a callback must not deadlock — the callback runs on the polling thread. */
    @Test
    void closingFromWithinACallbackDoesNotDeadlock() throws Exception {
        KeyValueStore store = new InMemoryKeyValueStore();
        CountDownLatch closed = new CountDownLatch(1);
        Watch[] holder = new Watch[1];

        holder[0] = Watches.polling(store, "indices/", FAST, () -> {
            holder[0].close();
            closed.countDown();
        });
        store.put("indices/a", bytes("one"));

        assertTrue(closed.await(5, TimeUnit.SECONDS), "close from a callback must return");
    }

    /**
     * A store failure must not silently kill the watch.
     *
     * <p>{@code scheduleWithFixedDelay} cancels its task on any thrown exception, so an escaping error would
     * stop notifications for ever while the consumer went on believing it was watching — the worst failure
     * this class can produce, and invisible.
     */
    @Test
    void aFailingScanDoesNotStopTheWatch() throws Exception {
        InMemoryKeyValueStore delegate = new InMemoryKeyValueStore();
        AtomicInteger scans = new AtomicInteger();
        KeyValueStore flaky = new ForwardingStore(delegate) {
            @Override
            public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
                // Scan 1 is the constructor's baseline and must succeed; fail the next few, then recover.
                int scan = scans.incrementAndGet();
                if (scan >= 2 && scan <= 4) {
                    throw new IllegalStateException("backend unavailable");
                }
                return super.scanPrefix(keyPrefix);
            }
        };
        CountDownLatch notified = new CountDownLatch(1);

        try (Watch ignored = Watches.polling(flaky, "indices/", FAST, notified::countDown)) {
            delegate.put("indices/a", bytes("one"));
            assertTrue(notified.await(5, TimeUnit.SECONDS),
                "the watch must survive transient scan failures and notify once the store recovers");
            assertTrue(Watches.pollFailures() > 0, "failures must be counted, not hidden");
        }
    }

    @Test
    void aNonPositiveIntervalIsRejected() {
        KeyValueStore store = new InMemoryKeyValueStore();

        assertThrows(IllegalArgumentException.class,
            () -> Watches.polling(store, "x/", Duration.ZERO, () -> {}));
        assertThrows(IllegalArgumentException.class,
            () -> Watches.polling(store, "x/", Duration.ofMillis(-1), () -> {}));
    }

    // ------------------------------------------------------------------
    // choosing native or polling
    // ------------------------------------------------------------------

    /** The default is no native support, so an ordinary backend falls back without the caller knowing. */
    @Test
    void watchOrPollFallsBackWhenTheBackendCannotWatch() {
        KeyValueStore store = new InMemoryKeyValueStore();

        assertEquals(Optional.empty(), store.watch("x/", () -> {}), "the SPI default is unsupported");
        try (Watch watch = Watches.watchOrPoll(store, "x/", FAST, () -> {})) {
            assertEquals("PollingWatch[x/]", watch.toString());
        }
    }

    /** And a backend that does watch is used directly, with no polling thread started. */
    @Test
    void watchOrPollPrefersANativeWatch() {
        Watch native_ = () -> {};
        KeyValueStore watchable = new ForwardingStore(new InMemoryKeyValueStore()) {
            @Override
            public Optional<Watch> watch(String keyPrefix, Runnable onChange) {
                return Optional.of(native_);
            }
        };

        try (Watch chosen = Watches.watchOrPoll(watchable, "x/", FAST, () -> {})) {
            assertSame(native_, chosen, "a native watch must be used rather than wrapped");
        }
    }

    /** Everything delegates, so a test can override exactly one method. */
    private abstract static class ForwardingStore implements KeyValueStore {

        private final KeyValueStore delegate;

        ForwardingStore(KeyValueStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<Versioned> get(String key) {
            return delegate.get(key);
        }

        @Override
        public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
            return delegate.scanPrefix(keyPrefix);
        }

        @Override
        public long put(String key, byte[] value) {
            return delegate.put(key, value);
        }

        @Override
        public long put(String key, byte[] value, long expectedRevision) {
            return delegate.put(key, value, expectedRevision);
        }

        @Override
        public boolean delete(String key) {
            return delegate.delete(key);
        }

        @Override
        public boolean delete(String key, long expectedRevision) {
            return delegate.delete(key, expectedRevision);
        }
    }
}
