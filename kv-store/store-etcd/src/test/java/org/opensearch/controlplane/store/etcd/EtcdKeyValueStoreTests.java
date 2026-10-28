/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.etcd;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.KeyValueStoreContractTests;
import org.opensearch.controlplane.store.KeyValueStores;
import org.opensearch.controlplane.store.RevisionConflictException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The etcd backend against the shared store contract, plus what only a real cluster shows.
 *
 * <pre>{@code
 * ../gradlew :store-etcd:test -Dcontrolplane.etcd=localhost:2379
 * }</pre>
 *
 * <p>Skipped without one. Unlike the ZooKeeper backend there is no companion unit test carrying the
 * risk, because etcd needs no key encoding to test — its keyspace is flat bytes. That makes the store
 * simpler and it also means <b>these tests are the only verification this backend has</b>.
 *
 * <p>Each test gets a fresh key prefix, so a failed run leaves its keys behind for inspection without
 * affecting the next.
 */
@EnabledIf(
    value = "serverAvailable",
    disabledReason = "needs Docker, or -Dcontrolplane.etcd=host:port pointing at a real etcd"
)
class EtcdKeyValueStoreTests extends KeyValueStoreContractTests {

    /** Referenced by {@code @EnabledIf}; see {@link EtcdTestServer}. */
    static boolean serverAvailable() {
        return EtcdTestServer.available();
    }

    private static final AtomicInteger PREFIXES = new AtomicInteger();

    private final List<KeyValueStore> opened = new ArrayList<>();
    private String prefix;

    @BeforeEach
    void freshPrefix() {
        prefix = "/cp-test-" + System.nanoTime() + "-" + PREFIXES.incrementAndGet();
    }

    @AfterEach
    void closeStores() {
        opened.forEach(KeyValueStore::close);
        opened.clear();
    }

    @Override
    protected KeyValueStore newStore() {
        try {
            KeyValueStore store = KeyValueStores.open("etcd://" + EtcdTestServer.endpoint() + prefix);
            opened.add(store);
            return store;
        } catch (Exception e) {
            throw new IllegalStateException("could not open an etcd store at " + prefix, e);
        }
    }

    // ------------------------------------------------------------------
    // revisions
    // ------------------------------------------------------------------

    /**
     * etcd's revision is cluster-wide, so it moves when <em>other</em> keys are written. The interface
     * permits that — revisions are opaque and not comparable across keys — and this pins it down so the
     * behaviour is a decision rather than a surprise.
     */
    @Test
    void revisionsAdvanceAcrossUnrelatedKeys() {
        KeyValueStore store = newStore();
        long first = store.put("a", bytes("one"));
        long second = store.put("b", bytes("two"));

        assertTrue(second > first, "the store revision is global; got " + first + " then " + second);
        assertEquals(first, store.get("a").orElseThrow().revision(), "a's own revision did not move");
    }

    /** Writing the same bytes still moves the revision, which the contract relies on. */
    @Test
    void rewritingAnIdenticalValueStillMovesTheRevision() {
        KeyValueStore store = newStore();
        long first = store.put("a", bytes("same"));
        long second = store.put("a", bytes("same"));

        assertTrue(second > first, "got " + first + " then " + second);
    }

    /** A recreated key cannot land on its old revision, so a stale compare-and-set cannot succeed. */
    @Test
    void aRecreatedKeyDoesNotReuseItsOldRevision() {
        KeyValueStore store = newStore();
        long first = store.put("a", bytes("one"));
        store.delete("a");
        long second = store.put("a", bytes("two"));

        assertTrue(second > first, "got " + first + " then " + second);
        assertThrows(RevisionConflictException.class, () -> store.put("a", bytes("three"), first));
    }

    /**
     * A failed compare-and-set reports the actual revision from the transaction's own else-branch read,
     * so the number has to be right rather than merely present.
     */
    @Test
    void aConflictReportsTheRealCurrentRevision() {
        KeyValueStore store = newStore();
        long stale = store.put("a", bytes("one"));
        long current = store.put("a", bytes("two"));

        RevisionConflictException e = assertThrows(RevisionConflictException.class,
            () -> store.put("a", bytes("three"), stale));
        assertEquals(stale, e.expectedRevision());
        assertEquals(current, e.actualRevision());
    }

    // ------------------------------------------------------------------
    // the prefix boundary
    // ------------------------------------------------------------------

    /** The root prefix is what stops one control plane seeing another's keys on a shared cluster. */
    @Test
    void twoPrefixesOnOneClusterAreIndependent() throws Exception {
        KeyValueStore first = newStore();
        prefix = prefix + "-other";
        KeyValueStore second = newStore();

        first.put("shared", bytes("first"));
        second.put("shared", bytes("second"));

        assertEquals(Optional.of("first"), asString(first, "shared"));
        assertEquals(Optional.of("second"), asString(second, "shared"));
        assertEquals(1, first.scanPrefix("").size());
    }

    @Test
    void aPrefixIsRequired() {
        String host = EtcdTestServer.endpoint();

        IOException e = assertThrows(IOException.class, () -> KeyValueStores.open("etcd://" + host));
        assertTrue(e.getMessage().contains("needs a key prefix"), e.getMessage());
    }

    /** Sibling keys sharing a textual prefix must not leak into a narrower scan. */
    @Test
    void scanPrefixStopsAtThePrefix() {
        KeyValueStore store = newStore();
        store.put("ingest/pipeline/a", bytes("A"));
        store.put("ingest/pipelinex", bytes("X"));

        assertEquals(List.of("ingest/pipeline/a"), List.copyOf(store.scanPrefix("ingest/pipeline/").keySet()));
        assertEquals(2, store.scanPrefix("ingest/pipeline").size(), "the shorter prefix matches both");
    }

    // ------------------------------------------------------------------
    // concurrency
    // ------------------------------------------------------------------

    /** One transaction, one winner — the property the revision scheme exists to provide. */
    @Test
    void concurrentCompareAndSetHasExactlyOneWinner() throws Exception {
        KeyValueStore store = newStore();
        long revision = store.put("contended", bytes("start"));

        int writers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        try {
            List<Callable<Boolean>> attempts = new ArrayList<>();
            for (int i = 0; i < writers; i++) {
                String value = "writer-" + i;
                attempts.add(() -> {
                    try {
                        store.put("contended", bytes(value), revision);
                        return true;
                    } catch (RevisionConflictException lost) {
                        return false;
                    }
                });
            }
            long winners = 0;
            for (Future<Boolean> result : pool.invokeAll(attempts)) {
                if (result.get()) {
                    winners++;
                }
            }
            assertEquals(1, winners, "exactly one compare-and-set may succeed at a given revision");
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /** And the same for create-if-absent, which is how indices.create stays safe. */
    @Test
    void concurrentCreateIfAbsentHasExactlyOneWinner() throws Exception {
        KeyValueStore store = newStore();

        int writers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        try {
            List<Callable<Boolean>> attempts = new ArrayList<>();
            for (int i = 0; i < writers; i++) {
                String value = "writer-" + i;
                attempts.add(() -> {
                    try {
                        store.put("created-once", bytes(value), KeyValueStore.ABSENT);
                        return true;
                    } catch (RevisionConflictException lost) {
                        return false;
                    }
                });
            }
            long winners = 0;
            for (Future<Boolean> result : pool.invokeAll(attempts)) {
                if (result.get()) {
                    winners++;
                }
            }
            assertEquals(1, winners, "create-if-absent must admit exactly one creator");
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    // ------------------------------------------------------------------
    // watches
    // ------------------------------------------------------------------

    @Test
    void aNativeWatchFiresForAWriteUnderThePrefix() throws Exception {
        KeyValueStore store = newStore();
        java.util.concurrent.CountDownLatch notified = new java.util.concurrent.CountDownLatch(1);

        java.util.Optional<org.opensearch.controlplane.store.Watch> watch =
            store.watch("indices/", notified::countDown);
        assertTrue(watch.isPresent(), "etcd watches a range natively");

        try (var ignored = watch.get()) {
            store.put("indices/a", bytes("one"));
            assertTrue(notified.await(10, TimeUnit.SECONDS), "the watch must fire");
        }
    }

    /** A range that is currently empty is an ordinary range, so etcd never declines as ZooKeeper can. */
    @Test
    void aNativeWatchOnAnUnwrittenPrefixIsGranted() throws Exception {
        KeyValueStore store = newStore();
        java.util.concurrent.CountDownLatch notified = new java.util.concurrent.CountDownLatch(1);

        java.util.Optional<org.opensearch.controlplane.store.Watch> watch =
            store.watch("never/written/", notified::countDown);
        assertTrue(watch.isPresent());

        try (var ignored = watch.get()) {
            store.put("never/written/first", bytes("1"));
            assertTrue(notified.await(10, TimeUnit.SECONDS));
        }
    }

    /**
     * A write outside the prefix must not notify.
     *
     * <p>Proving that nothing happened needs care, and an earlier version of this test got it wrong: it
     * used a <em>second</em> watch as a fence, on the reasoning that once the fence had fired anything
     * earlier must have been delivered too. That is false. etcd guarantees ordered delivery <b>within</b> a
     * watcher, not <b>across</b> watchers — two watches are two independent streams — so the fence could
     * fire while the counting watch had received nothing yet, and the assertion failed intermittently
     * against a perfectly correct store.
     *
     * <p>This version uses the guarantee that does hold. Both writes go to one watcher, and the
     * out-of-prefix write happens <em>first</em>, so a broken prefix filter would deliver it <em>before</em>
     * the in-prefix one. Waiting for a second event is therefore a positive test for the bug rather than a
     * bounded guess: if the filter leaks, the extra event is already queued behind the one we waited for and
     * arrives immediately. The short timeout is only ever paid when the test passes.
     */
    @Test
    void aWriteOutsideThePrefixDoesNotFire() throws Exception {
        KeyValueStore store = newStore();
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.CountDownLatch first = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch second = new java.util.concurrent.CountDownLatch(2);

        try (var ignored = store.watch("indices/", () -> {
            calls.incrementAndGet();
            first.countDown();
            second.countDown();
        }).orElseThrow()) {
            store.put("aliases/a", bytes("one"));       // must not notify
            store.put("indices/seen", bytes("two"));    // must notify

            assertTrue(first.await(10, TimeUnit.SECONDS), "the in-prefix write must notify");
            assertFalse(second.await(2, TimeUnit.SECONDS),
                "only the in-prefix write may notify, and a leaked event would already be queued");
            assertEquals(1, calls.get());
        }
    }

    /**
     * A closed watch must not notify.
     *
     * <p>Unavoidably a bounded wait: a closed watch should receive nothing ever, and there is no event to
     * wait for that would prove the absence. The live watch on another prefix is still worth keeping — it
     * establishes that etcd processed both writes and delivered to <em>somebody</em>, so a failure here
     * means the closed watch fired rather than that nothing happened at all. Same cross-stream caveat as
     * above applies, which is why the bounded wait follows it rather than replacing it.
     */
    @Test
    void closingAWatchStopsNotifications() throws Exception {
        KeyValueStore store = newStore();
        java.util.concurrent.CountDownLatch closedFired = new java.util.concurrent.CountDownLatch(1);

        var watch = store.watch("indices/", closedFired::countDown).orElseThrow();
        watch.close();

        java.util.concurrent.CountDownLatch live = new java.util.concurrent.CountDownLatch(1);
        try (var stillOpen = store.watch("other/", live::countDown).orElseThrow()) {
            store.put("indices/a", bytes("one"));
            store.put("other/fence", bytes("two"));
            assertTrue(live.await(10, TimeUnit.SECONDS), "etcd delivered to the open watch, so it is live");
        }
        assertFalse(closedFired.await(2, TimeUnit.SECONDS), "a closed watch must not notify");
    }

    /** watchOrPoll must take the native path here, not silently start a polling thread. */
    @Test
    void watchOrPollUsesTheNativeWatch() throws Exception {
        KeyValueStore store = newStore();
        java.util.concurrent.CountDownLatch notified = new java.util.concurrent.CountDownLatch(1);

        try (var watch = org.opensearch.controlplane.store.Watches.watchOrPoll(
            store, "indices/", java.time.Duration.ofHours(1), notified::countDown)) {
            // A one-hour poll interval would never fire in time, so arriving proves the native path.
            store.put("indices/a", bytes("one"));
            assertTrue(notified.await(10, TimeUnit.SECONDS),
                "with an hour-long poll interval, only a native watch could have delivered this");
        }
    }

    @Test
    void theSchemeIsDiscoveredFromTheClasspath() throws Exception {
        assertTrue(KeyValueStores.discover().containsKey("etcd"),
            "the provider jar is on the test classpath: " + KeyValueStores.discover().keySet());
    }
}
