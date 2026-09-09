/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.etcd;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The etcd backend against the shared store contract, plus what only a real cluster shows.
 *
 * <pre>{@code
 * ./gradlew :store-etcd:test -Dcontrolplane.etcd=localhost:2379
 * }</pre>
 *
 * <p>Skipped without one. Unlike the ZooKeeper backend there is no companion unit test carrying the
 * risk, because etcd needs no key encoding to test — its keyspace is flat bytes. That makes the store
 * simpler and it also means <b>these tests are the only verification this backend has</b>.
 *
 * <p>Each test gets a fresh key prefix, so a failed run leaves its keys behind for inspection without
 * affecting the next.
 */
@EnabledIfSystemProperty(
    named = "controlplane.etcd",
    matches = ".+",
    disabledReason = "needs -Dcontrolplane.etcd=host:port pointing at a real etcd"
)
class EtcdKeyValueStoreTests extends KeyValueStoreContractTests {

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
            KeyValueStore store = KeyValueStores.open(
                "etcd://" + System.getProperty("controlplane.etcd") + prefix);
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
        String host = System.getProperty("controlplane.etcd");

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

    @Test
    void theSchemeIsDiscoveredFromTheClasspath() throws Exception {
        assertTrue(KeyValueStores.discover().containsKey("etcd"),
            "the provider jar is on the test classpath: " + KeyValueStores.discover().keySet());
    }
}
