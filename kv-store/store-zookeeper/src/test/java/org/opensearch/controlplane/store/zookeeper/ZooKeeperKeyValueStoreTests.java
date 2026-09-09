/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.zookeeper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.KeyValueStoreContractTests;
import org.opensearch.controlplane.store.KeyValueStores;

import java.net.URI;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ZooKeeper backend against the shared store contract, plus the behaviour only a real ensemble shows.
 *
 * <h2>Running these</h2>
 *
 * <p>They need a live ZooKeeper and are skipped without one:
 *
 * <pre>{@code
 * ./gradlew :store-zookeeper:test -Dcontrolplane.zookeeper=localhost:2181
 * }</pre>
 *
 * <p>Skipped rather than failed, and rather than started in-process, because the sandbox this was written
 * in permits neither outbound connections nor listening sockets. {@link ZooKeeperKeysTests} carries the
 * part that can be checked without a server, and it carries most of the risk: the path encoding is where
 * a backend silently mangles keys, and every path it produces is validated by ZooKeeper's own
 * {@code PathUtils}.
 *
 * <p>Each test gets a fresh root znode, so a failed run leaves its state behind for inspection without
 * poisoning the next one.
 */
@EnabledIfSystemProperty(
    named = "controlplane.zookeeper",
    matches = ".+",
    disabledReason = "needs -Dcontrolplane.zookeeper=host:port pointing at a real ensemble"
)
class ZooKeeperKeyValueStoreTests extends KeyValueStoreContractTests {

    private static final AtomicInteger ROOTS = new AtomicInteger();

    private final List<KeyValueStore> opened = new ArrayList<>();
    private String root;

    @BeforeEach
    void freshRoot() {
        root = "/cp-test-" + System.nanoTime() + "-" + ROOTS.incrementAndGet();
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
                "zk://" + System.getProperty("controlplane.zookeeper") + root);
            opened.add(store);
            return store;
        } catch (Exception e) {
            throw new IllegalStateException("could not open a ZooKeeper store at " + root, e);
        }
    }

    // ------------------------------------------------------------------
    // behaviour the contract cannot express, because it is backend-specific
    // ------------------------------------------------------------------

    /**
     * A key can be the parent of another key, and deleting it must not take the subtree with it.
     * ZooKeeper refuses to delete a znode with children, so this is the case the framing byte exists for.
     */
    @Test
    void deletingAKeyThatIsAlsoAParentKeepsItsDescendants() {
        KeyValueStore store = newStore();
        store.put("a", bytes("parent"));
        store.put("a/b", bytes("child"));

        assertTrue(store.delete("a"));
        assertEquals(Optional.empty(), store.get("a"), "the key is gone");
        assertEquals(Optional.of("child"), asString(store, "a/b"), "the subtree survived");
        assertEquals(List.of("a/b"), List.copyOf(store.scanPrefix("a").keySet()),
            "a scan must not resurrect the deleted parent");
    }

    /** And the name is then free again, at a new revision. */
    @Test
    void aDeletedParentKeyCanBeRecreated() {
        KeyValueStore store = newStore();
        store.put("a", bytes("first"));
        store.put("a/b", bytes("child"));
        store.delete("a");

        long revision = store.put("a", bytes("second"), KeyValueStore.ABSENT);
        assertEquals(Optional.of("second"), asString(store, "a"));
        assertEquals(revision, store.get("a").orElseThrow().revision());
    }

    /**
     * Intermediate znodes are structure, not keys, so they must never appear in a scan. Without the
     * framing byte {@code tenant/acme} would show up as an empty-valued key of its own.
     */
    @Test
    void intermediateNodesAreNotKeys() {
        KeyValueStore store = newStore();
        store.put("tenant/acme/indices/one", bytes("1"));

        assertEquals(Optional.empty(), store.get("tenant"));
        assertEquals(Optional.empty(), store.get("tenant/acme"));
        assertEquals(Optional.empty(), store.get("tenant/acme/indices"));
        assertEquals(List.of("tenant/acme/indices/one"), List.copyOf(store.scanPrefix("").keySet()));
    }

    /**
     * The revision is the modification zxid rather than the znode version, which is what makes this safe:
     * a delete-and-recreate has to produce a revision the old one cannot be confused with, or a stale
     * compare-and-set would succeed against a different value.
     */
    @Test
    void aRecreatedKeyDoesNotReuseItsOldRevision() {
        KeyValueStore store = newStore();
        long first = store.put("a", bytes("one"));
        store.delete("a");
        long second = store.put("a", bytes("two"));

        assertTrue(second > first,
            "zxids are monotonic across the ensemble; got " + first + " then " + second);
    }

    /** Two writers, one winner — the property the whole revision scheme exists to provide. */
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
                    } catch (org.opensearch.controlplane.store.RevisionConflictException lost) {
                        return false;
                    }
                });
            }
            List<Future<Boolean>> results = pool.invokeAll(attempts);
            long winners = 0;
            for (Future<Boolean> result : results) {
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

    /** The root path is mandatory, so a control plane cannot scatter keys across an ensemble root. */
    @Test
    void aConnectionStringWithoutARootIsRejected() {
        String host = System.getProperty("controlplane.zookeeper");

        Exception e = org.junit.jupiter.api.Assertions.assertThrows(
            java.io.IOException.class, () -> KeyValueStores.open("zk://" + host));
        assertTrue(e.getMessage().contains("needs a root znode path"), e.getMessage());
    }

    @Test
    void theSchemeIsDiscoveredFromTheClasspath() throws Exception {
        assertTrue(KeyValueStores.discover().containsKey("zk"),
            "the provider jar is on the test classpath: " + KeyValueStores.discover().keySet());
        assertEquals("zk", URI.create("zk://h:2181/r").getScheme());
    }
}
