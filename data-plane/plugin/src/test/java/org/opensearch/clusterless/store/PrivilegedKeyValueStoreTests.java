/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.store;

import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;
import org.opensearch.controlplane.store.Watch;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * That wrapping the store in a privileged block did not change what the store does.
 *
 * <p><b>What this cannot test, stated plainly:</b> whether the privileged block actually satisfies the node's
 * security policy. The policy is enforced by a java agent that is installed in a running node and not in a unit
 * test, so {@code AccessController.doPrivileged} here is a plain method call that runs the action — which is
 * also exactly what it is in production, since its only job is to put a recognisable frame on the stack for the
 * agent's walker to stop at. Whether the agent then accepts what it finds is verifiable only by starting a node,
 * which is what {@code ./gradlew run} is for.
 *
 * <p>So this covers the risk the decorator itself introduced, which is a real one and cheap to check: a
 * delegating wrapper that drops a call, loses a return value or swallows an exception would be worse than the
 * problem it was written to solve, and would fail in ways that look like store bugs.
 *
 * <p>The store's own contract suite would be the ideal thing to run here — {@code KeyValueStoreContractTests} in
 * {@code store-api}'s test fixtures — but it is written for JUnit 5 and this build runs JUnit 4, which is the
 * same seam the node-contract checks had to be reshaped around. Worth revisiting if a third consumer needs it.
 */
public class PrivilegedKeyValueStoreTests extends OpenSearchTestCase {

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private final InMemoryKeyValueStore backing = new InMemoryKeyValueStore();
    private final KeyValueStore store = new PrivilegedKeyValueStore(backing);

    public void testWritesAndReadsPassThroughWithTheirRevisions() {
        long revision = store.put("nodes/heartbeat/n1", bytes("one"));

        Optional<KeyValueStore.Versioned> read = store.get("nodes/heartbeat/n1");
        assertTrue(read.isPresent());
        assertEquals("one", new String(read.get().value(), StandardCharsets.UTF_8));
        assertEquals("the revision must be the one the store assigned", revision, read.get().revision());
    }

    public void testAnAbsentKeyStaysDistinguishableFromAnEmptyValue() {
        // The store's contract makes a point of this, and marker keys depend on it. A wrapper that turned an
        // empty value into an absent key would break something two other services rely on.
        store.put("empty", new byte[0]);

        assertTrue(store.get("absent").isEmpty());
        assertTrue(store.get("empty").isPresent());
        assertEquals(0, store.get("empty").get().value().length);
    }

    public void testAConditionalWriteStillConflicts() {
        // The exception is the whole point of the method, and it is unchecked -- so a wrapper that caught and
        // logged it would turn a lost update into silence.
        long revision = store.put("key", bytes("first"));
        store.put("key", bytes("second"));

        expectThrows(RevisionConflictException.class, () -> store.put("key", bytes("third"), revision));
    }

    public void testCreateIfAbsentStillWorks() {
        store.put("key", bytes("only"), KeyValueStore.ABSENT);
        expectThrows(RevisionConflictException.class, () -> store.put("key", bytes("again"), KeyValueStore.ABSENT));
    }

    public void testScanPrefixReturnsTheSameOrderedView() {
        store.put("nodes/heartbeat/b", bytes("2"));
        store.put("nodes/heartbeat/a", bytes("1"));
        store.put("nodes/shards/a", bytes("3"));

        NavigableMap<String, KeyValueStore.Versioned> scanned = store.scanPrefix("nodes/heartbeat/");
        assertEquals(2, scanned.size());
        assertEquals("nodes/heartbeat/a", scanned.firstKey());
        assertEquals("nodes/heartbeat/b", scanned.lastKey());
    }

    public void testDeletesReportWhetherTheKeyExisted() {
        store.put("key", bytes("value"));

        assertTrue(store.delete("key"));
        assertFalse("deleting nothing is not an error, and says so", store.delete("key"));
    }

    public void testAConditionalDeleteStillConflicts() {
        long revision = store.put("key", bytes("first"));
        store.put("key", bytes("second"));

        expectThrows(RevisionConflictException.class, () -> store.delete("key", revision));
    }

    public void testAWatchIsWrappedAndStillCloses() {
        // The callback cannot be privileged -- it runs later, on a thread the backend owns -- but registering
        // and closing the watch can be, and closing has to reach the delegate or the backend leaks it.
        AtomicBoolean closed = new AtomicBoolean();
        KeyValueStore watchable = new PrivilegedKeyValueStore(new StubStore() {
            @Override
            public Optional<Watch> watch(String keyPrefix, Runnable onChange) {
                return Optional.of(() -> closed.set(true));
            }
        });

        Optional<Watch> watch = watchable.watch("nodes/", () -> {});
        assertTrue(watch.isPresent());
        watch.get().close();

        assertTrue("closing the wrapper must close the delegate's watch", closed.get());
    }

    public void testABackendThatCannotWatchStillSaysSo() {
        // The SPI's answer for "this backend has no watch" is an empty Optional, and consumers are told not to
        // branch on it. Mapping over the Optional preserves that; unwrapping it would not.
        assertTrue(store.watch("nodes/", () -> {}).isEmpty());
    }

    public void testCloseReachesTheDelegate() {
        AtomicBoolean closed = new AtomicBoolean();
        new PrivilegedKeyValueStore(new StubStore() {
            @Override
            public void close() {
                closed.set(true);
            }
        }).close();

        assertTrue(closed.get());
    }

    /** InMemoryKeyValueStore is final, so the two tests that need to observe a call need their own stub. */
    private static class StubStore implements KeyValueStore {

        @Override
        public Optional<Versioned> get(String key) {
            return Optional.empty();
        }

        @Override
        public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
            return new java.util.TreeMap<>();
        }

        @Override
        public long put(String key, byte[] value) {
            return 1;
        }

        @Override
        public long put(String key, byte[] value, long expectedRevision) {
            return 1;
        }

        @Override
        public boolean delete(String key) {
            return false;
        }

        @Override
        public boolean delete(String key, long expectedRevision) {
            return false;
        }
    }
}
