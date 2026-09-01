/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@link KeyValueStore} contract, run against every implementation.
 *
 * <p>Written entirely against the interface. A behaviour asserted here is one the control plane is
 * entitled to rely on regardless of backend, so etcd and DynamoDB implementations should extend this
 * too.
 */
abstract class KeyValueStoreContractTests {

    /** A fresh, empty store. */
    protected abstract KeyValueStore newStore();

    protected static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    protected static Optional<String> asString(KeyValueStore store, String key) {
        return store.get(key).map(versioned -> new String(versioned.value(), StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------
    // basics
    // ------------------------------------------------------------------

    @Test
    void getOnAnAbsentKeyIsEmpty() {
        assertEquals(Optional.empty(), newStore().get("nothing"));
    }

    @Test
    void deleteOnAnAbsentKeyIsFalse() {
        assertFalse(newStore().delete("nothing"));
    }

    @Test
    void putGetDelete() {
        KeyValueStore store = newStore();

        store.put("a", bytes("one"));
        assertEquals(Optional.of("one"), asString(store, "a"));

        store.put("a", bytes("two"));
        assertEquals(Optional.of("two"), asString(store, "a"));

        assertTrue(store.delete("a"));
        assertEquals(Optional.empty(), store.get("a"));
    }

    @Test
    void emptyValuesRoundTrip() {
        KeyValueStore store = newStore();
        store.put("a", new byte[0]);

        // Distinguishable from absent, which matters for marker keys.
        assertTrue(store.get("a").isPresent());
        assertEquals(0, store.get("a").orElseThrow().value().length);
    }

    @Test
    void binaryValuesRoundTripUnchanged() {
        KeyValueStore store = newStore();
        byte[] value = new byte[256];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) i;
        }
        store.put("a", value);

        assertArrayEqualsLocal(value, store.get("a").orElseThrow().value());
    }

    private static void assertArrayEqualsLocal(byte[] expected, byte[] actual) {
        assertEquals(expected.length, actual.length, "length");
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i], "byte " + i);
        }
    }

    // ------------------------------------------------------------------
    // revisions
    // ------------------------------------------------------------------

    @Test
    void revisionsAreNeverAbsentForAPresentKeyAndChangeOnEveryWrite() {
        KeyValueStore store = newStore();

        long first = store.put("a", bytes("one"));
        assertNotEquals(KeyValueStore.ABSENT, first, "a written key must not report the ABSENT revision");
        assertEquals(first, store.get("a").orElseThrow().revision());

        long second = store.put("a", bytes("two"));
        assertNotEquals(first, second);
        assertEquals(second, store.get("a").orElseThrow().revision());
    }

    @Test
    void conditionalPutWithAbsentRequiresTheKeyToBeNew() {
        KeyValueStore store = newStore();

        // The shape indices.create needs: create-if-absent.
        store.put("a", bytes("one"), KeyValueStore.ABSENT);
        assertEquals(Optional.of("one"), asString(store, "a"));

        RevisionConflictException e = assertThrows(
            RevisionConflictException.class,
            () -> store.put("a", bytes("two"), KeyValueStore.ABSENT)
        );
        assertEquals("a", e.key());
        assertEquals(KeyValueStore.ABSENT, e.expectedRevision());
        assertEquals(store.get("a").orElseThrow().revision(), e.actualRevision());
        assertEquals(Optional.of("one"), asString(store, "a"), "a failed write must not apply");
    }

    @Test
    void conditionalPutSucceedsAtTheCurrentRevision() {
        KeyValueStore store = newStore();
        long revision = store.put("a", bytes("one"));

        long updated = store.put("a", bytes("two"), revision);
        assertEquals(Optional.of("two"), asString(store, "a"));
        assertNotEquals(revision, updated);
    }

    @Test
    void conditionalPutFailsAtAStaleRevision() {
        KeyValueStore store = newStore();
        long stale = store.put("a", bytes("one"));
        long current = store.put("a", bytes("two"));

        // The read-modify-write cycle indices.update_aliases will need.
        assertThrows(RevisionConflictException.class, () -> store.put("a", bytes("three"), stale));
        assertEquals(current, store.get("a").orElseThrow().revision());
        assertEquals(Optional.of("two"), asString(store, "a"));
    }

    @Test
    void conditionalDeleteChecksTheRevision() {
        KeyValueStore store = newStore();
        long revision = store.put("a", bytes("one"));

        assertThrows(RevisionConflictException.class, () -> store.delete("a", revision + 999));
        assertTrue(store.get("a").isPresent(), "a failed delete must not apply");

        assertTrue(store.delete("a", revision));
        assertEquals(Optional.empty(), store.get("a"));
    }

    @Test
    void conditionalDeleteOfAnAbsentKeyMatchesAbsentRevision() {
        KeyValueStore store = newStore();

        assertFalse(store.delete("nothing", KeyValueStore.ABSENT));
        assertThrows(RevisionConflictException.class, () -> store.delete("nothing", 1L));
    }

    // ------------------------------------------------------------------
    // prefix scans
    // ------------------------------------------------------------------

    @Test
    void scanPrefixIsOrderedAndBounded() {
        KeyValueStore store = newStore();
        store.put("ingest/pipeline/b", bytes("B"));
        store.put("ingest/pipeline/a", bytes("A"));
        store.put("ingest/pipelinex", bytes("X"));
        store.put("index/config/a", bytes("I"));

        assertEquals(
            List.of("ingest/pipeline/a", "ingest/pipeline/b"),
            List.copyOf(store.scanPrefix("ingest/pipeline/").keySet())
        );
        assertEquals(4, store.scanPrefix("").size());
    }

    @Test
    void scanPrefixReturnsValuesAndRevisions() {
        KeyValueStore store = newStore();
        long revision = store.put("p/a", bytes("A"));

        KeyValueStore.Versioned entry = store.scanPrefix("p/").get("p/a");
        assertEquals("A", new String(entry.value(), StandardCharsets.UTF_8));
        assertEquals(revision, entry.revision());
    }

    @Test
    void scanPrefixOnAnEmptyStoreIsEmpty() {
        assertTrue(newStore().scanPrefix("anything").isEmpty());
    }

    @Test
    void scanPrefixHandlesKeysNeedingEscaping() {
        KeyValueStore store = newStore();
        // Names OpenSearch permits that a filesystem-backed store must not mishandle.
        List<String> keys = List.of(
            "t/acme/idx/a+b",
            "t/acme/idx/a.b",
            "t/acme/idx/a%2Fb",
            "t/acme/idx/..",
            "t/acme/idx/with space",
            "t/acme/idx/ünïcodé"
        );
        for (String key : keys) {
            store.put(key, bytes(key));
        }

        assertEquals(keys.size(), store.scanPrefix("t/acme/idx/").size());
        for (String key : keys) {
            assertEquals(Optional.of(key), asString(store, key), key);
        }
    }

    /** {@code ..} must be an ordinary key, not a traversal. */
    @Test
    void dotDotIsAnOrdinaryKeySegment() {
        KeyValueStore store = newStore();
        store.put("tenant/acme/x", bytes("acme"));
        store.put("tenant/acme/../globex/x", bytes("escaped"));

        assertEquals(Optional.of("acme"), asString(store, "tenant/acme/x"));
        assertEquals(Optional.of("escaped"), asString(store, "tenant/acme/../globex/x"));
        // The literal key was stored; it did not resolve to tenant/globex/x.
        assertEquals(Optional.empty(), store.get("tenant/globex/x"));
        assertEquals(1, store.scanPrefix("tenant/acme/x").size());
    }

    // ------------------------------------------------------------------
    // payload isolation
    // ------------------------------------------------------------------

    @Test
    void mutatingTheCallersArrayDoesNotAffectTheStore() {
        KeyValueStore store = newStore();
        byte[] written = bytes("one");
        store.put("a", written);

        written[0] = 'X';
        assertEquals(Optional.of("one"), asString(store, "a"));
    }
}
