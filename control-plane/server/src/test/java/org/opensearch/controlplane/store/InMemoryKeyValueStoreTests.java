/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryKeyValueStoreTests {

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void putGetDelete() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();

        assertTrue(store.get("a").isEmpty());
        assertFalse(store.delete("a"));

        store.put("a", bytes("one"));
        assertEquals(Optional.of("one"), store.getAsString("a"));

        store.put("a", bytes("two"));
        assertEquals(Optional.of("two"), store.getAsString("a"));

        assertTrue(store.delete("a"));
        assertTrue(store.get("a").isEmpty());
    }

    @Test
    void revisionsAdvanceOnEveryWrite() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();

        long first = store.put("a", bytes("one"));
        long second = store.put("a", bytes("two"));
        assertNotEquals(first, second);
        assertEquals(second, store.revisionOf("a"));

        assertEquals(KeyValueStore.ABSENT, store.revisionOf("missing"));
    }

    @Test
    void conditionalPutRequiresAbsenceWhenExpectingAbsent() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();

        // This is the shape indices.create needs: create-if-absent.
        store.put("a", bytes("one"), KeyValueStore.ABSENT);
        assertEquals(Optional.of("one"), store.getAsString("a"));

        RevisionConflictException e = assertThrows(
            RevisionConflictException.class,
            () -> store.put("a", bytes("two"), KeyValueStore.ABSENT)
        );
        assertEquals("a", e.key());
        assertEquals(KeyValueStore.ABSENT, e.expectedRevision());
        assertEquals(store.revisionOf("a"), e.actualRevision());
        assertEquals(Optional.of("one"), store.getAsString("a"), "failed write must not have applied");
    }

    @Test
    void conditionalPutSucceedsAtCurrentRevisionAndFailsAtStaleOne() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();

        long revision = store.put("a", bytes("one"));
        long updated = store.put("a", bytes("two"), revision);
        assertEquals(Optional.of("two"), store.getAsString("a"));

        // The read-modify-write cycle that indices.update_aliases will need.
        assertThrows(RevisionConflictException.class, () -> store.put("a", bytes("three"), revision));
        assertEquals(updated, store.revisionOf("a"));
    }

    @Test
    void conditionalDeleteChecksRevision() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();
        long revision = store.put("a", bytes("one"));

        assertThrows(RevisionConflictException.class, () -> store.delete("a", revision + 999));
        assertTrue(store.get("a").isPresent(), "failed delete must not have applied");

        assertTrue(store.delete("a", revision));
        assertTrue(store.get("a").isEmpty());
    }

    @Test
    void scanPrefixIsOrderedAndBounded() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();
        store.put("ingest/pipeline/b", bytes("B"));
        store.put("ingest/pipeline/a", bytes("A"));
        store.put("ingest/pipelinex", bytes("X"));
        store.put("index/config/a", bytes("I"));

        assertEquals(
            java.util.List.of("ingest/pipeline/a", "ingest/pipeline/b"),
            java.util.List.copyOf(store.scanPrefix("ingest/pipeline/").keySet())
        );
        assertEquals(4, store.size());
        assertEquals(4, store.scanPrefix("").size());
    }

    @Test
    void payloadsAreCopiedInAndOut() {
        InMemoryKeyValueStore store = new InMemoryKeyValueStore();
        byte[] written = bytes("one");
        store.put("a", written);

        written[0] = 'X';
        assertEquals(Optional.of("one"), store.getAsString("a"), "mutating the caller's array must not affect the store");

        byte[] read = store.get("a").orElseThrow().value();
        read[0] = 'Y';
        assertEquals(Optional.of("one"), store.getAsString("a"), "mutating a read array must not affect the store");
    }
}
