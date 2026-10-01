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
 * entitled to rely on regardless of backend, so <b>every</b> implementation extends this — it is
 * published as a test fixture of {@code :store-api} for exactly that reason, and a backend module gets
 * it with {@code testImplementation(testFixtures(project(":store-api")))}.
 *
 * <p>Three assertions here constrain backend design more than they look, and are worth reading before
 * writing one:
 *
 * <ul>
 *   <li>{@code emptyValuesRoundTrip} — an empty value must be distinguishable from an absent key,
 *       because marker keys rely on it. A backend cannot use "no data" to mean "not a key".</li>
 *   <li>{@code dotDotIsAnOrdinaryKeySegment} — {@code ..} is an ordinary segment and must never
 *       resolve. Any backend that maps keys onto a path-like namespace has to encode it; ZooKeeper
 *       rejects {@code .} and {@code ..} as znode names outright.</li>
 *   <li>{@code aKeyAndValueAtTheLimitsRoundTrip} — every backend must take the longest key and the largest
 *       value at once, after its own escaping and framing. A backend whose native limits are smaller has to
 *       work around them (hashing long keys, moving large values elsewhere), not lower the limits.</li>
 * </ul>
 */
public abstract class KeyValueStoreContractTests {

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
        assertTrue(first > 0 && first <= KeyValueStore.MAX_REVISION, "revisions are positive and JSON-safe: " + first);
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
        // Best effort: the backend may not know without another request.
        if (e.actualRevision() != RevisionConflictException.UNKNOWN) {
            assertEquals(store.get("a").orElseThrow().revision(), e.actualRevision());
        }
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

    @Test
    void aRecreatedKeyNeverReusesARevision() {
        // The guarantee readers actually depend on, and the one this suite did not check. Every reader in these
        // services uses revision equality as a change signal -- parsed at R, sees R again, skips the parse -- so a
        // key that comes back after deletion at a revision it has already been seen at, carrying different
        // content, reads as unchanged for ever, with nothing anywhere to indicate it.
        //
        // That was a real incident in miniature: a supervisor's desired state froze on a deleted index's uuid, the
        // data node correctly declined to build an index that no longer existed, and the only way out was
        // restarting the supervisor to clear its cache. The filesystem backend derived the next revision from the
        // key's own file, so deleting the file restarted the count at 1.
        KeyValueStore store = newStore();

        long first = store.put("a", bytes("one"));
        assertTrue(store.delete("a"));
        long afterRecreate = store.put("a", bytes("two"));

        assertNotEquals(first, afterRecreate, "a recreated key must not come back at a revision it has already had");
        assertTrue(afterRecreate > first, "and the revision must still move forward");
        assertEquals(Optional.of("two"), asString(store, "a"));
    }

    @Test
    void writingIdenticalBytesStillChangesTheRevision() {
        // Readers treat an unchanged revision as an unchanged key, and liveness is judged by how long a heartbeat's
        // revision has gone unchanged, so a rewrite of the same bytes has to register. Rules out content hashes.
        KeyValueStore store = newStore();
        long first = store.put("a", bytes("same"));
        long second = store.put("a", bytes("same"));
        long third = store.put("a", bytes("same"), second);

        assertNotEquals(first, second);
        assertNotEquals(second, third);
        assertEquals(third, store.get("a").orElseThrow().revision());
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

    /** More keys than a backend that pages its scans would put in one page. */
    @Test
    void scanPrefixReturnsEveryKeyOfALargePrefix() {
        KeyValueStore store = newStore();
        int count = 600;
        for (int i = 0; i < count; i++) {
            store.put(String.format(java.util.Locale.ROOT, "many/%04d", i), bytes("v" + i));
        }
        store.put("manyx", bytes("outside"));

        var scanned = store.scanPrefix("many/");
        assertEquals(count, scanned.size());
        assertEquals("many/0000", scanned.firstKey());
        assertEquals("many/0599", scanned.lastKey());
        assertEquals("v300", new String(scanned.get("many/0300").value(), StandardCharsets.UTF_8));
    }

    /** More bytes than gRPC's default 4 MiB message, which an unpaged range would have to fit in. */
    @Test
    void scanPrefixReturnsMoreThanFourMebibytes() {
        KeyValueStore store = newStore();
        int count = 6;
        for (int i = 0; i < count; i++) {
            store.put("big/" + i, filled(KeyValueStore.MAX_VALUE_BYTES, i));
        }

        var scanned = store.scanPrefix("big/");
        assertEquals(count, scanned.size());
        for (int i = 0; i < count; i++) {
            assertArrayEqualsLocal(filled(KeyValueStore.MAX_VALUE_BYTES, i), scanned.get("big/" + i).value());
        }
    }

    // ------------------------------------------------------------------
    // limits
    // ------------------------------------------------------------------

    /**
     * The worst case together: the longest key, of characters every escaping scheme expands, and the largest value.
     * ZooKeeper bounds the whole request, so this is the case its limit was set by.
     */
    @Test
    void aKeyAndValueAtTheLimitsRoundTrip() {
        KeyValueStore store = newStore();
        String key = "limit/" + "é".repeat((KeyValueStore.MAX_KEY_BYTES - "limit/".length()) / 2);
        assertEquals(KeyValueStore.MAX_KEY_BYTES, key.getBytes(StandardCharsets.UTF_8).length);
        byte[] value = filled(KeyValueStore.MAX_VALUE_BYTES, 7);

        long revision = store.put(key, value, KeyValueStore.ABSENT);
        assertArrayEqualsLocal(value, store.get(key).orElseThrow().value());
        assertEquals(List.of(key), List.copyOf(store.scanPrefix("limit/").keySet()));
        assertTrue(store.delete(key, revision));
    }

    @Test
    void aValueOverTheLimitIsRejectedAndNotApplied() {
        KeyValueStore store = newStore();
        long revision = store.put("a", bytes("small"));
        byte[] tooBig = new byte[KeyValueStore.MAX_VALUE_BYTES + 1];

        assertThrows(LimitExceededException.class, () -> store.put("a", tooBig));
        assertThrows(LimitExceededException.class, () -> store.put("a", tooBig, revision));
        assertThrows(LimitExceededException.class, () -> store.put("b", tooBig, KeyValueStore.ABSENT));
        assertEquals(revision, store.get("a").orElseThrow().revision());
        assertEquals(Optional.empty(), store.get("b"));
    }

    @Test
    void aKeyOverTheLimitIsRejected() {
        KeyValueStore store = newStore();
        String tooLong = "k".repeat(KeyValueStore.MAX_KEY_BYTES + 1);
        // Measured in UTF-8 bytes, not characters.
        String tooLongInBytes = "é".repeat(KeyValueStore.MAX_KEY_BYTES / 2 + 1);

        assertThrows(LimitExceededException.class, () -> store.put(tooLong, bytes("v")));
        assertThrows(LimitExceededException.class, () -> store.put(tooLong, bytes("v"), KeyValueStore.ABSENT));
        assertThrows(LimitExceededException.class, () -> store.put(tooLongInBytes, bytes("v")));
        assertTrue(store.scanPrefix("").isEmpty());
    }

    /** A key that cannot be written cannot exist, so reading one is an ordinary miss rather than an error. */
    @Test
    void readingAKeyOverTheLimitFindsNothing() {
        KeyValueStore store = newStore();
        String tooLong = "k".repeat(KeyValueStore.MAX_KEY_BYTES + 1);

        assertEquals(Optional.empty(), store.get(tooLong));
        assertFalse(store.delete(tooLong));
        assertFalse(store.delete(tooLong, KeyValueStore.ABSENT));
        assertTrue(store.scanPrefix(tooLong).isEmpty());
    }

    /** Bytes that differ per {@code seed}, so a scan handing back the wrong value cannot pass by accident. */
    private static byte[] filled(int length, int seed) {
        byte[] value = new byte[length];
        new java.util.Random(seed).nextBytes(value);
        return value;
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
