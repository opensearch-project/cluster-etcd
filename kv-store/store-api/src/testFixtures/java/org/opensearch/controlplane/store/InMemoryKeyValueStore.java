/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A {@link KeyValueStore} backed by a sorted map, for tests and local development.
 *
 * <p>Deliberately on the test source set: it is not durable, so nothing on the main source set
 * should be able to reach for it by accident. Making the server actually serve requests needs a
 * persistent implementation — a local filesystem store, then etcd or DynamoDB.
 *
 * <p>Fully synchronised rather than backed by a concurrent map. Conditional writes have to compare
 * and swap atomically, which a {@code ConcurrentSkipListMap} does not give for free, and this class
 * is never on a hot path.
 */
public final class InMemoryKeyValueStore implements KeyValueStore {

    private final NavigableMap<String, Versioned> entries = new TreeMap<>();
    private long nextRevision = 1L;

    @Override
    public synchronized Optional<Versioned> get(String key) {
        return Optional.ofNullable(entries.get(key)).map(InMemoryKeyValueStore::copyOf);
    }

    @Override
    public synchronized NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
        NavigableMap<String, Versioned> matched = new TreeMap<>();
        // Half-open range over the prefix rather than filtering every key.
        for (Map.Entry<String, Versioned> entry : entries.tailMap(keyPrefix, true).entrySet()) {
            if (entry.getKey().startsWith(keyPrefix) == false) {
                break;
            }
            matched.put(entry.getKey(), copyOf(entry.getValue()));
        }
        return matched;
    }

    @Override
    public synchronized long put(String key, byte[] value) {
        StoreLimits.checkWrite(key, value);
        long revision = nextRevision++;
        entries.put(key, new Versioned(value.clone(), revision));
        return revision;
    }

    @Override
    public synchronized long put(String key, byte[] value, long expectedRevision) {
        StoreLimits.checkWrite(key, value);
        requireRevision(key, expectedRevision);
        return put(key, value);
    }

    @Override
    public synchronized boolean delete(String key) {
        return entries.remove(key) != null;
    }

    @Override
    public synchronized boolean delete(String key, long expectedRevision) {
        requireRevision(key, expectedRevision);
        return delete(key);
    }

    /** Test helper: current revision of a key, or {@link KeyValueStore#ABSENT}. */
    public synchronized long revisionOf(String key) {
        Versioned existing = entries.get(key);
        return existing == null ? ABSENT : existing.revision();
    }

    /** Test helper: the stored payload as a UTF-8 string. */
    public synchronized Optional<String> getAsString(String key) {
        return get(key).map(versioned -> new String(versioned.value(), StandardCharsets.UTF_8));
    }

    public synchronized int size() {
        return entries.size();
    }

    private void requireRevision(String key, long expectedRevision) {
        long actual = revisionOf(key);
        if (actual != expectedRevision) {
            throw new RevisionConflictException(key, expectedRevision, actual);
        }
    }

    /**
     * Copy on the way out as well as in. The interface says callers must not mutate payloads, but a
     * test double that cannot be corrupted by a buggy handler is worth the copies.
     */
    private static Versioned copyOf(Versioned versioned) {
        return new Versioned(versioned.value().clone(), versioned.revision());
    }

    @Override
    public synchronized String toString() {
        return "InMemoryKeyValueStore" + entries.keySet();
    }
}
