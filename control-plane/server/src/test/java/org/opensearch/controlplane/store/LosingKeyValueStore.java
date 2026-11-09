/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

/**
 * A store that loses its answer to chosen writes, as a backend does when a connection drops or a timeout expires
 * after the request was sent: it throws {@link OutcomeUnknownException}, having applied the write or not.
 *
 * <p>Outcomes are queued per key and used one per write to that key, in order; a key with none queued behaves
 * normally.
 */
public final class LosingKeyValueStore implements KeyValueStore {

    /** What the next write to a key does. */
    public enum Outcome {
        /** Applies the write, then loses the answer. */
        APPLIED,
        /** Loses the write and the answer. */
        NOT_APPLIED,
        /** Loses the answer now and applies the write just before the next write to the key: a slow request. */
        APPLIED_LATE
    }

    private final KeyValueStore delegate;
    private final Map<String, Deque<Outcome>> outcomes = new HashMap<>();
    private final Map<String, Runnable> pendingLate = new HashMap<>();
    private final Map<String, Runnable> duringLoss = new HashMap<>();

    public LosingKeyValueStore(KeyValueStore delegate) {
        this.delegate = delegate;
    }

    public LosingKeyValueStore lose(String key, Outcome... sequence) {
        Deque<Outcome> queue = outcomes.computeIfAbsent(key, k -> new ArrayDeque<>());
        for (Outcome outcome : sequence) {
            queue.add(outcome);
        }
        return this;
    }

    /** Runs {@code write} while the next lost answer for {@code key} is being lost: another writer, in the gap. */
    public LosingKeyValueStore meanwhile(String key, Runnable write) {
        duringLoss.put(key, write);
        return this;
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
        return write(key, () -> delegate.put(key, value));
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        return write(key, () -> delegate.put(key, value, expectedRevision));
    }

    @Override
    public boolean delete(String key) {
        return delegate.delete(key);
    }

    @Override
    public boolean delete(String key, long expectedRevision) {
        return delegate.delete(key, expectedRevision);
    }

    private interface Write {
        long run();
    }

    private long write(String key, Write write) {
        Runnable late = pendingLate.remove(key);
        if (late != null) {
            late.run();
        }
        Deque<Outcome> queue = outcomes.get(key);
        Outcome outcome = queue == null ? null : queue.poll();
        if (outcome == null) {
            return write.run();
        }
        switch (outcome) {
            case APPLIED -> write.run();
            case APPLIED_LATE -> pendingLate.put(key, () -> {
                try {
                    write.run();
                } catch (RevisionConflictException alreadyThere) {
                    // The late copy found the key taken, as a delayed request would.
                }
            });
            case NOT_APPLIED -> { }
        }
        Runnable other = duringLoss.remove(key);
        if (other != null) {
            other.run();
        }
        throw new OutcomeUnknownException("lost the answer to a write of [" + key + "] (" + outcome + ")", null);
    }
}
