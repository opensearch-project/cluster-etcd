/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

/**
 * A store that runs a write of its own between a handler's read of a key and whatever comes next, to
 * test the read-modify-write paths deterministically: the handler reads, the interleaved write
 * moves the key's revision, and the handler's compare-and-set then loses.
 */
public final class InterleavingKeyValueStore implements KeyValueStore {

    private final KeyValueStore delegate;
    private final Map<String, Runnable> afterNextGet = new HashMap<>();

    public InterleavingKeyValueStore(KeyValueStore delegate) {
        this.delegate = delegate;
    }

    /** Runs {@code write} once, straight after the next {@code get} of {@code key} returns its value. */
    public void afterNextGet(String key, Runnable write) {
        afterNextGet.put(key, write);
    }

    @Override
    public Optional<Versioned> get(String key) {
        Optional<Versioned> value = delegate.get(key);
        Runnable write = afterNextGet.remove(key);
        if (write != null) {
            write.run();
        }
        return value;
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
