/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs a callback the first time anything is written through it, then gets out of the way.
 *
 * <p>Exists so that the control plane can notice a tenant coming into existence. It is a separate decorator
 * rather than a field on {@link TenantScopedKeyValueStore} on purpose: that class documents itself as the
 * whole tenancy mechanism, a view through which cross-tenant access "is not something a handler can get
 * wrong — there is no API through which to express it". A hook on it that writes <em>outside</em> the tenant
 * prefix would quietly weaken exactly that claim. Composing the two instead leaves the tenant view pure and
 * puts the registration where it can be read, in {@code OperationContextFactory}.
 *
 * <h2>First write, not first request</h2>
 *
 * <p>Reads deliberately do not trigger the callback. An unrestricted tenant resolver accepts any well-formed
 * id, so anything driven by requests rather than by writes can be grown without limit by an unauthenticated
 * caller inventing tenant names. Keying on writes bounds it to tenants that actually caused state to exist,
 * which is also the more honest definition of a tenant existing.
 *
 * <h2>The callback must not throw</h2>
 *
 * <p>Whatever it does is incidental to the operation the caller asked for, so a failure there must not fail
 * the write. That is the callback's responsibility rather than this class's — see
 * {@code TenantRegistry.ensureRegistered}, which swallows and logs. This class does not catch, so a genuine
 * programming error in a callback still surfaces rather than being hidden by a decorator nobody thinks about.
 *
 * <p>The callback runs <b>at most once per instance</b>, and an instance is per request, so the cost of
 * making it cheap across requests belongs to the callback too.
 */
public final class RegisteringKeyValueStore implements KeyValueStore {

    private final KeyValueStore delegate;
    private final Runnable onFirstWrite;
    private final AtomicBoolean fired = new AtomicBoolean();

    public RegisteringKeyValueStore(KeyValueStore delegate, Runnable onFirstWrite) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.onFirstWrite = Objects.requireNonNull(onFirstWrite, "onFirstWrite");
    }

    private void firstWrite() {
        if (fired.compareAndSet(false, true)) {
            onFirstWrite.run();
        }
    }

    // ------------------------------------------------------------------
    // mutating: announce, then delegate
    // ------------------------------------------------------------------

    @Override
    public long put(String key, byte[] value) {
        firstWrite();
        return delegate.put(key, value);
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        // Before the write rather than after: a conditional write that loses its race still means a caller
        // tried to create state for this tenant, and the registry is a discovery hint rather than a ledger.
        firstWrite();
        return delegate.put(key, value, expectedRevision);
    }

    @Override
    public boolean delete(String key) {
        firstWrite();
        return delegate.delete(key);
    }

    @Override
    public boolean delete(String key, long expectedRevision) {
        firstWrite();
        return delegate.delete(key, expectedRevision);
    }

    // ------------------------------------------------------------------
    // everything else: straight through
    // ------------------------------------------------------------------

    @Override
    public Optional<Versioned> get(String key) {
        return delegate.get(key);
    }

    @Override
    public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
        return delegate.scanPrefix(keyPrefix);
    }

    @Override
    public Optional<Watch> watch(String keyPrefix, Runnable onChange) {
        return delegate.watch(keyPrefix, onChange);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public String toString() {
        return "RegisteringKeyValueStore[" + delegate + "]";
    }
}
