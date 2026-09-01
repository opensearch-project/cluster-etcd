/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import org.opensearch.controlplane.tenant.TenantId;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A view of a {@link KeyValueStore} confined to one tenant's key prefix.
 *
 * <p>This is the whole tenancy mechanism. Handlers are handed one of these and never learn which
 * tenant they are serving, so cross-tenant access is not something a handler can get wrong — there
 * is no API through which to express it. Compare the alternative, where every handler receives a
 * tenant ID and is trusted to incorporate it into every key: that works right up until one of 81
 * handlers forgets, and it fails silently.
 *
 * <p>Keys are translated in both directions. {@link #scanPrefix} strips the tenant prefix from the
 * keys it returns, so callers see exactly the key space they wrote and code like
 * {@code PipelineStore.idFromKey} needs no knowledge of tenancy.
 *
 * <p>Revisions pass through untouched. They are opaque to callers, so a delegate's revision for a
 * prefixed key is a perfectly good revision for the unprefixed one.
 */
public final class TenantScopedKeyValueStore implements KeyValueStore {

    private final KeyValueStore delegate;
    private final String prefix;

    public TenantScopedKeyValueStore(KeyValueStore delegate, TenantId tenant) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.prefix = Objects.requireNonNull(tenant, "tenant").keyPrefix();
    }

    @Override
    public Optional<Versioned> get(String key) {
        return delegate.get(scoped(key));
    }

    @Override
    public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
        NavigableMap<String, Versioned> scoped = delegate.scanPrefix(scoped(keyPrefix));
        NavigableMap<String, Versioned> unscoped = new TreeMap<>();
        scoped.forEach((key, value) -> unscoped.put(unscoped(key), value));
        return unscoped;
    }

    @Override
    public long put(String key, byte[] value) {
        return delegate.put(scoped(key), value);
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        return delegate.put(scoped(key), value, expectedRevision);
    }

    @Override
    public boolean delete(String key) {
        return delegate.delete(scoped(key));
    }

    @Override
    public boolean delete(String key, long expectedRevision) {
        return delegate.delete(scoped(key), expectedRevision);
    }

    private String scoped(String key) {
        return prefix + Objects.requireNonNull(key, "key");
    }

    private String unscoped(String key) {
        // Only keys the delegate matched against our own prefix reach this.
        return key.substring(prefix.length());
    }

    @Override
    public String toString() {
        return "TenantScopedKeyValueStore[" + prefix + "]";
    }
}
