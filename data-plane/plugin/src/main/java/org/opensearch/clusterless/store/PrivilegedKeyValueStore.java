/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.store;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.Watch;
import org.opensearch.secure_sm.AccessController;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;

/**
 * Wraps every store call in a privileged block, so that the node's security policy checks this plugin's
 * permissions rather than its callers'.
 *
 * <p>Without this, nothing the store does works, and the reason is not obvious from the failure. OpenSearch 3.x
 * enforces its policy with a java agent that intercepts file and socket calls and walks the stack, requiring
 * <em>every</em> protection domain on it to hold the permission. A store call from this plugin is reached
 * through the thread pool that scheduled the pass, so the frames on the stack include the server's own jars —
 * and the server has no permission to write to whatever directory an operator named in {@code clusterless.store},
 * nor to open a socket to whatever etcd cluster they named. The first symptom was:
 *
 * <pre>
 *   SecurityException: Denied WRITE access to file: .../clusterless-store,
 *   domain: ProtectionDomain (file:.../lib/opensearch-cli-3.9.0-SNAPSHOT.jar)
 * </pre>
 *
 * <p>which names a jar that has nothing to do with this plugin and is simply further up the stack. Granting the
 * permission in {@code plugin-security.policy} cannot fix that: a plugin's policy applies to the plugin's own
 * jars, and the check fails on somebody else's.
 *
 * <p>{@code AccessController.doPrivileged} is what cuts the walk. The agent's stack extractor is written as
 * {@code takeWhile(frame is not doPrivileged)}, so the domains it collects are the ones inside the block: this
 * class and the backend jar beside it, both covered by {@code plugin-security.policy}. The implementation of
 * {@code doPrivileged} is a plain {@code action.run()} — its whole purpose is to put a recognisable frame on the
 * stack.
 *
 * <p><b>Every method, not just the ones that obviously touch the disk.</b> Reads are checked too
 * ({@code Denied OPEN (read) access}), and a backend that talks over the network is checked by the socket
 * interceptor in the same way. So this decorator is what makes any backend usable rather than a workaround for
 * the filesystem one.
 *
 * <p>It sits in this repository rather than in {@code store-api} on purpose. The store has no idea it is running
 * inside a node with a policy — it is also used by two ordinary JVM services that have none — and teaching it
 * about OpenSearch's security model to satisfy one of its three consumers would be the wrong direction for a
 * dependency to point.
 */
public final class PrivilegedKeyValueStore implements KeyValueStore {

    private final KeyValueStore delegate;

    public PrivilegedKeyValueStore(KeyValueStore delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public Optional<Versioned> get(String key) {
        return AccessController.doPrivileged(() -> delegate.get(key));
    }

    @Override
    public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
        return AccessController.doPrivileged(() -> delegate.scanPrefix(keyPrefix));
    }

    @Override
    public long put(String key, byte[] value) {
        return AccessController.doPrivileged(() -> delegate.put(key, value));
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        return AccessController.doPrivileged(() -> delegate.put(key, value, expectedRevision));
    }

    @Override
    public boolean delete(String key) {
        return AccessController.doPrivileged(() -> delegate.delete(key));
    }

    @Override
    public boolean delete(String key, long expectedRevision) {
        return AccessController.doPrivileged(() -> delegate.delete(key, expectedRevision));
    }

    /**
     * The watch itself is registered privileged, and so is closing it.
     *
     * <p>The callback is not, and cannot be: it runs later, on a thread the backend owns, with a stack this
     * class is not on. Whatever it does has to stand on its own — which is another reason the SPI's callback
     * carries no payload and means only "re-read", since re-reading goes back through this class.
     */
    @Override
    public Optional<Watch> watch(String keyPrefix, Runnable onChange) {
        return AccessController.doPrivileged(() -> delegate.watch(keyPrefix, onChange)).map(PrivilegedWatch::new);
    }

    @Override
    public void close() {
        AccessController.doPrivileged(delegate::close);
    }

    private record PrivilegedWatch(Watch delegate) implements Watch {
        @Override
        public void close() {
            AccessController.doPrivileged(delegate::close);
        }
    }
}
