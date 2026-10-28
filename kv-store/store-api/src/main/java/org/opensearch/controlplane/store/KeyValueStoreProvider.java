/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.net.URI;

/**
 * Creates a {@link KeyValueStore} for one URI scheme, discovered by {@link java.util.ServiceLoader}.
 *
 * <p>This is the seam that makes a backend a drop-in: a jar carrying an implementation plus a
 * {@code META-INF/services/org.opensearch.controlplane.store.KeyValueStoreProvider} entry is all the
 * control plane needs to speak to a new store. Nothing in {@code server} names etcd or ZooKeeper.
 *
 * <h2>Why {@code ServiceLoader} here and not for handlers</h2>
 *
 * <p>{@code ControlPlaneHandlers} deliberately rejects service loading, and the reasons given there —
 * a greppable list, no startup cost, a typo failing immediately — are good ones. They do not transfer:
 *
 * <ul>
 *   <li>A handler is <b>always</b> present in this repo, so an explicit list can name every one. A
 *       backend lives in a jar that may not be on the classpath at all, and the whole point is that
 *       {@code server} does not have to know it exists.</li>
 *   <li>The failure mode is different. A missed handler registration is silent — the operation answers
 *       501 and nobody notices. A missing store provider is fatal and immediate: the service cannot
 *       start, and {@link KeyValueStores#open} says which schemes it did find.</li>
 *   <li>There is exactly one lookup, at startup, of a handful of providers. The cost that argument was
 *       about does not arise.</li>
 * </ul>
 *
 * <p>Selection stays explicit where it matters: an operator names a scheme in {@code --store}, so which
 * backend is in use is a configuration fact, not the result of whichever jar happened to be found.
 */
public interface KeyValueStoreProvider {

    /**
     * The URI scheme this provider answers to, e.g. {@code etcd}.
     *
     * <p>Lower-case. Two providers claiming one scheme is a configuration error, reported at startup
     * rather than resolved by picking one.
     */
    String scheme();

    /**
     * Opens a store.
     *
     * <p>The URI carries everything the backend needs: authority for endpoints, path for a key-space
     * root, query parameters for anything else. {@code etcd://a:2379,b:2379/controlplane?timeout=5s}.
     *
     * @throws java.io.IOException if the backend cannot be reached or the URI is not usable
     */
    KeyValueStore open(URI uri) throws java.io.IOException;

    /** One line describing what this provider expects, shown in {@code --help} and on a bad URI. */
    String usage();
}
