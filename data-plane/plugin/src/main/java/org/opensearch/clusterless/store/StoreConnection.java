/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.KeyValueStoreProvider;
import org.opensearch.controlplane.store.KeyValueStores;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;

/**
 * Turns the {@code clusterless.store} URI into a {@link KeyValueStore}, and says something useful when it
 * cannot.
 *
 * <p>Everything above this holds a {@code KeyValueStore} and knows nothing about which one — D6.2, which
 * is a property that is much easier to keep than to recover. The whole of this plugin's knowledge of any
 * particular backend is the scheme string an operator wrote in a setting.
 *
 * <p>The reason this class exists at all rather than a bare call to {@link KeyValueStores#open} is the
 * diagnostic. A backend is a jar an operator has to install, so "no provider for scheme [etcd]" has two
 * completely different causes — a typo, or a missing jar — and an operator staring at a node that will not
 * start needs to be told which. The list of schemes that <em>were</em> found distinguishes them, and it is
 * the same list {@code :plugin:schemes} prints at build time.
 */
public final class StoreConnection {

    private static final Logger logger = LogManager.getLogger(StoreConnection.class);

    private StoreConnection() {}

    /**
     * @param uri a store URI such as {@code file:///var/lib/clusterless} or
     *            {@code etcd://etcd-0:2379,etcd-1:2379/opensearch}
     * @throws IllegalArgumentException if the URI is unusable or no installed backend claims its scheme.
     *     Unchecked, and fatal at node start: a data plane node with no store has nowhere to read desired
     *     state and nowhere to publish what it holds, so there is nothing useful for it to do.
     */
    public static KeyValueStore open(String uri) {
        String scheme = schemeOf(uri);
        try {
            // Opening is privileged for the same reason every later call is: the node's policy checks every
            // protection domain on the stack, and the frames above this one belong to the server. Even creating
            // the store's directory fails otherwise, from a jar with nothing to do with this plugin.
            KeyValueStore store = new PrivilegedKeyValueStore(
                AccessController.doPrivilegedChecked(() -> KeyValueStores.open(uri))
            );
            logger.info("clusterless: opened store [{}] using the [{}] backend", uri, scheme);
            return store;
        } catch (IOException e) {
            throw new IllegalArgumentException(
                "could not open the clusterless store [" + uri + "]: " + e.getMessage() + "\n" + describeInstalled(),
                e
            );
        }
    }

    /**
     * What is installed, in a form an operator can act on.
     *
     * <p>Deliberately says that an empty list is not the same as a broken installation: this plugin bundles
     * only the SPI and the filesystem backend that comes with it, so a deployment against etcd or ZooKeeper
     * needs a jar dropped beside it. Without saying so, a short list reads as though those backends do not
     * exist rather than as though they were not installed.
     */
    public static String describeInstalled() {
        Map<String, KeyValueStoreProvider> providers;
        try {
            providers = AccessController.doPrivilegedChecked(KeyValueStores::discover);
        } catch (IOException e) {
            return "the installed store backends could not be listed: " + e.getMessage();
        }
        StringBuilder description = new StringBuilder();
        if (providers.isEmpty()) {
            description.append("No store backends are installed at all, which should be impossible: store-api bundles one.");
        } else {
            description.append("Installed store backends:");
            providers.forEach((scheme, provider) -> description.append("\n  ").append(provider.usage()));
        }
        description.append("\n\nA backend is a jar on this plugin's classpath, found by ServiceLoader. To add one, put it")
            .append("\nin the plugin's installation directory (typically plugins/opensearch-data-plane/) and restart")
            .append("\nthe node. Nothing in this plugin names a backend, so a scheme that is not listed above is a")
            .append("\nmissing jar rather than an unsupported store.");
        return description.toString();
    }

    private static String schemeOf(String uri) {
        try {
            String scheme = new URI(uri).getScheme();
            return scheme == null ? "(none)" : scheme.toLowerCase(Locale.ROOT);
        } catch (URISyntaxException e) {
            return "(unparseable)";
        }
    }
}
