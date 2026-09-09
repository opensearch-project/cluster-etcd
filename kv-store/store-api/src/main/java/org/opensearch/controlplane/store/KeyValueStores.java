/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;

/**
 * Resolves a {@code --store} URI to a {@link KeyValueStore} through {@link KeyValueStoreProvider}.
 *
 * <p>The one place the control plane turns configuration into a backend. Everything above it holds a
 * {@code KeyValueStore} and knows nothing about which one.
 */
public final class KeyValueStores {

    private KeyValueStores() {}

    /**
     * Opens the store a URI names.
     *
     * @param spec a store URI such as {@code file:///var/lib/cp} or
     *             {@code etcd://localhost:2379/controlplane}
     * @throws IOException if the URI is unusable, no provider claims its scheme, or the backend cannot
     *                     be reached
     */
    public static KeyValueStore open(String spec) throws IOException {
        URI uri;
        try {
            uri = new URI(spec);
        } catch (URISyntaxException e) {
            throw new IOException("store [" + spec + "] is not a valid URI: " + e.getReason(), e);
        }
        if (uri.getScheme() == null) {
            throw new IOException(
                "store [" + spec + "] has no scheme; expected something like " + describeAvailable());
        }

        String scheme = uri.getScheme().toLowerCase(java.util.Locale.ROOT);
        Map<String, KeyValueStoreProvider> providers = discover();
        KeyValueStoreProvider provider = providers.get(scheme);
        if (provider == null) {
            throw new IOException(
                "no store provider for scheme [" + scheme + "]. " + describeAvailable()
                    + ". A backend is a jar on the classpath; check that it is there."
            );
        }
        return provider.open(uri);
    }

    /**
     * Every provider on the classpath, by scheme.
     *
     * <p>A duplicate scheme is an error rather than a silent win for whichever jar loaded first: two
     * etcd implementations on one classpath is a packaging mistake, and picking one arbitrarily would
     * mean the running backend is not determined by configuration.
     */
    public static Map<String, KeyValueStoreProvider> discover() throws IOException {
        Map<String, KeyValueStoreProvider> byScheme = new TreeMap<>();
        List<String> duplicates = new ArrayList<>();
        for (KeyValueStoreProvider provider : ServiceLoader.load(
            KeyValueStoreProvider.class, KeyValueStores.class.getClassLoader())) {
            String scheme = provider.scheme().toLowerCase(java.util.Locale.ROOT);
            KeyValueStoreProvider existing = byScheme.put(scheme, provider);
            if (existing != null) {
                duplicates.add(scheme + " (" + existing.getClass().getName()
                    + " and " + provider.getClass().getName() + ")");
            }
        }
        if (duplicates.isEmpty() == false) {
            throw new IOException("more than one store provider claims a scheme: " + String.join(", ", duplicates));
        }
        return byScheme;
    }

    /** For {@code --help} and for the error a bad scheme produces. */
    public static String describeAvailable() {
        Map<String, KeyValueStoreProvider> providers;
        try {
            providers = discover();
        } catch (IOException e) {
            return "store providers could not be listed: " + e.getMessage();
        }
        if (providers.isEmpty()) {
            return "no store providers are on the classpath";
        }
        StringBuilder description = new StringBuilder("available stores:");
        providers.forEach((scheme, provider) -> description.append("\n  ").append(provider.usage()));
        return description.toString();
    }
}
