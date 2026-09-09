/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.etcd;

import io.etcd.jetcd.Client;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.KeyValueStoreProvider;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code etcd://host1:2379,host2:2379/controlplane}
 *
 * <p>The authority is a comma-separated endpoint list and the path is the key prefix everything lives
 * under. The prefix is required for the same reason ZooKeeper's root is: an etcd cluster is shared
 * infrastructure, and an unprefixed control plane would scan the whole keyspace on
 * {@code scanPrefix("")}.
 */
public final class EtcdKeyValueStoreProvider implements KeyValueStoreProvider {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

    @Override
    public String scheme() {
        return "etcd";
    }

    @Override
    public KeyValueStore open(URI uri) throws IOException {
        String authority = uri.getAuthority();
        if (authority == null || authority.isBlank()) {
            throw new IOException("store [" + uri + "] needs at least one etcd endpoint: " + usage());
        }
        String root = uri.getPath();
        if (root == null || root.isBlank() || root.equals("/")) {
            throw new IOException(
                "store [" + uri + "] needs a key prefix, so that these keys cannot collide with anything "
                    + "else on the cluster: " + usage());
        }
        if (root.endsWith("/")) {
            root = root.substring(0, root.length() - 1);
        }

        // jetcd wants fully qualified endpoints; the URI carries bare host:port, which is what an
        // operator expects to write.
        List<String> endpoints = new ArrayList<>();
        for (String authorityPart : authority.split(",")) {
            String endpoint = authorityPart.trim();
            if (endpoint.isEmpty()) {
                continue;
            }
            endpoints.add(endpoint.contains("://") ? endpoint : "http://" + endpoint);
        }
        if (endpoints.isEmpty()) {
            throw new IOException("store [" + uri + "] needs at least one etcd endpoint: " + usage());
        }

        Duration timeout = timeoutFrom(uri);
        Client client = Client.builder().endpoints(endpoints.toArray(String[]::new)).build();
        return new EtcdKeyValueStore(client, root, timeout.toMillis(), true);
    }

    /** {@code ?timeout=30s}, because a control plane far from its etcd wants a longer leash. */
    private static Duration timeoutFrom(URI uri) throws IOException {
        String query = uri.getQuery();
        if (query == null) {
            return DEFAULT_TIMEOUT;
        }
        for (String parameter : query.split("&")) {
            int equals = parameter.indexOf('=');
            if (equals < 0 || parameter.substring(0, equals).equals("timeout") == false) {
                continue;
            }
            String value = parameter.substring(equals + 1);
            try {
                // Seconds unless a unit is given, matching how OpenSearch reads a duration.
                return value.endsWith("ms")
                    ? Duration.ofMillis(Long.parseLong(value.substring(0, value.length() - 2)))
                    : Duration.ofSeconds(Long.parseLong(value.endsWith("s")
                        ? value.substring(0, value.length() - 1)
                        : value));
            } catch (NumberFormatException e) {
                throw new IOException("store [" + uri + "] has an unreadable timeout [" + value + "]");
            }
        }
        return DEFAULT_TIMEOUT;
    }

    @Override
    public String usage() {
        return "etcd://host:2379[,host:2379]/prefix   etcd v3, one key per key";
    }
}
