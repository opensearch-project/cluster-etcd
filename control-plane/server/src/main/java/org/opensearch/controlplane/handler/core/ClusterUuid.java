/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import org.opensearch.controlplane.store.CreateIfAbsent;
import org.opensearch.controlplane.store.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * The {@code cluster_uuid} reported by {@code GET /}.
 *
 * <p>Generated once and persisted, because it must be stable across restarts: clients treat a change
 * as "this is a different cluster" and may discard cached state. Stored through the tenant-scoped
 * store, so each tenant gets its own — a tenant sees a coherent virtual cluster of its own rather
 * than one shared identity.
 */
public final class ClusterUuid {

    /** Under the tenant prefix, so it is per-tenant. */
    static final String KEY = "cluster/uuid";

    private static final SecureRandom RANDOM = new SecureRandom();

    private ClusterUuid() {}

    /** Reads the stored UUID, generating and persisting one on first call. */
    public static String resolve(KeyValueStore store) {
        return store.get(KEY)
            .map(versioned -> new String(versioned.value(), StandardCharsets.UTF_8))
            .orElseGet(() -> generateAndStore(store));
    }

    private static String generateAndStore(KeyValueStore store) {
        // Create-if-absent: two concurrent first requests must not end up with different UUIDs. Whichever value
        // ends up stored is the one of record, ours or another request's, so the stored value is adopted either way;
        // a lost answer is settled by the same read.
        CreateIfAbsent.create(store, KEY, generate().getBytes(StandardCharsets.UTF_8), stored -> true);
        return store.get(KEY)
            .map(versioned -> new String(versioned.value(), StandardCharsets.UTF_8))
            .orElseThrow(() -> new IllegalStateException("cluster uuid was stored and is now gone"));
    }

    /**
     * OpenSearch's format: 128 random bits, base64url, no padding, giving 22 characters such as
     * {@code G9zz_G-6QJub-r6T13kkOw}.
     */
    private static String generate() {
        byte[] bytes = new byte[16];
        RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
}
