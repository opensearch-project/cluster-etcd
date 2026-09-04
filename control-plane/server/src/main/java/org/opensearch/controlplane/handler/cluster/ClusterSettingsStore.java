/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

/**
 * Cluster settings in the key/value store: one document holding the two scopes.
 *
 * <p><b>Persistent and transient are kept separate and otherwise uninterpreted.</b> The control plane's
 * job here is to remember what a client set, in the scope it set it in. Which one wins is a question for
 * whoever reads them, and the data plane resolves it by merging with transient taking precedence — the
 * semantics OpenSearch itself has. Nothing here needs to know that, and not knowing it is what keeps
 * this a store rather than a policy.
 *
 * <p>Both scopes hold {@link Settings} in the canonical flat form, so a client may write
 * {@code {"cluster":{"max_shards_per_node":1500}}} or {@code {"cluster.max_shards_per_node":1500}} and
 * read back whichever shape it asks for.
 *
 * <p>One document rather than one per setting, because a cluster-settings update is naturally
 * read-modify-write over the whole set and there is exactly one of them per tenant. That makes the
 * update a single-key compare-and-swap, which is genuinely atomic on any backend — the same property
 * that made aliases their own resource, arrived at from the opposite direction.
 *
 * <p>Note "per tenant": the store handed to a handler is already tenant-confined, so each tenant has its
 * own cluster settings. That is the same choice as {@code cluster_uuid} being per tenant — each tenant
 * sees a coherent virtual cluster of its own.
 */
final class ClusterSettingsStore {

    static final String KEY = "cluster/settings";

    static final String PERSISTENT = "persistent";
    static final String TRANSIENT = "transient";

    private final KeyValueStore store;

    ClusterSettingsStore(KeyValueStore store) {
        this.store = store;
    }

    /** Both scopes and the revision they were read at, for a read-modify-write that must not lose one. */
    record Current(Settings persistent, Settings transientSettings, long revision) {}

    Current read() {
        Optional<KeyValueStore.Versioned> stored = store.get(KEY);
        if (stored.isEmpty()) {
            return new Current(Settings.EMPTY, Settings.EMPTY, KeyValueStore.ABSENT);
        }
        JsonNode document = parse(stored.get().value());
        return new Current(
            Settings.of(document.path(PERSISTENT)),
            Settings.of(document.path(TRANSIENT)),
            stored.get().revision()
        );
    }

    /**
     * Writes both scopes back, failing if they changed since they were read.
     *
     * @return false if another writer got there first
     */
    boolean write(Settings persistent, Settings transientSettings, long expectedRevision) {
        ObjectNode document = Json.object();
        document.set(PERSISTENT, persistent.toJson(true));
        document.set(TRANSIENT, transientSettings.toJson(true));
        try {
            store.put(KEY, Json.toBytes(document), expectedRevision);
            return true;
        } catch (RevisionConflictException lostTheRace) {
            return false;
        }
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt cluster settings", e);
        }
    }
}
