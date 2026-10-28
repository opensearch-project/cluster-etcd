/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The weighted shard routing weights: one document per tenant, carrying its own version.
 *
 * <p>There is exactly one, because that is what OpenSearch has — {@code WeightedRoutingMetadata} is a
 * single cluster-state custom holding one attribute name and its weights, not a document per attribute.
 * A {@code PUT} for a second attribute <em>replaces</em> the first rather than sitting beside it, which
 * is verified and surprising enough to be worth stating twice; see {@link WeightedRoutingHandlers}.
 *
 * <h2>Two versions, and they are not the same version</h2>
 *
 * <p>This document is the first to carry a client-visible {@code _version}, and it is unrelated to the
 * key/value store's revision:
 *
 * <ul>
 *   <li>{@code _version} is <b>OpenSearch's</b> optimistic concurrency token. The client sends the
 *       version it believes is current, a mismatch is a 409, and a successful write stores
 *       {@code sent + 1}. It is part of the API and its exact values are observable.</li>
 *   <li>The store revision is <b>ours</b>, and guards the read-modify-write against another request
 *       interleaving between the read and the write — the same compare-and-set every other store here
 *       uses. A client never sees it.</li>
 * </ul>
 *
 * <p>Both are needed. Dropping {@code _version} would break the API; relying on it alone would let two
 * concurrent requests that both sent the correct version each decide they had won.
 *
 * <h2>Absent is not the same as empty</h2>
 *
 * <p>A cluster that has never had weights set has no metadata custom at all, and one whose weights were
 * deleted has an empty custom at a bumped version. They answer differently — {@code GET} gives
 * {@code {}} for the first and {@code {"weights":{},"_version":N,...}} for the second — so the absent
 * case is {@link Optional#empty()} here rather than a document with no weights.
 *
 * <p>This distinction is only reachable on a cluster that has never had weights, which is exactly the
 * state a probe destroys the first time it writes any. Worth remembering when verifying against a live
 * cluster: the pristine branch cannot be re-entered without wiping the data directory.
 */
final class WeightedRoutingStore {

    static final String KEY = "cluster/weighted_routing";

    /** {@code WeightedRoutingMetadata.INITIAL_VERSION} — what a first write must claim to be replacing. */
    static final long INITIAL_VERSION = -1;

    /** {@code WeightedRoutingMetadata.VERSION_UNSET_VALUE} — no {@code _version} was supplied. */
    static final long VERSION_UNSET = -2;

    private static final String ATTRIBUTE = "attribute";
    private static final String WEIGHTS = "weights";
    private static final String VERSION = "version";

    private final KeyValueStore store;

    WeightedRoutingStore(KeyValueStore store) {
        this.store = store;
    }

    /**
     * The stored weights and the revision they were read at.
     *
     * <p>{@code weights} is a {@link HashMap} on purpose, not merely by habit: OpenSearch renders it with
     * {@code Map.toString()} into two client-visible error messages, so reproducing those byte for byte
     * means reproducing Java's hash iteration order. JSON output sorts instead — see
     * {@link WeightedRoutingHandlers}.
     *
     * @param attribute the awareness attribute the weights are for; empty string once deleted, exactly as
     *                  {@code new WeightedRouting()} leaves it
     */
    record Document(String attribute, HashMap<String, Double> weights, long version, long revision) {}

    Optional<Document> read() {
        Optional<KeyValueStore.Versioned> stored = store.get(KEY);
        if (stored.isEmpty()) {
            return Optional.empty();
        }
        JsonNode document = parse(stored.get().value());
        HashMap<String, Double> weights = new HashMap<>();
        document.path(WEIGHTS).properties()
            .forEach(weight -> weights.put(weight.getKey(), weight.getValue().asDouble()));
        return Optional.of(new Document(
            document.path(ATTRIBUTE).asText(""),
            weights,
            document.path(VERSION).asLong(INITIAL_VERSION),
            stored.get().revision()
        ));
    }

    /**
     * Writes the single document, failing if it changed since it was read.
     *
     * <p>Weights are stored as JSON numbers because that is what they are — a weight is a double, and it
     * is only the wire format that spells one as a string on the way in and on the way out.
     *
     * @return false if another writer got there first
     */
    boolean write(String attribute, Map<String, Double> weights, long version, long expectedRevision) {
        ObjectNode document = Json.object();
        document.put(ATTRIBUTE, attribute);
        ObjectNode stored = document.putObject(WEIGHTS);
        // Sorted, so the stored bytes do not depend on hash order even though the in-memory map does.
        new java.util.TreeMap<>(weights).forEach(stored::put);
        document.put(VERSION, version);
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
            throw new UncheckedIOException("corrupt weighted routing metadata", e);
        }
    }
}
