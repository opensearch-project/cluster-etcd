/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Index metadata in the key/value store.
 *
 * <p>Key layout is {@code indices/<name>}, holding the whole metadata document. One document per
 * index rather than a single cluster-state blob, so that creating one index never has to
 * read-modify-write another's metadata — which is what makes concurrent creates safe with nothing
 * more than a create-if-absent.
 *
 * <p>Every index carries a UUID, because OpenSearch's own errors expose it: the already-exists
 * message is {@code index [name/uuid] already exists}, so a client can see it.
 */
final class IndexStore {

    /** Shared with the resolver, which scans the same prefix. */
    static final String KEY_PREFIX = IndexAbstractions.INDEX_KEY_PREFIX;

    private static final SecureRandom RANDOM = new SecureRandom();

    private final KeyValueStore store;

    IndexStore(KeyValueStore store) {
        this.store = store;
    }

    static String key(String index) {
        return KEY_PREFIX + index;
    }

    static String nameFromKey(String key) {
        return key.substring(KEY_PREFIX.length());
    }

    Optional<JsonNode> get(String index) {
        return store.get(key(index)).map(versioned -> parse(versioned.value()));
    }

    boolean exists(String index) {
        return store.get(key(index)).isPresent();
    }

    NavigableMap<String, JsonNode> all() {
        NavigableMap<String, JsonNode> indices = new TreeMap<>();
        store.scanPrefix(KEY_PREFIX).forEach((key, versioned) ->
            indices.put(nameFromKey(key), parse(versioned.value())));
        return indices;
    }

    boolean delete(String index) {
        return store.delete(key(index));
    }

    /**
     * Writes metadata for a new index, failing if it already exists.
     *
     * <p>Create-if-absent rather than read-then-write: two concurrent creates of the same name must
     * not both succeed, and a check followed by an unconditional put has a window between them. This
     * is the reason {@link KeyValueStore} carries revisions at all.
     *
     * @return the created metadata, or empty if the index already existed
     */
    Optional<ObjectNode> createIfAbsent(ObjectNode metadata, String index) {
        try {
            store.put(key(index), Json.toBytes(metadata), KeyValueStore.ABSENT);
            return Optional.of(metadata);
        } catch (RevisionConflictException alreadyExists) {
            return Optional.empty();
        }
    }

    /** OpenSearch's format: 128 random bits, base64url, unpadded. */
    static String newUuid() {
        byte[] bytes = new byte[16];
        RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            // Stored by us, so unparseable means the store is corrupt rather than the client wrong.
            throw new UncheckedIOException("corrupt index metadata: " + new String(value, StandardCharsets.UTF_8), e);
        }
    }
}
