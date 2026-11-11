/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.capability.NodeVersion;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.store.CreateIfAbsent;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;
import org.opensearch.controlplane.store.WriteLimits;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalInt;
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
public final class IndexStore {

    /** Shared with the resolver, which scans the same prefix. */
    static final String KEY_PREFIX = IndexAbstractions.INDEX_KEY_PREFIX;

    /**
     * The OpenSearch version an index was created at, {@code "3.10.0"}: fixed at creation to the oldest data-node
     * version and never rewritten. Top-level beside {@code uuid} and {@code creation_date}, and rendered into
     * settings as {@code index.version.created}.
     */
    public static final String VERSION_CREATED = "version_created";

    static final String STATE = "state";

    /** OpenSearch's own spellings: {@code IndexMetadata.State} serialises as {@code open} / {@code close}. */
    static final String STATE_OPEN = "open";
    static final String STATE_CLOSE = "close";

    /**
     * Written into an index's settings when it is closed and removed when it is reopened.
     *
     * <p>Not invented here — a real cluster does exactly this, and it is observable:
     * {@code GET /i/_settings} on a closed index reports
     * {@code index.verified_before_close: "true"}, and reopening removes the key rather than setting it
     * to false. Reproduced so a closed index's settings match.
     */
    static final String SETTING_VERIFIED_BEFORE_CLOSE = "index.verified_before_close";

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

    /**
     * The rejection for an index document that would be over the store's value limit, checked before
     * writing so that an operation over several indices is refused before it changes any of them.
     */
    static Optional<RestResponse> rejectIfTooLarge(RestRequest request, String index, ObjectNode metadata) {
        OptionalInt bytes = WriteLimits.oversized(metadata);
        return bytes.isPresent()
            ? Optional.of(ErrorResponses.documentTooLarge(request, "index [" + index + "]", bytes.getAsInt()))
            : Optional.empty();
    }

    /** Metadata with the revision it was read at, for a read-modify-write that must not lose updates. */
    Optional<KeyValueStore.Versioned> getVersioned(String index) {
        return store.get(key(index));
    }

    /**
     * Writes metadata back, failing if the index changed since it was read.
     *
     * <p>Compare-and-set rather than an unconditional put: {@code put_mapping} is a
     * read-modify-write, and two concurrent updates to different fields of the same index would
     * otherwise silently lose one. This is the second reason {@link KeyValueStore} carries revisions,
     * after create-if-absent.
     *
     * @return false if another writer got there first
     */
    boolean replace(String index, ObjectNode metadata, long expectedRevision) {
        try {
            store.put(key(index), Json.toBytes(metadata), expectedRevision);
            return true;
        } catch (RevisionConflictException lostTheRace) {
            return false;
        }
    }

    /**
     * An index's settings as a cluster reports them.
     *
     * <p>Not simply the stored {@code settings} block. A real cluster writes {@code index.uuid},
     * {@code index.creation_date} and {@code index.provided_name} into an index's settings when it
     * creates it; here they are top-level metadata fields, so they are rendered back in on the way out.
     * Same information, held once rather than twice.
     *
     * <p>{@code index.version.created} comes from {@link #VERSION_CREATED} the same way, rendered as upstream's
     * {@code Version.id} ({@link NodeVersion#encodedId}), which is what a client of {@code get_settings} sees. An
     * index created before a minimum node version was declared has none, and reports none.
     */
    public static Settings reportedSettings(JsonNode metadata) {
        Settings settings = Settings.forIndex(metadata.path("settings"));
        if (metadata.hasNonNull("uuid")) {
            settings = settings.with("index.uuid", metadata.get("uuid").asText());
        }
        if (metadata.hasNonNull("creation_date")) {
            settings = settings.with("index.creation_date", metadata.get("creation_date").asText());
        }
        if (metadata.hasNonNull("index")) {
            settings = settings.with("index.provided_name", metadata.get("index").asText());
        }
        if (metadata.hasNonNull(VERSION_CREATED)) {
            try {
                settings = settings.with("index.version.created",
                    Integer.toString(NodeVersion.parse(metadata.get(VERSION_CREATED).asText()).encodedId()));
            } catch (IllegalArgumentException unreadable) {
                // Written by this service, so this would be a corrupt document; report what is there rather than fail.
            }
        }
        return settings;
    }

    boolean exists(String index) {
        return store.get(key(index)).isPresent();
    }

    static boolean isOpen(JsonNode metadata) {
        return STATE_OPEN.equals(metadata.path(STATE).asText(STATE_OPEN));
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
     * <p>Settled when the store loses its answer, by the uuid this request generated, which no other create can
     * have stored: see {@link CreateIfAbsent}. Without that, a create that applied would answer an error, and the
     * client's retry {@code resource_already_exists_exception} for an index it made itself.
     *
     * @return the created metadata, or empty if another create got there first
     * @throws org.opensearch.controlplane.store.OutcomeUnknownException if the store never said
     */
    Optional<ObjectNode> createIfAbsent(ObjectNode metadata, String index) {
        String uuid = metadata.path("uuid").asText();
        boolean ours = CreateIfAbsent.create(store, key(index), Json.toBytes(metadata),
            stored -> uuid.equals(parse(stored).path("uuid").asText(null)));
        return ours ? Optional.of(metadata) : Optional.empty();
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
