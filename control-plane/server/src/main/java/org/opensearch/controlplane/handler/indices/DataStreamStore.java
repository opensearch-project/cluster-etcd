/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Data streams, one document per stream under {@code data_streams/}.
 *
 * <p>A data stream is a name in front of an ordered list of hidden backing indices, the last of which is
 * the write index. The stream document holds that list, the timestamp field its backing indices index on,
 * and the generation counter; the backing indices themselves are ordinary index documents in
 * {@link IndexStore}, which is what lets every existing index read serve them without knowing they belong
 * to a stream.
 *
 * <h2>The document is not the wire shape</h2>
 *
 * <p>{@code timestamp_field} is stored as a plain string and rendered as {@code {"name": "@timestamp"}} on
 * the way out, the same way settings are stored flat and rendered nested. There is one piece of
 * information and the wrapper is a rendering of it.
 *
 * <h2>Backing index names are derived, not stored twice</h2>
 *
 * <p>{@code DataStream.getDefaultBackingIndexName}: {@code .ds-<name>-%06d}, so generation 1 of
 * {@code logs} is {@code .ds-logs-000001}. Transcribed rather than invented, because the name is a
 * contract — the data plane parses a backing index name to find the stream it belongs to, and
 * {@code rollover} has to produce the next one.
 */
final class DataStreamStore {

    static final String KEY_PREFIX = "data_streams/";

    static final String BACKING_INDEX_PREFIX = ".ds-";

    static final String NAME = "name";
    static final String TIMESTAMP_FIELD = "timestamp_field";
    static final String INDICES = "indices";
    static final String GENERATION = "generation";
    static final String INDEX_NAME = "index_name";
    static final String INDEX_UUID = "index_uuid";

    /** {@code @timestamp}, when the template's {@code data_stream} block does not name one. */
    static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";

    private final KeyValueStore store;

    DataStreamStore(KeyValueStore store) {
        this.store = store;
    }

    static String key(String name) {
        return KEY_PREFIX + name;
    }

    /** {@code .ds-<name>-000001} for generation 1. */
    static String backingIndexName(String dataStream, long generation) {
        return String.format(Locale.ROOT, BACKING_INDEX_PREFIX + "%s-%06d", dataStream, generation);
    }

    /** One backing index as the stream records it. */
    record BackingIndex(String name, String uuid) {}

    Optional<ObjectNode> get(String name) {
        return store.get(key(name)).map(versioned -> (ObjectNode) parse(versioned.value()));
    }

    boolean exists(String name) {
        return store.get(key(name)).isPresent();
    }

    /** Every stream, by name. Sorted, which is the order {@code get_data_stream} reports. */
    NavigableMap<String, ObjectNode> all() {
        NavigableMap<String, ObjectNode> streams = new TreeMap<>();
        store.scanPrefix(KEY_PREFIX).forEach((key, versioned) ->
            streams.put(key.substring(KEY_PREFIX.length()), (ObjectNode) parse(versioned.value())));
        return streams;
    }

    /**
     * Writes a new stream, failing if one already has the name.
     *
     * @return false if another writer got there first
     */
    boolean createIfAbsent(String name, String timestampField, long generation, List<BackingIndex> indices) {
        ObjectNode document = Json.object();
        document.put(NAME, name);
        document.put(TIMESTAMP_FIELD, timestampField);
        document.put(GENERATION, generation);
        ArrayNode backing = document.putArray(INDICES);
        indices.forEach(index -> {
            ObjectNode entry = backing.addObject();
            entry.put(INDEX_NAME, index.name());
            entry.put(INDEX_UUID, index.uuid());
        });
        try {
            store.put(key(name), Json.toBytes(document), KeyValueStore.ABSENT);
            return true;
        } catch (org.opensearch.controlplane.store.RevisionConflictException lostTheRace) {
            return false;
        }
    }

    boolean delete(String name) {
        return store.delete(key(name));
    }

    /** The backing indices a stream document names, in order — the last is the write index. */
    static List<BackingIndex> backingIndicesOf(JsonNode document) {
        List<BackingIndex> indices = new ArrayList<>();
        document.path(INDICES).forEach(entry ->
            indices.add(new BackingIndex(entry.path(INDEX_NAME).asText(""), entry.path(INDEX_UUID).asText(""))));
        return indices;
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt data stream metadata", e);
        }
    }
}
