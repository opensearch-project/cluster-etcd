/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.Map;
import java.util.Objects;

/**
 * {@code tenant/<t>/indices/<name>} — one index's configuration, written by the control plane.
 *
 * <p>The node reads this rather than being told it. That decision is recorded in {@code NODE_CONTRACT.md}'s
 * open question 1 and it is the reason the assignment stays small: inlining settings and mappings into every
 * node's assignment would make the hot, per-node document carry a copy of data that is identical for every
 * reader.
 *
 * <p>Three things about this document are traps, and all three are stated in {@code KEY_LAYOUT.md}:
 *
 * <ul>
 *   <li><b>{@code settings} is flat and every value is a string.</b> {@code "3"}, not {@code 3};
 *       {@code "true"}, not {@code true}. Keys are always fully qualified with the {@code index.} prefix
 *       whatever shape the client sent. Reading {@code "3"} as an int the wrong way silently yields zero,
 *       which for {@code index.number_of_shards} would be an index with no shards.
 *   <li><b>{@code uuid} is the identity, not the name.</b> This document is keyed by name — the only place in
 *       the layout that is — so the pair must be checked. See {@link #matches}.
 *   <li><b>{@code state} is {@code open} or {@code close}.</b> A closed index still has settings and mappings
 *       and still exists; it simply should not be serving, and the contract has a node report it as having no
 *       copies at all.
 * </ul>
 *
 * <p>And a fourth trap that is not in the settings block at all: <b>{@code ingestion_status} is a first-class
 * field</b>, a sibling of {@code state} rather than a setting, because that is what it is upstream —
 * {@code IngestionStatus.isPaused} on {@code IndexMetadata}. The obvious guess of {@code index.ingestion.paused}
 * under {@code settings} finds nothing, silently, which is why it is worth naming here.
 *
 * @param revision the store revision this was read at, which is what the inventory reports as
 *     {@code config_revision}. Not part of the document: the store supplies it, and it is opaque.
 */
public record IndexConfig(
    String name,
    String uuid,
    long creationDate,
    boolean open,
    boolean ingestionPaused,
    Map<String, String> settings,
    Map<String, Object> mappings,
    long revision
) {

    /** The index document's field for the pause, matching {@code IndexMetadata}'s own key. */
    public static final String FIELD_INGESTION_STATUS = "ingestion_status";

    /** Matching {@code IngestionStatus.IS_PAUSED}. Absent means not paused. */
    public static final String FIELD_IS_PAUSED = "is_paused";

    public IndexConfig {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uuid, "uuid");
        settings = settings == null ? Map.of() : Map.copyOf(settings);
        mappings = mappings == null ? Map.of() : Map.copyOf(mappings);
    }

    /**
     * Everything except the ingestion pause, which only an index with a pull-based source can mean anything by.
     *
     * <p>Kept so that the pause could be added without touching every construction site, and because a caller
     * with no opinion about ingestion should not have to express one.
     */
    public IndexConfig(
        String name,
        String uuid,
        long creationDate,
        boolean open,
        Map<String, String> settings,
        Map<String, Object> mappings,
        long revision
    ) {
        this(name, uuid, creationDate, open, false, settings, mappings, revision);
    }

    /**
     * Whether this document really describes the index an assignment named.
     *
     * <p><b>The one torn read in the contract that a later pass would not correct.</b> The assignment
     * identifies an index by uuid and this document is keyed by name, so an index deleted and recreated under
     * the same name hands the node a perfectly valid document describing a different index. Both halves read
     * cleanly; only the pair is wrong — and by the time a later pass noticed, the node would have built the
     * shard already, against the wrong mappings, and reported it as the uuid it was asked for.
     *
     * <p>A mismatch therefore means "this index is not present yet", not "configuration to apply".
     */
    public boolean matches(String assignedUuid) {
        return uuid.equals(assignedUuid);
    }

    /**
     * The setting an index cannot do without, read the way the flat-string shape demands.
     *
     * @throws MalformedDocumentException if it is absent or unparseable, because guessing a shard count is
     *     worse than declining to build the index
     */
    public int numberOfShards() {
        return requiredInt("index.number_of_shards");
    }

    /** Replicas the control plane asked for. Zero is a legitimate answer, so absence is not. */
    public int numberOfReplicas() {
        return requiredInt("index.number_of_replicas");
    }

    /**
     * {@code DOCUMENT} or {@code SEGMENT}, which decides whether a copy resolves peers at all.
     *
     * <p>Defaults to segment replication when the setting is absent, matching the deployment this plugin
     * currently supports: the first version is segment replication from the remote store, and a copy that
     * pulls from the remote store resolves no peers. A document-replication index reaching a node that cannot
     * do docrep yet is a real possibility, and the honest failure is a copy that cannot be built rather than
     * one built as though it were something else — see {@code Replication}.
     */
    public String replication() {
        return settings.getOrDefault("index.replication.type", "SEGMENT").toUpperCase(java.util.Locale.ROOT);
    }

    private int requiredInt(String key) {
        String value = settings.get(key);
        if (value == null) {
            throw new MalformedDocumentException("index [" + name + "]", "[" + key + "] is missing from its settings");
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            // The silent-zero bug, made loud. Settings are strings here, so this is the likely failure.
            throw new MalformedDocumentException("index [" + name + "]", "[" + key + "] is not a number: [" + value + "]");
        }
    }

    // ---------------------------------------------------------------- reading

    public static IndexConfig read(byte[] bytes, long revision) {
        String what = "index configuration";
        Map<String, Object> document = Documents.parse(bytes, what);

        String name = Documents.requireString(document, "index", what);
        String uuid = Documents.requireString(document, "uuid", what);
        // Epoch millis in a string, matching what OpenSearch stores. Absent is survivable: the fabricator
        // derives a deterministic date rather than reaching for the clock, because a date generated per node
        // would make the same index look different on every node.
        long creationDate = Documents.number(document, "creation_date", 0, what);
        boolean open = "open".equalsIgnoreCase(Documents.string(document, "state", "open"));
        // A first-class field beside state, not a setting. Absent means not paused, which is what makes every
        // index document written before the pause existed still legal.
        boolean ingestionPaused = Documents.bool(Documents.object(document, FIELD_INGESTION_STATUS), FIELD_IS_PAUSED, false);

        return new IndexConfig(
            name,
            uuid,
            creationDate,
            open,
            ingestionPaused,
            Documents.stringMap(document, "settings"),
            Documents.object(document, "mappings"),
            revision
        );
    }
}
