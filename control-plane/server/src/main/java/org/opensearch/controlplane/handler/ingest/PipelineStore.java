/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.ingest;

import org.opensearch.controlplane.store.KeyValueStore;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Pipeline definitions in the key/value store: key layout and ID expression resolution.
 *
 * <p>Payloads are stored as opaque canonical JSON. The control plane deliberately does not model
 * processors — it has no processor registry and cannot execute a pipeline, so validating anything
 * beyond "this is a JSON object" would mean duplicating knowledge that belongs to the data plane.
 * Definitions are stored and returned verbatim.
 */
final class PipelineStore {

    /** Key layout: {@code ingest/pipeline/<id>}. */
    static final String KEY_PREFIX = "ingest/pipeline/";

    /**
     * Marker for "this cluster has had ingest metadata at some point".
     *
     * <p>Mirrors the presence of {@code IngestMetadata} in OpenSearch's cluster state, which is
     * created by the first successful pipeline PUT and never removed thereafter — not even when
     * every pipeline is deleted. {@link DeletePipelineHandler} needs it: OpenSearch's
     * {@code IngestService.innerDelete} returns early with no error when there is no ingest
     * metadata at all, so a delete of a nonexistent pipeline is a 200 on a pristine cluster but a
     * 404 once any pipeline has ever existed. Verified against a live cluster.
     *
     * <p>Deliberately outside {@link #KEY_PREFIX} so it never shows up in a pipeline scan.
     */
    static final String METADATA_MARKER_KEY = "ingest/metadata-initialized";

    private static final byte[] MARKER_VALUE = new byte[0];

    private final KeyValueStore store;

    PipelineStore(KeyValueStore store) {
        this.store = store;
    }

    static String key(String id) {
        return KEY_PREFIX + id;
    }

    static String idFromKey(String key) {
        return key.substring(KEY_PREFIX.length());
    }

    Optional<byte[]> get(String id) {
        return store.get(key(id)).map(KeyValueStore.Versioned::value);
    }

    void put(String id, byte[] canonicalJson) {
        store.put(key(id), canonicalJson);
        if (hasIngestMetadata() == false) {
            store.put(METADATA_MARKER_KEY, MARKER_VALUE);
        }
    }

    /**
     * Whether ingest metadata has ever existed here. Once true, never false again — see
     * {@link #METADATA_MARKER_KEY}.
     */
    boolean hasIngestMetadata() {
        return store.get(METADATA_MARKER_KEY).isPresent();
    }

    boolean delete(String id) {
        return store.delete(key(id));
    }

    /** Every stored pipeline, in ID order. */
    NavigableMap<String, byte[]> all() {
        NavigableMap<String, byte[]> pipelines = new TreeMap<>();
        store.scanPrefix(KEY_PREFIX).forEach((key, versioned) -> pipelines.put(idFromKey(key), versioned.value()));
        return pipelines;
    }

    /**
     * Resolves an ID expression: a comma-separated list of IDs, each of which may contain
     * {@code *} wildcards. A {@code null} expression means all pipelines, matching
     * {@code GET /_ingest/pipeline} with no ID.
     */
    NavigableMap<String, byte[]> resolve(String idExpression) {
        if (idExpression == null || idExpression.isEmpty() || "*".equals(idExpression)) {
            return all();
        }

        NavigableMap<String, byte[]> matched = new TreeMap<>();
        NavigableMap<String, byte[]> everything = null;

        for (String token : idExpression.split(",")) {
            String id = token.trim();
            if (id.isEmpty()) {
                continue;
            }
            if (hasWildcard(id)) {
                // Only scan once, however many wildcard tokens there are.
                if (everything == null) {
                    everything = all();
                }
                Pattern pattern = globToPattern(id);
                everything.forEach((candidate, value) -> {
                    if (pattern.matcher(candidate).matches()) {
                        matched.put(candidate, value);
                    }
                });
            } else {
                get(id).ifPresent(value -> matched.put(id, value));
            }
        }
        return matched;
    }

    static boolean hasWildcard(String idExpression) {
        return idExpression != null && idExpression.indexOf('*') >= 0;
    }

    /** The spec documents only {@code *}, so everything else is matched literally. */
    private static Pattern globToPattern(String glob) {
        StringBuilder regex = new StringBuilder(glob.length() + 8);
        int literalStart = 0;
        for (int i = 0; i < glob.length(); i++) {
            if (glob.charAt(i) == '*') {
                if (i > literalStart) {
                    regex.append(Pattern.quote(glob.substring(literalStart, i)));
                }
                regex.append(".*");
                literalStart = i + 1;
            }
        }
        if (literalStart < glob.length()) {
            regex.append(Pattern.quote(glob.substring(literalStart)));
        }
        return Pattern.compile(regex.toString());
    }
}
