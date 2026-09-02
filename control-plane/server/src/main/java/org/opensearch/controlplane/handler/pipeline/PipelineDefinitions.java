/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.pipeline;

import org.opensearch.controlplane.store.KeyValueStore;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Pipeline definitions in the key/value store: key layout and ID expression resolution, for either
 * {@link PipelineKind}.
 *
 * <p>Payloads are stored as opaque canonical JSON. The control plane does not model processors: it has
 * no processor registry and cannot execute a pipeline, so definitions are stored and returned
 * verbatim.
 *
 * <p><b>This is a known divergence, not a settled position.</b> A real cluster validates processor
 * types at PUT time and rejects unknown ones with {@code 400 illegal_argument_exception: Invalid
 * processor type <name>}. We accept them, so a client can store a pipeline that will fail at
 * execution time rather than at definition time. Observed directly: a pipeline using
 * {@code normalization-processor} is accepted here and rejected by a cluster without the
 * neural-search plugin.
 *
 * <p>The awkward part is that validity is not a property of the control plane. Whether
 * {@code normalization-processor} is valid depends on which plugins the data nodes have, which this
 * service has no way to know today. Resolving it means either a processor registry told what is
 * installed, or the data plane publishing its available processor types for the control plane to
 * validate against — a channel that does not exist yet.
 */
final class PipelineDefinitions {

    private final KeyValueStore store;
    private final PipelineKind kind;

    PipelineDefinitions(KeyValueStore store, PipelineKind kind) {
        this.store = store;
        this.kind = kind;
    }

    String key(String id) {
        return kind.keyPrefix() + id;
    }

    private String idFromKey(String key) {
        return key.substring(kind.keyPrefix().length());
    }

    Optional<byte[]> get(String id) {
        return store.get(key(id)).map(KeyValueStore.Versioned::value);
    }

    void put(String id, byte[] canonicalJson) {
        store.put(key(id), canonicalJson);
        if (hasMetadata() == false) {
            store.put(kind.metadataMarkerKey(), new byte[0]);
        }
    }

    boolean delete(String id) {
        return store.delete(key(id));
    }

    /**
     * Whether pipeline metadata has ever existed here. Once true, never false again.
     *
     * <p>Mirrors the presence of {@code IngestMetadata} (or its search equivalent) in OpenSearch's
     * cluster state, which the first successful put creates and nothing ever removes — not even
     * deleting every pipeline. {@link DeletePipelineHandler} needs it: deleting a nonexistent pipeline
     * is a 200 on a pristine cluster but a 404 once any pipeline has ever existed. Verified against a
     * live cluster for both kinds.
     */
    boolean hasMetadata() {
        return store.get(kind.metadataMarkerKey()).isPresent();
    }

    /** Every stored definition, in ID order. */
    NavigableMap<String, byte[]> all() {
        NavigableMap<String, byte[]> definitions = new TreeMap<>();
        store.scanPrefix(kind.keyPrefix())
             .forEach((key, versioned) -> definitions.put(idFromKey(key), versioned.value()));
        return definitions;
    }

    /**
     * Resolves an ID expression: a comma-separated list of IDs, each of which may contain {@code *}
     * wildcards. A {@code null} or empty expression means all definitions, matching a bare
     * {@code GET} with no ID.
     */
    NavigableMap<String, byte[]> resolve(String idExpression) {
        if (idExpression == null || idExpression.isEmpty() || MATCH_ALL.equals(idExpression)) {
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
                // Scan once, however many wildcard tokens there are.
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

    /** OpenSearch exempts only the exact match-all pattern from the not-found check, not wildcards generally. */
    static final String MATCH_ALL = "*";

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
