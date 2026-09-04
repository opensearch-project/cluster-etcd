/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.snapshot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.metadata.Settings;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.NameExpression;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Snapshot repository <em>configuration</em> in the key/value store, one document per repository.
 *
 * <p>Configuration only, and the distinction carries the whole design. A repository's definition — its
 * name, type and settings — is control plane state and lives here. What is <em>in</em> the repository is
 * not: {@code RepositoryData}, the list of snapshots and which indices each holds, lives in
 * {@code index-N} blobs inside the repository itself rather than in cluster state. So this store is
 * enough for {@code create_repository}, {@code get_repository} and {@code delete_repository}, and is not
 * enough for anything else in the namespace. See {@code docs/where-the-hard-parts-are.md} on the
 * boundary and on the second outbound seam the rest of it needs.
 *
 * <p>Settings are held in the canonical flat form, like every other settings document — but without the
 * implicit {@code index.} prefix, since a repository setting is written out in full ({@code location},
 * {@code compress}, {@code base_path}).
 */
final class RepositoryStore {

    static final String KEY_PREFIX = "snapshot/repositories/";

    static final String TYPE = "type";
    static final String SETTINGS = "settings";

    /** {@code GET /_snapshot/_all} means everything and, unlike {@code *}, is not a pattern. */
    static final String ALL = "_all";

    /**
     * The "has ever had a repository" marker, a sibling of the definitions rather than one of them.
     *
     * <p>Mirrors the presence of {@code RepositoriesMetadata} in OpenSearch's cluster state, which the
     * first successful put creates and nothing removes — not even deleting every repository. It is
     * observable, and it is the <b>second</b> instance of this artifact in this API: the first is
     * {@code IngestMetadata} behind {@code PipelineDefinitions.hasMetadata}. Same shape of trap, same
     * treatment.
     *
     * <p>Why it matters here: with no metadata at all, {@code TransportGetRepositoriesAction} takes an
     * early branch that reports {@code RepositoryMissingException(repositories[0])} — the first token
     * <em>verbatim</em>, pattern and all. Once metadata exists it takes the normal path, where a pattern
     * matching nothing contributes nothing and an all-pattern expression answers a bare {@code {}}. So
     * {@code GET /_snapshot/foo*} is a 404 on a pristine cluster and a 200 on one that has ever had a
     * repository.
     *
     * <p>Every probe of this behaviour on an empty cluster sees only the early branch, which is exactly
     * how the equivalent pipeline rule was missed the first time.
     */
    private static final String METADATA_MARKER_KEY = "snapshot/repositories-initialized";

    private final KeyValueStore store;

    RepositoryStore(KeyValueStore store) {
        this.store = store;
    }

    static String key(String name) {
        return KEY_PREFIX + name;
    }

    void put(String name, String type, Settings settings) {
        ObjectNode definition = Json.object();
        definition.put(TYPE, type);
        definition.set(SETTINGS, settings.toJson(true));
        store.put(key(name), Json.toBytes(definition));
        if (hasMetadata() == false) {
            store.put(METADATA_MARKER_KEY, new byte[0]);
        }
    }

    /** Whether repository metadata has ever existed here. Once true, never false again. */
    boolean hasMetadata() {
        return store.get(METADATA_MARKER_KEY).isPresent();
    }

    NavigableMap<String, ObjectNode> all() {
        NavigableMap<String, ObjectNode> repositories = new TreeMap<>();
        store.scanPrefix(KEY_PREFIX).forEach((key, versioned) ->
            repositories.put(key.substring(KEY_PREFIX.length()), (ObjectNode) parse(versioned.value())));
        return repositories;
    }

    /** Either the repositories to report, or the name to report missing. */
    record Resolution(NavigableMap<String, ObjectNode> found, String missing) {

        boolean failed() {
            return missing != null;
        }
    }

    /**
     * Resolves the {@code {repository}} placeholder for a <b>read</b>, following
     * {@code TransportGetRepositoriesAction}.
     *
     * <ul>
     *   <li>Absent, or exactly {@code _all} — everything, and a 200 even when there is nothing.</li>
     *   <li>No metadata at all — the first token reported missing verbatim. See
     *       {@link #METADATA_MARKER_KEY}.</li>
     *   <li>Otherwise — concrete names are collected as given and patterns contribute their matches, in
     *       request order; then the <b>first collected name that does not exist</b> is reported missing.
     *       A pattern that matches nothing contributes nothing and is therefore <em>not</em> an error,
     *       so an expression of only such patterns answers a bare {@code {}}.</li>
     * </ul>
     */
    Resolution resolveForRead(String expression) {
        if (expression == null || expression.isBlank() || ALL.equals(expression.trim())) {
            return new Resolution(all(), null);
        }
        if (hasMetadata() == false) {
            return new Resolution(null, expression.split(",")[0].trim());
        }

        NavigableMap<String, ObjectNode> everything = all();
        // Insertion order, so the first missing name is the one a real cluster would name.
        Set<String> wanted = new LinkedHashSet<>();
        for (String token : expression.split(",")) {
            String name = token.trim();
            if (name.isEmpty()) {
                continue;
            }
            if (name.indexOf('*') < 0) {
                wanted.add(name);
            } else {
                Pattern pattern = NameExpression.toPattern(name);
                everything.keySet().forEach(candidate -> {
                    if (pattern.matcher(candidate).matches()) {
                        wanted.add(candidate);
                    }
                });
            }
        }

        NavigableMap<String, ObjectNode> found = new TreeMap<>();
        for (String name : wanted) {
            ObjectNode definition = everything.get(name);
            if (definition == null) {
                return new Resolution(null, name);
            }
            found.put(name, definition);
        }
        return new Resolution(found, null);
    }

    /**
     * Resolves it for a <b>delete</b>, which follows different rules —
     * {@code RepositoriesService.unregisterRepository}.
     *
     * <p>The expression is a <b>single glob</b>, not a comma-separated list: commas are part of the name.
     * Everything matching is deleted. When nothing matches, the exact pattern {@code *} is a no-op 200
     * — the source says so in as many words, "we use a wildcard so we don't barf if it's not present" —
     * and anything else is a 404 naming the expression verbatim.
     *
     * <p>Match-all exempt, partial wildcards not: the same rule as deleting an ingest pipeline, where
     * {@code *} is a 200 and {@code logs-*} matching nothing is a 404.
     */
    Resolution resolveForDelete(String expression) {
        String glob = expression == null ? "" : expression.trim();
        NavigableMap<String, ObjectNode> matched = new TreeMap<>();
        Pattern pattern = NameExpression.toPattern(glob);
        all().forEach((name, definition) -> {
            if (pattern.matcher(name).matches()) {
                matched.put(name, definition);
            }
        });
        if (matched.isEmpty() == false) {
            return new Resolution(matched, null);
        }
        return "*".equals(glob) ? new Resolution(matched, null) : new Resolution(null, glob);
    }

    boolean delete(String name) {
        return store.delete(key(name));
    }

    private static JsonNode parse(byte[] value) {
        try {
            return Json.parse(value);
        } catch (IOException e) {
            throw new UncheckedIOException("corrupt repository definition", e);
        }
    }
}
