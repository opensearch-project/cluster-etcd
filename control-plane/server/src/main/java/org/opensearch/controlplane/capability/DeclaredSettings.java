/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A settings registry read from a file, generated from the cluster this control plane fronts.
 *
 * <p>{@code GET /_cluster/settings_registry} emits it directly, so the whole workflow is a curl:
 *
 * <pre>{@code
 * curl 'localhost:9200/_cluster/settings_registry?pretty' -o settings_registry.yaml
 * ../gradlew :server:run --args="--settings-registry settings_registry.yaml"
 * }</pre>
 *
 * <p>A real cluster reports 221 index settings and 801 cluster settings, so the file is around 180 kB.
 *
 * <pre>{@code
 * version: "3.9.0"
 * index:
 *   - {key: "index.number_of_shards",           kind: exact,  properties: [Final, IndexScope]}
 *   - {key: "index.number_of_replicas",         kind: exact,  properties: [Dynamic, IndexScope]}
 *   - {key: "index.analysis.",                  kind: prefix, properties: [IndexScope]}
 *   - {key: "index.uuid",                       kind: exact,  properties: [IndexScope, PrivateIndex]}
 * cluster:
 *   - {key: "cluster.max_shards_per_node",      kind: exact,  properties: [Dynamic, NodeScope]}
 *   - {key: "cluster.remote.*.seeds",           kind: affix,  properties: [Dynamic, NodeScope]}
 * }</pre>
 *
 * <h2>Matching</h2>
 *
 * <p>Three entry kinds, because the registry genuinely has three and collapsing them would reject
 * valid settings:
 *
 * <ul>
 *   <li><b>{@code exact}</b> — the key and nothing else. 213 of the 221 index settings.</li>
 *   <li><b>{@code prefix}</b> — any key beginning with it. {@code index.analysis.} admits
 *       {@code index.analysis.analyzer.my.tokenizer} and everything else beneath it. All 8 of the
 *       non-exact index settings are this kind, including the three
 *       {@code index.routing.allocation.*} groups.</li>
 *   <li><b>{@code affix}</b> — a pattern whose {@code *} stands for one namespace segment, so
 *       {@code cluster.remote.*.seeds} admits {@code cluster.remote.other_cluster.seeds}. Only cluster
 *       settings use these, 45 of them.</li>
 * </ul>
 *
 * <p>The emitter guarantees an invariant this relies on, and it was checked against a real registry of
 * 1022 entries: <b>an {@code affix} key always contains a {@code *}, and a {@code prefix} key never
 * does and always ends with a {@code .}</b> So the two never have to be told apart by inspection.
 *
 * <p>Exact keys are matched first and by hash lookup; only a miss falls through to the patterns, of
 * which there are a few dozen. Settings are validated per request, so the common case is not a scan.
 *
 * <h2>Updateability</h2>
 *
 * <p>Derived from the {@code properties} list, which carries OpenSearch's {@code Setting.Property}
 * enum verbatim, and it is a <b>four-way</b> split rather than a two-way one:
 *
 * <ul>
 *   <li>{@code InternalIndex} or {@code PrivateIndex} — the user may not supply it at all. Outranks
 *       the rest.</li>
 *   <li>{@code Dynamic} — changeable at any time.</li>
 *   <li>{@code Final} — never changeable, not even on a closed index.</li>
 *   <li>neither — <b>static</b>: changeable only while the index is closed.</li>
 * </ul>
 *
 * <p>The last two are easy to collapse and must not be. Verified: {@code index.codec} has neither
 * property and is accepted on a closed index, while {@code index.number_of_shards} is {@code Final} and
 * is refused even then. Treating "not {@code Dynamic}" as final would refuse 45 of the 221 index
 * settings that a real cluster allows on a closed index.
 */
public final class DeclaredSettings implements SettingsRegistry {

    /** One registry entry: how to match it, and what it permits. */
    private record Entry(String key, Pattern pattern, Updateability updateability) {

        static Entry exact(String key, Updateability updateability) {
            return new Entry(key, null, updateability);
        }

        static Entry pattern(String key, Pattern pattern, Updateability updateability) {
            return new Entry(key, pattern, updateability);
        }
    }

    private final String version;
    private final Map<Scope, Map<String, Entry>> exact;
    private final Map<Scope, List<Entry>> patterns;

    private DeclaredSettings(String version, Map<Scope, Map<String, Entry>> exact, Map<Scope, List<Entry>> patterns) {
        this.version = version;
        this.exact = exact;
        this.patterns = patterns;
    }

    /**
     * The OpenSearch version the registry was generated from, or {@code null} if the file did not say.
     *
     * <p>Worth surfacing at startup: the registry is a point-in-time snapshot, and a file generated
     * against a different build than the one being fronted is the failure mode this mechanism has.
     */
    public String version() {
        return version;
    }

    /**
     * Reads a settings registry, in either of the two shapes it arrives in.
     *
     * <p>The endpoint emits a standalone document with the scopes at the top level, so
     * {@code curl .../_cluster/settings_registry -o registry.yaml} produces a file this reads directly
     * with no tooling in between:
     *
     * <pre>{@code
     * version: "3.9.0"
     * index:   [ ... ]
     * cluster: [ ... ]
     * }</pre>
     *
     * <p>The same lists are also honoured nested under {@code settings:} inside a capabilities file, for
     * a deployment that would rather carry one file than two.
     *
     * @return empty when the document has neither shape, so a capabilities file predating this feature
     *         still loads and simply does not enforce settings
     */
    public static Optional<DeclaredSettings> load(Path file) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode root = mapper.readTree(file.toFile());
        if (root == null || root.isObject() == false) {
            throw new IOException("settings registry " + file + " is not a YAML mapping");
        }
        // A capabilities file nests the scopes under `settings`; the endpoint's own document does not.
        JsonNode settings = root.path("settings").isObject() ? root.get("settings") : root;
        if (hasAnyScope(settings) == false) {
            return Optional.empty();
        }

        Map<Scope, Map<String, Entry>> exact = new EnumMap<>(Scope.class);
        Map<Scope, List<Entry>> patterns = new EnumMap<>(Scope.class);
        for (Scope scope : Scope.values()) {
            exact.put(scope, new HashMap<>());
            patterns.put(scope, new ArrayList<>());

            JsonNode entries = settings.path(scopeKey(scope));
            if (entries.isMissingNode() || entries.isNull()) {
                continue;
            }
            if (entries.isArray() == false) {
                throw new IOException(scopeKey(scope) + " must be a list of entries in " + file);
            }
            for (JsonNode entry : entries) {
                read(file, scope, entry, exact, patterns);
            }
        }
        return Optional.of(new DeclaredSettings(root.path("version").asText(null), exact, patterns));
    }

    private static boolean hasAnyScope(JsonNode node) {
        for (Scope scope : Scope.values()) {
            if (node.path(scopeKey(scope)).isArray()) {
                return true;
            }
        }
        return false;
    }

    private static String scopeKey(Scope scope) {
        return scope.name().toLowerCase(Locale.ROOT);
    }

    private static void read(
        Path file,
        Scope scope,
        JsonNode entry,
        Map<Scope, Map<String, Entry>> exact,
        Map<Scope, List<Entry>> patterns
    ) throws IOException {
        String key = entry.path("key").asText(null);
        if (key == null || key.isBlank()) {
            throw new IOException("a settings entry in " + file + " has no `key`");
        }
        String kind = entry.path("kind").asText("exact");
        Updateability updateability = updateabilityOf(entry.path("properties"));

        switch (kind) {
            case "exact" -> exact.get(scope).put(key, Entry.exact(key, updateability));
            case "prefix" -> patterns.get(scope)
                .add(Entry.pattern(key, Pattern.compile(Pattern.quote(key) + ".+"), updateability));
            case "affix" -> patterns.get(scope).add(
                Entry.pattern(key, affixPattern(key), updateability));
            default -> throw new IOException(
                "settings entry [" + key + "] in " + file + " has unknown kind [" + kind + "]; "
                    + "expected exact, prefix or affix"
            );
        }
    }

    /**
     * An affix key as a regex, {@code *} standing for one namespace.
     *
     * <p>The namespace expression is OpenSearch's own, from {@code Setting.AffixKey}: word characters
     * and hyphens, dot-separated. Everything around the holes is quoted, so a key full of dots is
     * matched literally rather than as any-character.
     */
    private static Pattern affixPattern(String key) {
        return Pattern.compile(
            Arrays.stream(key.split("\\*", -1))
                .map(Pattern::quote)
                .collect(Collectors.joining("(?:[-\\w]+[.])*[-\\w]+"))
        );
    }

    /** {@code Setting.Property} values, read the way OpenSearch reads them. */
    private static Updateability updateabilityOf(JsonNode properties) {
        boolean dynamic = false;
        boolean isFinal = false;
        for (JsonNode property : properties) {
            String name = property.asText();
            if (name.equals("InternalIndex") || name.equals("PrivateIndex")) {
                // Outranks everything else: the user may not supply this at all.
                return Updateability.NOT_USER_SETTABLE;
            }
            if (name.equals("Dynamic")) {
                dynamic = true;
            } else if (name.equals("Final")) {
                isFinal = true;
            }
        }
        if (dynamic) {
            return Updateability.DYNAMIC;
        }
        // Final means never; the absence of both means "only while closed". Collapsing the two would
        // wrongly refuse a static setting on a closed index, which is the case indices.close exists for.
        return isFinal ? Updateability.FINAL : Updateability.STATIC;
    }

    @Override
    public boolean isKnown(Scope scope, String key) {
        return find(scope, key) != null;
    }

    @Override
    public Updateability updateabilityOf(Scope scope, String key) {
        Entry entry = find(scope, key);
        // An unknown key is rejected by isKnown before updateability is ever consulted; answering FINAL
        // rather than throwing keeps a caller that checks in the other order from silently permitting.
        return entry == null ? Updateability.FINAL : entry.updateability();
    }

    private Entry find(Scope scope, String key) {
        Entry direct = exact.get(scope).get(key);
        if (direct != null) {
            return direct;
        }
        for (Entry entry : patterns.get(scope)) {
            if (entry.pattern().matcher(key).matches()) {
                return entry;
            }
        }
        return null;
    }

    @Override
    public boolean isEnforcing() {
        return true;
    }

    @Override
    public Set<String> exactKeys(Scope scope) {
        return Set.copyOf(exact.get(scope).keySet());
    }

    @Override
    public Set<String> declaredKeys(Scope scope) {
        Set<String> keys = new TreeSet<>(exact.get(scope).keySet());
        patterns.get(scope).forEach(entry -> keys.add(entry.key()));
        return keys;
    }

    @Override
    public String toString() {
        return "enforcing ("
            + Arrays.stream(Scope.values())
                .map(scope -> scopeKey(scope) + "=" + declaredKeys(scope).size())
                .collect(Collectors.joining(", "))
            + " settings declared"
            + (version == null ? "" : ", generated from OpenSearch " + version)
            + ")";
    }
}
