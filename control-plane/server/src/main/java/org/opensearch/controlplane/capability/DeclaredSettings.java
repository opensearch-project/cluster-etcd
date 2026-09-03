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
 * A settings registry read from the file the deployer supplies, generated from their own cluster.
 *
 * <p>Folded into the same file as {@link DeclaredCapabilities} rather than carried in a second one:
 * two snapshots are two things to refresh after an upgrade, and the instruction "remember to refresh
 * both" is the kind that gets half-followed.
 *
 * <pre>{@code
 * settings:
 *   index:
 *     - {key: "index.number_of_shards",              kind: exact,  properties: [Final, IndexScope]}
 *     - {key: "index.number_of_replicas",            kind: exact,  properties: [Dynamic, IndexScope]}
 *     - {key: "index.analysis.",                     kind: prefix, properties: [IndexScope]}
 *     - {key: "index.routing.allocation.include.*",  kind: affix,  properties: [Dynamic, IndexScope]}
 *     - {key: "index.uuid",                          kind: exact,  properties: [IndexScope, PrivateIndex]}
 *   cluster:
 *     - {key: "cluster.max_shards_per_node",         kind: exact,  properties: [Dynamic, NodeScope]}
 * }</pre>
 *
 * <h2>Matching</h2>
 *
 * <p>Three entry kinds, because the registry genuinely has three and collapsing them would reject
 * valid settings:
 *
 * <ul>
 *   <li><b>{@code exact}</b> — the key and nothing else.</li>
 *   <li><b>{@code prefix}</b> — any key beginning with it. {@code index.analysis.} admits
 *       {@code index.analysis.analyzer.my.tokenizer} and everything else beneath it.</li>
 *   <li><b>{@code affix}</b> — a pattern whose {@code *} stands for one namespace segment, so
 *       {@code index.routing.allocation.include.*} admits
 *       {@code index.routing.allocation.include._name}.</li>
 * </ul>
 *
 * <p>Exact keys are matched first and by hash lookup; only a miss falls through to the patterns, of
 * which there are a handful. Settings are validated per request, so the common case should not be a
 * scan.
 *
 * <h2>Updateability</h2>
 *
 * <p>Derived from the {@code properties} list, which carries OpenSearch's {@code Setting.Property}
 * enum verbatim. {@code InternalIndex} or {@code PrivateIndex} means a user may not supply the setting
 * at all and outranks the rest; otherwise {@code Dynamic} is dynamic and anything else — {@code Final}
 * included, and equally the absence of {@code Dynamic} — is fixed once set.
 *
 * <p>Note "not {@code Dynamic}" rather than "{@code Final}": a setting with neither property is static,
 * and treating only explicit {@code Final} as immutable would let most static settings through.
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

    private final Map<Scope, Map<String, Entry>> exact;
    private final Map<Scope, List<Entry>> patterns;

    private DeclaredSettings(Map<Scope, Map<String, Entry>> exact, Map<Scope, List<Entry>> patterns) {
        this.exact = exact;
        this.patterns = patterns;
    }

    /**
     * Reads the {@code settings} section of a capabilities file.
     *
     * @return empty when the file has no {@code settings} section, so a capabilities file predating
     *         this feature still loads and simply does not enforce settings
     */
    public static Optional<DeclaredSettings> load(Path file) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode root = mapper.readTree(file.toFile());
        if (root == null || root.isObject() == false) {
            throw new IOException("capabilities file " + file + " is not a YAML mapping");
        }
        JsonNode settings = root.path("settings");
        if (settings.isObject() == false) {
            return Optional.empty();
        }

        Map<Scope, Map<String, Entry>> exact = new EnumMap<>(Scope.class);
        Map<Scope, List<Entry>> patterns = new EnumMap<>(Scope.class);
        for (Scope scope : Scope.values()) {
            exact.put(scope, new HashMap<>());
            patterns.put(scope, new ArrayList<>());

            JsonNode entries = settings.path(scope.name().toLowerCase(Locale.ROOT));
            if (entries.isMissingNode() || entries.isNull()) {
                continue;
            }
            if (entries.isArray() == false) {
                throw new IOException("settings." + scope.name().toLowerCase(Locale.ROOT)
                    + " must be a list of entries in " + file);
            }
            for (JsonNode entry : entries) {
                read(file, scope, entry, exact, patterns);
            }
        }
        return Optional.of(new DeclaredSettings(exact, patterns));
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
        for (JsonNode property : properties) {
            String name = property.asText();
            if (name.equals("InternalIndex") || name.equals("PrivateIndex")) {
                // Outranks everything else: the user may not supply this at all.
                return Updateability.NOT_USER_SETTABLE;
            }
            if (name.equals("Dynamic")) {
                dynamic = true;
            }
        }
        return dynamic ? Updateability.DYNAMIC : Updateability.FINAL;
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
    public Set<String> declaredKeys(Scope scope) {
        Set<String> keys = new TreeSet<>(exact.get(scope).keySet());
        patterns.get(scope).forEach(entry -> keys.add(entry.key()));
        return keys;
    }

    @Override
    public String toString() {
        return "enforcing ("
            + Arrays.stream(Scope.values())
                .map(scope -> scope.name().toLowerCase(Locale.ROOT) + "=" + declaredKeys(scope).size())
                .collect(Collectors.joining(", "))
            + " settings declared)";
    }
}
