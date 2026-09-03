/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.capability;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.controlplane.capability.SettingsRegistry.Scope;
import static org.opensearch.controlplane.capability.SettingsRegistry.Updateability;

/**
 * The settings registry, read from the format {@code GET /_cluster/settings_registry} emits.
 *
 * <p>The fixture is deliberately a real cross-section rather than a toy: an exact dynamic setting, an
 * exact static one, the {@code index.analysis.} group prefix that would break a keys-only registry, an
 * affix setting with the hole in the middle, and a private setting a user may not supply.
 */
class DeclaredSettingsTests {

    /**
     * The shape {@code GET /_cluster/settings_registry} emits: scopes at the top level, no wrapper.
     * A cross-section of the real thing rather than a toy — an exact dynamic setting, an exact static
     * one, one with no {@code Dynamic} property at all, the {@code index.analysis.} group prefix that
     * would break a keys-only registry, a cluster affix with the hole in the middle, and a private
     * setting a user may not supply.
     */
    private static final String FIXTURE = """
        version: "3.9.0"
        index:
          - {key: "index.number_of_shards",            kind: exact,  properties: [Final, IndexScope]}
          - {key: "index.number_of_replicas",          kind: exact,  properties: [Dynamic, IndexScope]}
          - {key: "index.codec",                       kind: exact,  properties: [IndexScope]}
          - {key: "index.analysis.",                   kind: prefix, properties: [IndexScope]}
          - {key: "index.routing.allocation.include.", kind: prefix, properties: [Dynamic, IndexScope]}
          - {key: "index.uuid",                        kind: exact,  properties: [IndexScope, PrivateIndex]}
        cluster:
          - {key: "cluster.max_shards_per_node",       kind: exact,  properties: [Dynamic, NodeScope]}
          - {key: "cluster.remote.*.seeds",            kind: affix,  properties: [Dynamic, NodeScope]}
        """;

    /** The same registry nested inside a capabilities file, which is also accepted. */
    private static final String FOLDED = """
        processors:
          ingest: [append]
        settings:
          index:
            - {key: "index.number_of_replicas", kind: exact, properties: [Dynamic, IndexScope]}
        """;

    private DeclaredSettings load(Path dir, String yaml) throws IOException {
        Path file = dir.resolve("capabilities.yaml");
        Files.writeString(file, yaml);
        return DeclaredSettings.load(file).orElseThrow();
    }

    @Test
    void matchesExactKeys(@TempDir Path dir) throws Exception {
        DeclaredSettings registry = load(dir, FIXTURE);

        assertTrue(registry.isKnown(Scope.INDEX, "index.number_of_shards"));
        assertTrue(registry.isKnown(Scope.CLUSTER, "cluster.max_shards_per_node"));
        assertFalse(registry.isKnown(Scope.INDEX, "index.totally_bogus"));
    }

    /** The scopes are separate registries and must not leak into one another. */
    @Test
    void scopesAreDistinct(@TempDir Path dir) throws Exception {
        DeclaredSettings registry = load(dir, FIXTURE);

        assertFalse(registry.isKnown(Scope.CLUSTER, "index.number_of_shards"));
        assertFalse(registry.isKnown(Scope.INDEX, "cluster.max_shards_per_node"));
    }

    /**
     * The case a keys-only registry would get wrong: {@code Setting.groupSetting("index.analysis.")}
     * admits anything beneath it, so every custom analyzer depends on this working.
     */
    @Test
    void aPrefixAdmitsEverythingBeneathIt(@TempDir Path dir) throws Exception {
        DeclaredSettings registry = load(dir, FIXTURE);

        assertTrue(registry.isKnown(Scope.INDEX, "index.analysis.analyzer.my_analyzer.tokenizer"));
        assertTrue(registry.isKnown(Scope.INDEX, "index.analysis.filter.my_stop.stopwords"));
        assertFalse(registry.isKnown(Scope.INDEX, "index.analysis."), "the prefix alone is not a setting");
        assertFalse(registry.isKnown(Scope.INDEX, "index.analysisX.something"), "the dot is literal");
    }

    @Test
    void anAffixMatchesOneNamespaceInTheMiddle(@TempDir Path dir) throws Exception {
        DeclaredSettings registry = load(dir, FIXTURE);

        assertTrue(registry.isKnown(Scope.CLUSTER, "cluster.remote.other_cluster.seeds"));
        assertFalse(registry.isKnown(Scope.CLUSTER, "cluster.remote..seeds"),
            "the pattern needs something in the hole");
        assertFalse(registry.isKnown(Scope.CLUSTER, "cluster.remote.other_cluster.mode"),
            "the suffix is literal");
    }

    /**
     * Routing allocation is a {@code prefix}, not an {@code affix} — it is built by
     * {@code Setting.prefixKeySetting}, whose key has no {@code *} and matches like a prefix. Pinned
     * because an earlier draft of the format had it the other way round.
     */
    @Test
    void routingAllocationIsAPrefix(@TempDir Path dir) throws Exception {
        DeclaredSettings registry = load(dir, FIXTURE);

        assertTrue(registry.isKnown(Scope.INDEX, "index.routing.allocation.include._name"));
        assertTrue(registry.isKnown(Scope.INDEX, "index.routing.allocation.include.rack"));
        assertFalse(registry.isKnown(Scope.INDEX, "index.routing.allocation.exclude._name"),
            "a different prefix, not declared here");
    }

    @Test
    void alsoReadsTheRegistryNestedInACapabilitiesFile(@TempDir Path dir) throws Exception {
        DeclaredSettings registry = load(dir, FOLDED);

        assertTrue(registry.isKnown(Scope.INDEX, "index.number_of_replicas"));
        assertFalse(registry.isKnown(Scope.INDEX, "index.totally_bogus"));
    }

    @Test
    void recordsTheVersionItWasGeneratedFrom(@TempDir Path dir) throws Exception {
        assertEquals("3.9.0", load(dir, FIXTURE).version());
        assertNull(load(dir, FOLDED).version(), "absent when the file did not say");
    }

    /**
     * Static means "not Dynamic", not "declared Final". Most static settings carry neither property,
     * so keying off Final alone would let them all through.
     */
    @Test
    void updateabilityComesFromTheProperties(@TempDir Path dir) throws Exception {
        DeclaredSettings registry = load(dir, FIXTURE);

        assertEquals(Updateability.DYNAMIC, registry.updateabilityOf(Scope.INDEX, "index.number_of_replicas"));
        assertEquals(Updateability.FINAL, registry.updateabilityOf(Scope.INDEX, "index.number_of_shards"));
        assertEquals(Updateability.FINAL, registry.updateabilityOf(Scope.INDEX, "index.codec"),
            "no Dynamic property means static, even without Final");
        assertEquals(Updateability.DYNAMIC,
            registry.updateabilityOf(Scope.INDEX, "index.routing.allocation.include._name"),
            "a pattern's updateability applies to everything it matches");
    }

    /** PrivateIndex and InternalIndex outrank everything: the user may not supply these at all. */
    @Test
    void privateSettingsAreNotUserSettable(@TempDir Path dir) throws Exception {
        DeclaredSettings registry = load(dir, FIXTURE);

        assertTrue(registry.isKnown(Scope.INDEX, "index.uuid"), "known, but not settable");
        assertEquals(Updateability.NOT_USER_SETTABLE, registry.updateabilityOf(Scope.INDEX, "index.uuid"));
    }

    /** An unknown key never reads as permissive, whichever order a caller checks in. */
    @Test
    void anUnknownKeyIsNotDynamic(@TempDir Path dir) throws Exception {
        DeclaredSettings registry = load(dir, FIXTURE);

        assertEquals(Updateability.FINAL, registry.updateabilityOf(Scope.INDEX, "index.totally_bogus"));
    }

    /** A capabilities file written before this feature existed must still load. */
    @Test
    void aFileWithNoSettingsSectionIsAbsentRatherThanEmpty(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("capabilities.yaml");
        Files.writeString(file, "processors:\n  ingest: [append]\n");

        assertEquals(Optional.empty(), DeclaredSettings.load(file),
            "absent, so the caller falls back to not enforcing rather than rejecting everything");
    }

    @Test
    void reportsWhatItDeclares(@TempDir Path dir) throws Exception {
        DeclaredSettings registry = load(dir, FIXTURE);

        assertTrue(registry.isEnforcing());
        assertEquals(6, registry.declaredKeys(Scope.INDEX).size());
        assertEquals(2, registry.declaredKeys(Scope.CLUSTER).size());
        assertTrue(registry.declaredKeys(Scope.INDEX).contains("index.analysis."), "patterns are listed too");
    }

    @Test
    void rejectsAMalformedFile(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("bad.yaml");

        Files.writeString(file, "index:\n  - {kind: exact, properties: []}\n");
        assertTrue(assertThrows(IOException.class, () -> DeclaredSettings.load(file))
            .getMessage().contains("no `key`"));

        Files.writeString(file, "index:\n  - {key: \"a\", kind: sideways}\n");
        assertTrue(assertThrows(IOException.class, () -> DeclaredSettings.load(file))
            .getMessage().contains("unknown kind [sideways]"));

        Files.writeString(file, "settings:\n  index: not-a-list\n");
        assertEquals(Optional.empty(), DeclaredSettings.load(file), "no usable scope list");
    }

    /** Nothing is declared, so nothing is enforced — the default, and startup says so. */
    @Test
    void thePermissiveDefaultAcceptsAnything() {
        SettingsRegistry registry = new PermissiveSettings();

        assertFalse(registry.isEnforcing());
        assertTrue(registry.isKnown(Scope.INDEX, "index.totally_bogus"));
        assertEquals(Updateability.DYNAMIC, registry.updateabilityOf(Scope.INDEX, "index.number_of_shards"));
        assertTrue(registry.declaredKeys(Scope.INDEX).isEmpty());
    }
}
