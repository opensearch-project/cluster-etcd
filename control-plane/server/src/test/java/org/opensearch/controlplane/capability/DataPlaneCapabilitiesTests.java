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
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataPlaneCapabilitiesTests {

    @TempDir
    Path tempDir;

    private Path write(String yaml) throws IOException {
        Path file = tempDir.resolve("capabilities-" + yaml.hashCode() + ".yaml");
        Files.writeString(file, yaml);
        return file;
    }

    // ------------------------------------------------------------------
    // permissive
    // ------------------------------------------------------------------

    @Test
    void permissiveAcceptsAnythingAndSaysSo() {
        DataPlaneCapabilities capabilities = new PermissiveCapabilities();

        assertFalse(capabilities.isEnforcing());
        assertTrue(capabilities.supportsProcessor(ProcessorKind.INGEST, "anything-at-all"));
        assertTrue(capabilities.supportsProcessor(ProcessorKind.SEARCH_PHASE_RESULTS, "normalization-processor"));
        assertTrue(capabilities.toString().contains("permissive"), "startup output should be unambiguous");
    }

    // ------------------------------------------------------------------
    // declared
    // ------------------------------------------------------------------

    @Test
    void declaredEnforcesPerKind() {
        DataPlaneCapabilities capabilities = new DeclaredCapabilities(Map.of(
            ProcessorKind.INGEST, Set.of("append", "drop"),
            ProcessorKind.SEARCH_RESPONSE, Set.of("sort")
        ));

        assertTrue(capabilities.isEnforcing());
        assertTrue(capabilities.supportsProcessor(ProcessorKind.INGEST, "append"));
        assertFalse(capabilities.supportsProcessor(ProcessorKind.INGEST, "normalization-processor"));
        // Kinds are separate namespaces: sort is a response processor, not an ingest one.
        assertTrue(capabilities.supportsProcessor(ProcessorKind.SEARCH_RESPONSE, "sort"));
        assertFalse(capabilities.supportsProcessor(ProcessorKind.INGEST, "sort"));
    }

    /**
     * An omitted kind means "none", not "unconstrained". A cluster with no plugins genuinely reports
     * {@code phase_results_processors: []}, so absent and empty have to mean the same thing for the
     * file to be able to express that at all.
     */
    @Test
    void anOmittedKindIsEmptyRatherThanUnconstrained() {
        DataPlaneCapabilities capabilities =
            new DeclaredCapabilities(Map.of(ProcessorKind.INGEST, Set.of("append")));

        assertEquals(Set.of(), capabilities.supportedProcessors(ProcessorKind.SEARCH_PHASE_RESULTS));
        assertFalse(capabilities.supportsProcessor(ProcessorKind.SEARCH_PHASE_RESULTS, "normalization-processor"));
    }

    @Test
    void loadsAGeneratedFile() throws IOException {
        // Exactly the shape tools/dump-capabilities.sh emits, including an empty kind.
        Path file = write("""
            processors:
              ingest:
                - append
                - drop
              search_request:
                - filter_query
              search_response:
                - sort
              search_phase_results: []
            """);

        DataPlaneCapabilities capabilities = DeclaredCapabilities.load(file);
        assertTrue(capabilities.supportsProcessor(ProcessorKind.INGEST, "drop"));
        assertTrue(capabilities.supportsProcessor(ProcessorKind.SEARCH_REQUEST, "filter_query"));
        assertEquals(Set.of(), capabilities.supportedProcessors(ProcessorKind.SEARCH_PHASE_RESULTS));
        assertTrue(capabilities.toString().contains("enforcing"));
    }

    @Test
    void loadRejectsAFileWithNoProcessorsMapping() throws IOException {
        Path file = write("something_else: true\n");

        IOException e = assertThrows(IOException.class, () -> DeclaredCapabilities.load(file));
        assertTrue(e.getMessage().contains("dump-capabilities.sh"), "the error should say how to fix it");
    }

    @Test
    void loadRejectsAnUnknownKind() throws IOException {
        Path file = write("processors:\n  not_a_kind: [x]\n");

        assertThrows(IllegalArgumentException.class, () -> DeclaredCapabilities.load(file));
    }

    @Test
    void loadRejectsANonListKind() throws IOException {
        Path file = write("processors:\n  ingest: append\n");

        assertThrows(IOException.class, () -> DeclaredCapabilities.load(file));
    }

    @Test
    void configKeysRoundTrip() {
        for (ProcessorKind kind : ProcessorKind.values()) {
            assertEquals(kind, ProcessorKind.fromConfigKey(kind.configKey()));
        }
        assertThrows(IllegalArgumentException.class, () -> ProcessorKind.fromConfigKey("nope"));
    }
}
