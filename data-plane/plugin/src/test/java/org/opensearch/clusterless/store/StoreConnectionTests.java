/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.store;

import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opening the store, and — more usefully — failing to.
 *
 * <p>D6.2 asks that a missing backend jar be distinguishable from a typo in the URI. That is not a nicety:
 * a backend is a separate jar an operator installs, so those two causes produce the same symptom and lead
 * to completely different actions. Every assertion here is about the text an operator will read at 3am.
 */
class StoreConnectionTests {

    @Test
    void aStoreCanBeOpenedThroughTheSpiWithoutNamingABackend() throws IOException {
        // The filesystem backend ships with store-api, so this exercises real ServiceLoader discovery
        // rather than a stub -- and it does it without this repository mentioning any implementation.
        Path directory = Files.createTempDirectory("clusterless-store-test");
        try (KeyValueStore store = StoreConnection.open(directory.toUri().toString())) {
            store.put("nodes/heartbeat/n1", new byte[] { 1 });
            assertTrue(store.get("nodes/heartbeat/n1").isPresent());
        }
    }

    @Test
    void anUnknownSchemeSaysWhatIsInstalledAndHowToAddMore() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> StoreConnection.open("nosuchbackend://somewhere:1234/root")
        );

        assertTrue(e.getMessage().contains("nosuchbackend"), e.getMessage());
        // The list is what distinguishes a typo from a missing jar...
        assertTrue(e.getMessage().contains("Installed store backends:"), e.getMessage());
        // ...and this is what stops a short list reading as "those backends do not exist".
        assertTrue(e.getMessage().contains("found by ServiceLoader"), e.getMessage());
        assertTrue(e.getMessage().contains("plugin's installation directory"), e.getMessage());
    }

    @Test
    void aUriWithNoSchemeAtAllIsAlsoExplained() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> StoreConnection.open("/var/lib/clusterless"));
        assertTrue(e.getMessage().contains("Installed store backends:"), e.getMessage());
    }

    @Test
    void whatIsInstalledCanBeAskedForWithoutOpeningAnything() {
        // The runtime half of the diagnostic D6.2 asks for; ./gradlew :plugin:schemes prints the same text
        // for a build, which is where somebody packaging a deployment will look.
        String described = StoreConnection.describeInstalled();
        assertTrue(described.contains("Installed store backends:"), described);
    }
}
