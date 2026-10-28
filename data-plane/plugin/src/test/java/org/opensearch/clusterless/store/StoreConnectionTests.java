/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.store;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

/**
 * Opening the store, and — more usefully — failing to.
 *
 * <p>D6.2 asks that a missing backend jar be distinguishable from a typo in the URI. That is not a nicety:
 * a backend is a separate jar an operator installs, so those two causes produce the same symptom and lead
 * to completely different actions. Every assertion here is about the text an operator will read at 3am.
 */
public class StoreConnectionTests extends OpenSearchTestCase {

    public void testAStoreCanBeOpenedThroughTheSpiWithoutNamingABackend() {
        // The filesystem backend ships with store-api, so this exercises real ServiceLoader discovery
        // rather than a stub -- and it does it without this repository mentioning any implementation.
        //
        // createTempDir() rather than Files.createTempDirectory: forbidden-APIs bans the JDK call, because
        // it leaves the directory behind for the operating system to deal with. LuceneTestCase's version is
        // registered with the test framework and removed when the suite finishes.
        Path directory = createTempDir();
        try (KeyValueStore store = StoreConnection.open(directory.toUri().toString())) {
            store.put("nodes/heartbeat/n1", new byte[] { 1 });
            assertTrue(store.get("nodes/heartbeat/n1").isPresent());
        }
    }

    public void testAnUnknownSchemeSaysWhatIsInstalledAndHowToAddMore() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> StoreConnection.open("nosuchbackend://somewhere:1234/root")
        );

        assertTrue(e.getMessage(), e.getMessage().contains("nosuchbackend"));
        // The list is what distinguishes a typo from a missing jar...
        assertTrue(e.getMessage(), e.getMessage().contains("Installed store backends:"));
        // ...and this is what stops a short list reading as "those backends do not exist".
        assertTrue(e.getMessage(), e.getMessage().contains("found by ServiceLoader"));
        assertTrue(e.getMessage(), e.getMessage().contains("plugin's installation directory"));
    }

    public void testAUriWithNoSchemeAtAllIsAlsoExplained() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> StoreConnection.open("/var/lib/clusterless"));
        assertTrue(e.getMessage(), e.getMessage().contains("Installed store backends:"));
    }

    public void testWhatIsInstalledCanBeAskedForWithoutOpeningAnything() {
        // The runtime half of the diagnostic D6.2 asks for; ../gradlew :plugin:schemes prints the same text
        // for a build, which is where somebody packaging a deployment will look.
        String described = StoreConnection.describeInstalled();
        assertTrue(described, described.contains("Installed store backends:"));
    }
}
