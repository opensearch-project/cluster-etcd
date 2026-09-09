/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Resolving a {@code --store} URI through the provider seam.
 *
 * <p>This is the whole of what makes a backend pluggable, so the failure paths matter as much as the
 * happy one: an operator who misspells a scheme, or forgets the backend jar, should be told which
 * schemes this deployment actually has rather than getting a stack trace.
 */
class KeyValueStoresTests {

    @Test
    void opensTheFilesystemStoreFromAnAbsoluteFileUri(@TempDir Path dir) throws Exception {
        KeyValueStore store = KeyValueStores.open(dir.toUri().toString());

        assertInstanceOf(FileSystemKeyValueStore.class, store);
        store.put("a", "one".getBytes(StandardCharsets.UTF_8));
        assertEquals(Optional.of("one"),
            store.get("a").map(v -> new String(v.value(), StandardCharsets.UTF_8)));
    }

    /** {@code file:relative/path} has a null URI path, and means the obvious thing anyway. */
    @Test
    void opensTheFilesystemStoreFromARelativeFileUri(@TempDir Path dir) throws Exception {
        KeyValueStore store = KeyValueStores.open("file:" + dir.resolve("nested"));

        assertInstanceOf(FileSystemKeyValueStore.class, store);
        store.put("a", new byte[0]);
        assertTrue(store.get("a").isPresent());
    }

    /**
     * {@code file:} alone does not even parse — {@code URI} wants a scheme-specific part — so it is
     * reported as a malformed URI rather than reaching the provider. The provider keeps a blank-path
     * guard anyway, but this is the message an operator actually gets.
     */
    @Test
    void aSchemeWithNothingAfterItIsRejected() {
        IOException e = assertThrows(IOException.class, () -> KeyValueStores.open("file:"));

        assertTrue(e.getMessage().contains("not a valid URI"), e.getMessage());
        assertTrue(e.getMessage().contains("scheme-specific part"), e.getMessage());
    }

    /** The message has to name what IS available, since the likely cause is a missing jar. */
    @Test
    void anUnknownSchemeListsTheSchemesThatExist() {
        IOException e = assertThrows(IOException.class,
            () -> KeyValueStores.open("etcd://localhost:2379/cp"));

        assertTrue(e.getMessage().contains("no store provider for scheme [etcd]"), e.getMessage());
        assertTrue(e.getMessage().contains("file:///path/to/dir"), e.getMessage());
        assertTrue(e.getMessage().contains("jar on the classpath"), e.getMessage());
    }

    @Test
    void aBareDirectoryIsNotAStoreUri() {
        IOException e = assertThrows(IOException.class, () -> KeyValueStores.open("/var/lib/cp"));

        assertTrue(e.getMessage().contains("has no scheme"), e.getMessage());
    }

    @Test
    void aMalformedUriSaysSo() {
        IOException e = assertThrows(IOException.class, () -> KeyValueStores.open("file://a b/c"));

        assertTrue(e.getMessage().contains("not a valid URI"), e.getMessage());
    }

    /** The scheme is matched case-insensitively; URI schemes are not case-sensitive. */
    @Test
    void theSchemeIsCaseInsensitive(@TempDir Path dir) throws Exception {
        assertInstanceOf(FileSystemKeyValueStore.class,
            KeyValueStores.open("FILE://" + dir.toAbsolutePath()));
    }

    @Test
    void discoveryFindsTheBundledProvider() throws Exception {
        assertTrue(KeyValueStores.discover().containsKey("file"),
            "the file provider ships with the SPI and must always be found: "
                + KeyValueStores.discover().keySet());
    }

    /** Shown in --help, so it must read as a list of options rather than a class dump. */
    @Test
    void theAvailableDescriptionListsUsageLines() {
        String description = KeyValueStores.describeAvailable();

        assertTrue(description.startsWith("available stores:"), description);
        assertTrue(description.contains("file:///path/to/dir"), description);
    }
}
