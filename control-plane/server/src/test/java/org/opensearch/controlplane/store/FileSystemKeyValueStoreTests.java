/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSystemKeyValueStoreTests extends KeyValueStoreContractTests {

    @TempDir
    Path tempDir;

    private int storeCounter;

    @Override
    protected KeyValueStore newStore() {
        // A fresh subdirectory per call, since the contract tests expect an empty store each time.
        return new FileSystemKeyValueStore(tempDir.resolve("store-" + storeCounter++));
    }

    private static List<String> fileNames(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    // ------------------------------------------------------------------
    // durability
    // ------------------------------------------------------------------

    @Test
    void dataSurvivesReopening() {
        Path directory = tempDir.resolve("persistent");
        long revision;
        {
            KeyValueStore store = new FileSystemKeyValueStore(directory);
            revision = store.put("ingest/pipeline/p", bytes("payload"));
        }

        KeyValueStore reopened = new FileSystemKeyValueStore(directory);
        assertEquals(Optional.of("payload"), asString(reopened, "ingest/pipeline/p"));
        assertEquals(revision, reopened.get("ingest/pipeline/p").orElseThrow().revision(),
            "revisions must survive a restart, or conditional writes break across one");
    }

    @Test
    void revisionsKeepAdvancingAfterReopening() {
        Path directory = tempDir.resolve("advancing");
        long first = new FileSystemKeyValueStore(directory).put("a", bytes("1"));
        long second = new FileSystemKeyValueStore(directory).put("a", bytes("2"));

        assertTrue(second > first, "a reopened store must not reuse a revision");
    }

    @Test
    void conditionalWriteWorksAcrossAReopen() {
        Path directory = tempDir.resolve("cas");
        long revision = new FileSystemKeyValueStore(directory).put("a", bytes("1"));

        KeyValueStore reopened = new FileSystemKeyValueStore(directory);
        reopened.put("a", bytes("2"), revision);
        assertEquals(Optional.of("2"), asString(reopened, "a"));
        assertThrows(RevisionConflictException.class, () -> reopened.put("a", bytes("3"), revision));
    }

    @Test
    void createsTheDirectoryIfMissing() {
        Path nested = tempDir.resolve("a/b/c");
        assertFalse(Files.exists(nested));

        new FileSystemKeyValueStore(nested).put("k", bytes("v"));
        assertTrue(Files.isDirectory(nested));
    }

    // ------------------------------------------------------------------
    // key encoding -- the security-relevant part
    // ------------------------------------------------------------------

    @Test
    void keysBecomeFlatEncodedFilenamesWithNoNesting() throws IOException {
        Path directory = tempDir.resolve("layout");
        KeyValueStore store = new FileSystemKeyValueStore(directory);
        store.put("tenant/acme/ingest/pipeline/p", bytes("v"));

        assertEquals(
            List.of("tenant%2Facme%2Fingest%2Fpipeline%2Fp"),
            fileNames(directory),
            "the key must not become a directory tree"
        );
    }

    /**
     * The traversal case the {@code KeyValueStore} javadoc warns about. Encoding {@code .} and
     * {@code /} means a key can never address a path outside the store directory.
     */
    @Test
    void traversalSequencesCannotEscapeTheDirectory() throws IOException {
        Path directory = tempDir.resolve("escape");
        Path sibling = tempDir.resolve("sibling");
        Files.createDirectories(sibling);
        Files.writeString(sibling.resolve("secret"), "do not touch");

        KeyValueStore store = new FileSystemKeyValueStore(directory);
        store.put("../sibling/secret", bytes("overwritten"));

        assertEquals("do not touch", Files.readString(sibling.resolve("secret")), "the sibling file must be untouched");
        assertEquals(Optional.of("overwritten"), asString(store, "../sibling/secret"));
        assertEquals(List.of("%2E%2E%2Fsibling%2Fsecret"), fileNames(directory));
    }

    @Test
    void encodingRoundTripsAwkwardKeys() {
        for (String key : List.of("a", "a/b", "..", ".", "a+b", "a b", "%2F", "ünïcodé", "A-Z_0-9")) {
            assertEquals(key, FileSystemKeyValueStore.decode(FileSystemKeyValueStore.encode(key)), key);
        }
    }

    @Test
    void alphanumericKeysAreLeftReadable() {
        assertEquals("simple-key_1", FileSystemKeyValueStore.encode("simple-key_1"));
    }

    @Test
    void anOverlongKeyFailsLoudlyRatherThanTruncating() {
        KeyValueStore store = newStore();
        String tooLong = "k".repeat(FileSystemKeyValueStore.MAX_ENCODED_KEY_LENGTH + 1);

        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> store.put(tooLong, bytes("v"))
        );
        assertTrue(e.getMessage().contains("filename budget"));
    }

    @Test
    void aKeyOfNonAsciiCharactersHitsTheBudgetSooner() {
        // Percent-encoding triples multi-byte characters, so the usable key length is shorter.
        KeyValueStore store = newStore();
        assertThrows(IllegalArgumentException.class, () -> store.put("é".repeat(41), bytes("v")));
    }

    // ------------------------------------------------------------------
    // on-disk hygiene
    // ------------------------------------------------------------------

    @Test
    void temporaryFilesAreNotLeftBehindOrScanned() throws IOException {
        Path directory = tempDir.resolve("temps");
        KeyValueStore store = new FileSystemKeyValueStore(directory);
        store.put("a", bytes("1"));
        store.put("a", bytes("2"));
        store.put("b", bytes("3"));

        assertTrue(fileNames(directory).stream().noneMatch(n -> n.startsWith(".tmp-")), "no temp files should remain");
        assertEquals(2, store.scanPrefix("").size());
    }

    @Test
    void aStrayTemporaryFileIsIgnoredByScans() throws IOException {
        Path directory = tempDir.resolve("stray");
        KeyValueStore store = new FileSystemKeyValueStore(directory);
        store.put("a", bytes("1"));
        // Simulate a crash mid-write.
        Files.writeString(directory.resolve(".tmp-leftover"), "garbage");

        assertEquals(List.of("a"), List.copyOf(store.scanPrefix("").keySet()));
    }

    @Test
    void deleteRemovesTheFile() throws IOException {
        Path directory = tempDir.resolve("deletes");
        KeyValueStore store = new FileSystemKeyValueStore(directory);
        store.put("a", bytes("1"));
        assertEquals(1, fileNames(directory).size());

        assertTrue(store.delete("a"));
        assertEquals(List.of(), fileNames(directory));
    }

    @Test
    void aTruncatedFileIsReportedRatherThanSilentlyRead() throws IOException {
        Path directory = tempDir.resolve("truncated");
        KeyValueStore store = new FileSystemKeyValueStore(directory);
        store.put("a", bytes("1"));

        // Shorter than the revision header.
        Files.write(directory.resolve("a"), new byte[] { 1, 2, 3 });

        assertThrows(java.io.UncheckedIOException.class, () -> store.get("a"));
    }

    @Test
    void theRevisionHeaderIsNotPartOfThePayload() throws IOException {
        Path directory = tempDir.resolve("framing");
        KeyValueStore store = new FileSystemKeyValueStore(directory);
        store.put("a", bytes("payload"));

        byte[] raw = Files.readAllBytes(directory.resolve("a"));
        assertEquals(Long.BYTES + "payload".length(), raw.length);
        assertEquals("payload", new String(raw, Long.BYTES, raw.length - Long.BYTES, StandardCharsets.UTF_8));
    }
}
