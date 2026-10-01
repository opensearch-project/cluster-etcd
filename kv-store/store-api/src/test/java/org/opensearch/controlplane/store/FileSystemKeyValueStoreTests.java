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

    /**
     * The files that represent keys, which is what every caller of this actually means.
     *
     * <p>Dot-prefixed names are the store's own bookkeeping — {@code .revision}, the counter that stops a revision
     * being reused after a key is deleted, and {@code .tmp-} files mid-write. Percent-encoding means no real key
     * can produce a leading dot, so filtering them cannot hide one.
     */
    private static List<String> fileNames(Path directory) throws IOException {
        try (Stream<Path> entries = Files.list(directory)) {
            return entries.map(p -> p.getFileName().toString()).filter(name -> name.startsWith(".") == false).sorted().toList();
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

    /**
     * A key the interface allows but whose encoding is too long for a filename. Production keys reach about
     * 400 bytes, and every {@code /} encodes to three characters, so this is not hypothetical.
     */
    @Test
    void aKeyTooLongToEncodeIsStoredUnderItsHash() throws IOException {
        Path directory = tempDir.resolve("hashed");
        KeyValueStore store = new FileSystemKeyValueStore(directory);
        String key = "tenant/" + "t".repeat(128) + "/indices/" + "i".repeat(255);
        assertTrue(FileSystemKeyValueStore.encode(key).length() > FileSystemKeyValueStore.MAX_ENCODED_KEY_LENGTH);

        long revision = store.put(key, bytes("v"));
        store.put("tenant/short", bytes("s"));

        List<String> names = fileNames(directory);
        assertEquals(2, names.size(), names.toString());
        assertTrue(names.stream().anyMatch(name -> name.startsWith(FileSystemKeyValueStore.HASHED_PREFIX)), names.toString());

        assertEquals(Optional.of("v"), asString(store, key));
        assertEquals(List.of("tenant/short", key), List.copyOf(store.scanPrefix("tenant/").keySet()));
        assertEquals(revision, store.scanPrefix("tenant/" + "t".repeat(10)).get(key).revision());
        assertTrue(store.scanPrefix("tenant/x").isEmpty(), "a hashed key must still be filtered by prefix");

        FileSystemKeyValueStore reopened = new FileSystemKeyValueStore(directory);
        assertEquals(Optional.of("v"), asString(reopened, key));
        assertTrue(reopened.put("other", bytes("o")) > revision, "a hashed key's revision counts towards recovery");
        assertTrue(reopened.delete(key, revision));
        assertEquals(Optional.empty(), reopened.get(key));
    }

    /** Percent-encoding triples multi-byte characters, so a non-ASCII key needs a hash sooner. */
    @Test
    void aNonAsciiKeyWithinTheLimitRoundTrips() {
        KeyValueStore store = newStore();
        String key = "é".repeat(KeyValueStore.MAX_KEY_BYTES / 2);

        store.put(key, bytes("v"));
        assertEquals(Optional.of("v"), asString(store, key));
        assertEquals(List.of(key), List.copyOf(store.scanPrefix("é").keySet()));
    }

    /**
     * Deleting whichever key holds the highest revision must not let a later write reissue it, which is
     * exactly what recovering the counter by scanning the surviving keys would do.
     *
     * <p>Specific to this store, not the contract: the interface promises nothing across keys, but this
     * store's per-key guarantee rests on one store-wide counter, and this is how that counter breaks.
     */
    @Test
    void aDeletedKeysRevisionIsNotHandedToAnotherWrite() {
        Path directory = tempDir.resolve("counter");
        KeyValueStore store = new FileSystemKeyValueStore(directory);
        store.put("a", bytes("one"));
        long highest = store.put("b", bytes("two"));
        assertTrue(store.delete("b"));

        long next = new FileSystemKeyValueStore(directory).put("b", bytes("three"));
        assertTrue(next > highest, "the counter must survive the delete of the key carrying it");
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
    void anUnreadableExtraFileDoesNotStopTheStoreOpening() throws IOException {
        // The revision counter is recovered by scanning the directory on construction, so construction now looks at
        // files it did not write. A directory shares space with whatever ends up in it, and refusing to open over a
        // subdirectory or a stray file would make the store fail on untidiness it has no opinion about. Caught by a
        // data-plane test that puts an extra file in a store directory on purpose.
        Path directory = tempDir.resolve("untidy");
        KeyValueStore first = new FileSystemKeyValueStore(directory);
        long revision = first.put("a", bytes("one"));

        Files.writeString(directory.resolve("not-a-store-file"), "x");
        Files.createDirectory(directory.resolve("a-subdirectory"));

        KeyValueStore reopened = new FileSystemKeyValueStore(directory);
        assertEquals(Optional.of("one"), asString(reopened, "a"));
        assertTrue(reopened.put("b", bytes("two")) > revision, "and revisions still move forward");
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
