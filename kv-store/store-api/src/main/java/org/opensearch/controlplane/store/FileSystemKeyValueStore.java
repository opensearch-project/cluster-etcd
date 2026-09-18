/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link KeyValueStore} backed by a directory on the local filesystem.
 *
 * <p>Intended for local development, single-node deployments, and integration tests. Production is
 * expected to use etcd or DynamoDB; see the limitations below for why.
 *
 * <h2>Layout</h2>
 *
 * <p>One flat directory, one file per key, with the filename being a strict percent-encoding of the
 * key. Keys are deliberately <em>not</em> mapped onto nested directories. That would be the obvious
 * design and it is a trap: keys carry user input (index names, pipeline IDs), so a key segment of
 * {@code ..} would escape the directory, and with tenancy the key prefix is the only thing keeping
 * tenants apart. Encoding everything outside {@code [A-Za-z0-9_-]} means {@code ..} becomes
 * {@code %2E%2E} and {@code /} becomes {@code %2F} — ordinary filenames with no traversal
 * possible, and no exposure to case-insensitive or reserved-name behaviour either.
 *
 * <p>Each file is an 8-byte big-endian revision followed by the payload.
 *
 * <h2>Durability</h2>
 *
 * <p>Writes go to a temporary file in the same directory, are fsynced, and are then moved into place
 * with {@link StandardCopyOption#ATOMIC_MOVE}. A reader therefore never observes a half-written
 * value. The directory itself is fsynced on a best-effort basis — the JDK cannot portably fsync a
 * directory, so after a power loss a completed rename may still be missing even though the file
 * contents were durable.
 *
 * <h2>Limitations</h2>
 *
 * <ul>
 *   <li><b>Compare-and-set is process-local.</b> Conditional writes are serialised by an in-process
 *       lock, so two control plane instances sharing one directory will race and lose updates. One
 *       process per directory, or use a store that offers real CAS.</li>
 *   <li><b>{@link #scanPrefix} is O(number of keys).</b> It lists the directory and decodes every
 *       name. Fine for control plane volumes; not a substitute for an index.</li>
 *   <li><b>Key length is bounded</b> by the filesystem's filename limit — see
 *       {@link #MAX_ENCODED_KEY_LENGTH}. Exceeding it fails loudly rather than truncating.</li>
 * </ul>
 */
public final class FileSystemKeyValueStore implements KeyValueStore {

    /**
     * Cap on the encoded filename length. 255 bytes is the common filesystem limit; the margin
     * leaves room for the temporary-file prefix. Percent-encoding can triple a key's length, so the
     * usable key length is smaller for keys with many non-alphanumeric characters.
     */
    static final int MAX_ENCODED_KEY_LENGTH = 240;

    private static final int REVISION_HEADER_BYTES = Long.BYTES;
    private static final String TEMP_PREFIX = ".tmp-";

    /**
     * Where the last issued revision is kept, so that a revision is never reused after a key is deleted.
     *
     * <p>Named with a leading dot and skipped when scanning: it is not a key, and percent-encoding means no real
     * key can ever encode to this name.
     */
    private static final String REVISION_COUNTER_FILE = ".revision";

    /** Serialises conditional writes. Process-local; see the class javadoc. */
    private final ReentrantLock lock = new ReentrantLock();
    private final Path directory;

    /**
     * The last revision this store issued, store-wide rather than per key.
     *
     * <p>It used to be derived per key, from the file itself — {@code currentRevision(key) + 1} — which needed no
     * counter to persist, justified on the grounds that the interface makes revisions opaque and comparable only
     * for equality. That is exactly why it was wrong: <b>equality is how a reader detects change</b>, so a key
     * deleted and recreated came back at revision 1 having already been seen at revision 1 with different content,
     * and a reader caching on the revision kept the stale value for ever. It cost a real debugging session — a
     * supervisor whose desired state froze on a deleted index's uuid, while the node correctly declined to build it.
     *
     * <p>Both other backends already satisfied the guarantee: etcd's {@code mod_revision} and ZooKeeper's
     * {@code mzxid} are cluster-wide transaction ids, and {@code ZooKeeperKeyValueStore} rejects
     * {@code Stat.getVersion()} for precisely this reason. This was the only backend that reasoned its way to a
     * per-key counter, and the interface did not say it could not.
     */
    private long lastIssuedRevision;

    public FileSystemKeyValueStore(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory").toAbsolutePath();
        try {
            Files.createDirectories(this.directory);
        } catch (IOException e) {
            throw new UncheckedIOException("could not create store directory " + this.directory, e);
        }
        this.lastIssuedRevision = recoverLastIssuedRevision();
    }

    /**
     * The floor for the next revision after a restart: the counter file, or the highest revision any surviving key
     * carries, whichever is greater.
     *
     * <p>Taking the maximum rather than trusting the counter makes a lost or truncated counter file survivable —
     * the keys are then the floor, which is the answer the old per-key scheme gave. Taking the maximum rather than
     * trusting the keys is what closes the case this exists for, where the key carrying the highest revision is
     * the one that was deleted.
     */
    private long recoverLastIssuedRevision() {
        long recovered = ABSENT;
        Path counter = directory.resolve(REVISION_COUNTER_FILE);
        try {
            if (Files.exists(counter)) {
                byte[] contents = Files.readAllBytes(counter);
                if (contents.length >= REVISION_HEADER_BYTES) {
                    recovered = ByteBuffer.wrap(contents, 0, REVISION_HEADER_BYTES).getLong();
                }
            }
        } catch (IOException e) {
            // Best effort. The keys below are a safe floor, so an unreadable counter costs only the case it was
            // added for, and failing construction over it would be worse.
            recovered = ABSENT;
        }
        try (java.util.stream.Stream<Path> entries = Files.list(directory)) {
            for (Path file : entries.toList()) {
                String name = file.getFileName().toString();
                if (name.startsWith(TEMP_PREFIX) || name.equals(REVISION_COUNTER_FILE)) {
                    continue;
                }
                recovered = Math.max(recovered, read(file).map(Versioned::revision).orElse(ABSENT));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not scan " + directory + " to recover the revision counter", e);
        }
        return recovered;
    }

    /**
     * The next revision, made durable before it is used.
     *
     * <p>The ordering matters: the counter is fsynced <em>before</em> the value that will carry the revision, so a
     * crash between the two leaves the counter ahead rather than behind. Ahead costs a gap in the numbers, which
     * the interface explicitly permits — revisions are not dense — while behind would reissue one, which is the
     * whole thing this prevents.
     */
    private long nextRevision() {
        assert lock.isHeldByCurrentThread();
        long next = lastIssuedRevision + 1;
        Path counter = directory.resolve(REVISION_COUNTER_FILE);
        Path temp = null;
        try {
            temp = Files.createTempFile(directory, TEMP_PREFIX, null);
            try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.allocate(REVISION_HEADER_BYTES).putLong(next).flip());
                channel.force(true);
            }
            Files.move(temp, counter, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            temp = null;
            syncDirectory();
        } catch (IOException e) {
            throw new UncheckedIOException("could not record the revision counter in " + directory, e);
        } finally {
            if (temp != null) {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException suppressed) {
                    // Inert, and named so that a scan skips it.
                }
            }
        }
        lastIssuedRevision = next;
        return next;
    }

    public Path directory() {
        return directory;
    }

    // ------------------------------------------------------------------
    // reads
    // ------------------------------------------------------------------

    @Override
    public Optional<Versioned> get(String key) {
        lock.lock();
        try {
            return read(pathFor(key));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        NavigableMap<String, Versioned> matched = new TreeMap<>();
        lock.lock();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            for (Path entry : entries) {
                String fileName = entry.getFileName().toString();
                if (fileName.startsWith(TEMP_PREFIX) || fileName.equals(REVISION_COUNTER_FILE)) {
                    continue;
                }
                String key = decode(fileName);
                if (key.startsWith(keyPrefix) == false) {
                    continue;
                }
                // A concurrent delete between listing and reading is not an error.
                read(entry).ifPresent(versioned -> matched.put(key, versioned));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not scan " + directory, e);
        } finally {
            lock.unlock();
        }
        return matched;
    }

    private Optional<Versioned> read(Path file) {
        try {
            byte[] contents = Files.readAllBytes(file);
            if (contents.length < REVISION_HEADER_BYTES) {
                throw new IOException("truncated store file " + file + " (" + contents.length + " bytes)");
            }
            long revision = ByteBuffer.wrap(contents, 0, REVISION_HEADER_BYTES).getLong();
            byte[] value = new byte[contents.length - REVISION_HEADER_BYTES];
            System.arraycopy(contents, REVISION_HEADER_BYTES, value, 0, value.length);
            return Optional.of(new Versioned(value, revision));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new UncheckedIOException("could not read " + file, e);
        }
    }

    // ------------------------------------------------------------------
    // writes
    // ------------------------------------------------------------------

    @Override
    public long put(String key, byte[] value) {
        lock.lock();
        try {
            return write(key, value, nextRevision());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        lock.lock();
        try {
            long actual = currentRevision(key);
            if (actual != expectedRevision) {
                throw new RevisionConflictException(key, expectedRevision, actual);
            }
            return write(key, value, nextRevision());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean delete(String key) {
        lock.lock();
        try {
            return Files.deleteIfExists(pathFor(key));
        } catch (IOException e) {
            throw new UncheckedIOException("could not delete " + key, e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean delete(String key, long expectedRevision) {
        lock.lock();
        try {
            long actual = currentRevision(key);
            if (actual != expectedRevision) {
                throw new RevisionConflictException(key, expectedRevision, actual);
            }
            return Files.deleteIfExists(pathFor(key));
        } catch (IOException e) {
            throw new UncheckedIOException("could not delete " + key, e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * What a key is currently at, for the compare in a conditional write, or {@link #ABSENT} if it is not there.
     *
     * <p>Read from the key rather than from the counter, because this answers "what is this key at" and not "what
     * was issued last". The revision it returns is not a basis for the <em>next</em> one — see
     * {@link #nextRevision()} for why that is store-wide.
     */
    private long currentRevision(String key) {
        return read(pathFor(key)).map(Versioned::revision).orElse(ABSENT);
    }

    /** Write-to-temp, fsync, atomic-move, so a reader never sees a partial value. */
    private long write(String key, byte[] value, long revision) {
        Objects.requireNonNull(value, "value");
        Path target = pathFor(key);
        Path temp = null;
        try {
            temp = Files.createTempFile(directory, TEMP_PREFIX, null);
            try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.allocate(REVISION_HEADER_BYTES).putLong(revision).flip());
                channel.write(ByteBuffer.wrap(value));
                channel.force(true);
            }
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            temp = null;
            syncDirectory();
            return revision;
        } catch (IOException e) {
            throw new UncheckedIOException("could not write " + key, e);
        } finally {
            if (temp != null) {
                try {
                    Files.deleteIfExists(temp);
                } catch (IOException suppressed) {
                    // Leaving a .tmp- file behind is harmless: scans skip them.
                }
            }
        }
    }

    /**
     * Best effort. The JDK offers no portable way to fsync a directory, and opening one as a
     * {@link FileChannel} fails on some platforms, so a failure here is not fatal — it only means the
     * rename may not survive a power loss.
     */
    private void syncDirectory() {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException | RuntimeException e) {
            // Intentionally ignored; see the javadoc.
        }
    }

    // ------------------------------------------------------------------
    // key <-> filename
    // ------------------------------------------------------------------

    private Path pathFor(String key) {
        return directory.resolve(encode(Objects.requireNonNull(key, "key")));
    }

    /**
     * Percent-encodes everything outside {@code [A-Za-z0-9_-]}, so the result can never contain a
     * path separator, {@code .}, or {@code ..}.
     */
    static String encode(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        StringBuilder encoded = new StringBuilder(bytes.length + 16);
        for (byte b : bytes) {
            int c = b & 0xFF;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
                encoded.append((char) c);
            } else {
                encoded.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16)))
                       .append(Character.toUpperCase(Character.forDigit(c & 0x0F, 16)));
            }
        }
        if (encoded.length() > MAX_ENCODED_KEY_LENGTH) {
            throw new IllegalArgumentException(
                "key [" + key + "] encodes to " + encoded.length() + " characters, exceeding the "
                    + MAX_ENCODED_KEY_LENGTH + "-character filename budget of this store"
            );
        }
        return encoded.toString();
    }

    static String decode(String fileName) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(fileName.length());
        for (int i = 0; i < fileName.length(); i++) {
            char c = fileName.charAt(i);
            if (c == '%' && i + 2 < fileName.length()) {
                int high = Character.digit(fileName.charAt(i + 1), 16);
                int low = Character.digit(fileName.charAt(i + 2), 16);
                if (high >= 0 && low >= 0) {
                    out.write((high << 4) + low);
                    i += 2;
                    continue;
                }
            }
            out.write(c);
        }
        return out.toString(StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return "FileSystemKeyValueStore[" + directory + "]";
    }
}
