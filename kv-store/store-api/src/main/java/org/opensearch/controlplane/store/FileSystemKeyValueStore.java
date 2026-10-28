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
 * <p>A key whose encoding is longer than a filename can be — {@link #MAX_ENCODED_KEY_LENGTH} — is stored
 * under {@code %%} and the SHA-256 of the key instead, and its file carries the key itself between the
 * revision and the payload: a 4-byte length, then the UTF-8 bytes. No encoded key can start {@code %%},
 * because the encoding only ever writes {@code %} before two hex digits, so the two kinds of name cannot
 * collide. A scan has to open a hashed file to learn its key; keys long enough to need one are rare.
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
 *   <li><b>Long keys cost a file read to scan</b>, since a hashed filename does not say which key it
 *       holds. See the layout above.</li>
 * </ul>
 */
public final class FileSystemKeyValueStore implements KeyValueStore {

    /**
     * Cap on the encoded filename length. 255 bytes is the common filesystem limit; the margin
     * leaves room for the temporary-file prefix. Percent-encoding can triple a key's length, so the
     * usable key length is smaller for keys with many non-alphanumeric characters.
     */
    static final int MAX_ENCODED_KEY_LENGTH = 240;

    /** Starts the filename of a key too long to encode into one. See the class javadoc. */
    static final String HASHED_PREFIX = "%%";

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
                // Anything that is not a readable store file contributes nothing, and must not stop the store
                // opening. A directory shares this one with whatever else ends up in it -- a subdirectory, an
                // editor's backup, a test fixture's stray file -- and refusing to construct over one would make the
                // store fail on exactly the untidiness it has no opinion about. Found by a data-plane test that puts
                // an extra file in a store directory on purpose, which the old per-key scheme never had to look at.
                try {
                    recovered = Math.max(recovered, readStored(file).map(stored -> stored.versioned().revision()).orElse(ABSENT));
                } catch (RuntimeException unreadable) {
                    // Swallowed deliberately: the counter file above is the authority in the ordinary case, and a
                    // key file this cannot read is one that get() will fail on just as loudly when asked for it.
                }
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
            return read(key);
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
                if (fileName.startsWith(HASHED_PREFIX)) {
                    // The name says nothing about the key, so the file has to be opened to find out.
                    readStored(entry).filter(stored -> stored.key().startsWith(keyPrefix))
                        .ifPresent(stored -> matched.put(stored.key(), stored.versioned()));
                    continue;
                }
                String key = decode(fileName);
                if (key.startsWith(keyPrefix) == false) {
                    continue;
                }
                // A concurrent delete between listing and reading is not an error.
                readStored(entry).ifPresent(stored -> matched.put(key, stored.versioned()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not scan " + directory, e);
        } finally {
            lock.unlock();
        }
        return matched;
    }

    /** What one file holds. {@code key} is null for an encoded filename, which carries the key in its name. */
    private record Stored(String key, Versioned versioned) {}

    private Optional<Versioned> read(String key) {
        Path file = pathFor(key);
        return readStored(file).map(stored -> {
            if (stored.key() != null && stored.key().equals(key) == false) {
                // Two keys with one SHA-256. Not expected in the lifetime of the universe, but not a reason to
                // hand back another key's value either.
                throw new IllegalStateException("hashed store file " + file + " holds [" + stored.key()
                    + "], not [" + key + "]");
            }
            return stored.versioned();
        });
    }

    private Optional<Stored> readStored(Path file) {
        try {
            byte[] contents = Files.readAllBytes(file);
            if (contents.length < REVISION_HEADER_BYTES) {
                throw new IOException("truncated store file " + file + " (" + contents.length + " bytes)");
            }
            ByteBuffer buffer = ByteBuffer.wrap(contents);
            long revision = buffer.getLong();
            String key = null;
            if (file.getFileName().toString().startsWith(HASHED_PREFIX)) {
                if (buffer.remaining() < Integer.BYTES) {
                    throw new IOException("truncated store file " + file + " (no key length)");
                }
                int keyLength = buffer.getInt();
                if (keyLength < 0 || keyLength > buffer.remaining()) {
                    throw new IOException("truncated store file " + file + " (key of " + keyLength + " bytes)");
                }
                byte[] keyBytes = new byte[keyLength];
                buffer.get(keyBytes);
                key = new String(keyBytes, StandardCharsets.UTF_8);
            }
            byte[] value = new byte[buffer.remaining()];
            buffer.get(value);
            return Optional.of(new Stored(key, new Versioned(value, revision)));
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
        StoreLimits.checkWrite(key, value);
        lock.lock();
        try {
            return write(key, value, nextRevision());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        StoreLimits.checkWrite(key, value);
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
        return read(key).map(Versioned::revision).orElse(ABSENT);
    }

    /** Write-to-temp, fsync, atomic-move, so a reader never sees a partial value. */
    private long write(String key, byte[] value, long revision) {
        Path target = pathFor(key);
        Path temp = null;
        try {
            temp = Files.createTempFile(directory, TEMP_PREFIX, null);
            try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.allocate(REVISION_HEADER_BYTES).putLong(revision).flip());
                if (target.getFileName().toString().startsWith(HASHED_PREFIX)) {
                    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                    channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(keyBytes.length).flip());
                    channel.write(ByteBuffer.wrap(keyBytes));
                }
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
        return directory.resolve(fileNameFor(Objects.requireNonNull(key, "key")));
    }

    /** The encoded key, or {@link #HASHED_PREFIX} and its SHA-256 when the encoding is too long for a filename. */
    static String fileNameFor(String key) {
        String encoded = encode(key);
        if (encoded.length() <= MAX_ENCODED_KEY_LENGTH) {
            return encoded;
        }
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8));
            return HASHED_PREFIX + java.util.HexFormat.of().formatHex(digest);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new AssertionError("every JDK provides SHA-256", e);
        }
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
