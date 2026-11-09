/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.OutcomeUnknownException;
import org.opensearch.controlplane.store.RevisionConflictException;
import org.opensearch.controlplane.store.StoreLimits;
import org.opensearch.controlplane.store.Watch;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

/**
 * A {@link KeyValueStore} backed by ZooKeeper, one znode per key segment.
 *
 * <p>See {@link ZooKeeperKeys} for the path mapping and why the hierarchy is kept where the filesystem
 * store flattens.
 *
 * <h2>Revisions are zxids, not znode versions</h2>
 *
 * <p>The obvious choice is {@code Stat.getVersion()}, and it is wrong twice over. It starts at
 * <b>zero</b>, which the interface reserves for {@link KeyValueStore#ABSENT}, so a freshly created key
 * would report itself absent. And it resets when a key is deleted and recreated, so a caller holding a
 * stale revision could compare-and-set successfully against a different value that happens to be at the
 * same version — a plain ABA bug.
 *
 * <p>So the revision is {@code Stat.getMzxid()}: the cluster-wide transaction id of the last
 * modification. Monotonic across the whole ensemble, never reused, and never zero for a node this store
 * created.
 *
 * <p>The catch is that ZooKeeper's conditional operations take a <em>version</em>, not a zxid. A
 * conditional write therefore reads the {@link Stat}, checks {@code mzxid} against what the caller
 * expected, and then writes with {@code stat.getVersion()}. The version check is what makes it atomic:
 * anything that changed the znode in between moves the version, and ZooKeeper rejects the write with
 * {@code BadVersion}. The zxid comparison supplies the semantics, the version comparison supplies the
 * atomicity. Costs one extra round trip per conditional write.
 *
 * <h2>A znode is not necessarily a key</h2>
 *
 * <p>A hierarchy needs intermediate nodes, and {@code indices/} is a parent rather than a key. Stored
 * values therefore carry a frame — {@link #FRAME_KEY}, then a 16-byte write id, then the payload — and
 * <b>a znode is a key if and only if its data starts with that byte</b>. Everything else — Curator's
 * parent znodes, a deleted key's tombstone — is structure.
 *
 * <p>That is what lets an <em>empty</em> value stay distinguishable from an absent key, which the store
 * contract requires because marker keys depend on it. A frame with no payload is an empty value; no frame
 * at all is not a key.
 *
 * <p>It also handles the case a flat layout never meets: deleting a key that has descendants.
 * {@code delete("a")} where {@code a/b} exists cannot remove the znode — ZooKeeper answers
 * {@code NotEmpty} — so the znode is left as structure instead. The key is gone by every observable
 * measure and the subtree survives.
 *
 * <p>Parents are created <b>persistent, not as containers</b>, though containers would tidy themselves
 * away. ZooKeeper reaps a container once its last child is gone, whatever data it holds, and setData does
 * not change a znode's mode. A key can be written at a path that is already a parent — ids are user input,
 * and {@code a/b} makes {@code a} a parent — and a container there would take that key with it when
 * {@code a/b} was deleted. The cost is that structure is never reaped: empty parents stay, bounded by the
 * distinct parent paths ever written, and every read already ignores them.
 *
 * <h2>A lost answer is settled by reading back, never by retrying</h2>
 *
 * <p>Curator retries an operation that fails with {@code ConnectionLoss}, and that is wrong for a
 * conditional write. The first attempt may have been applied with only its answer lost, and the retry then
 * meets the write it made itself: a create gets {@code NodeExists}, a versioned write {@code BadVersion}.
 * Reported as conflicts, a created index answers "already exists" and is never built, and a renewed lease
 * is given up by the process that holds it.
 *
 * <p>So the mutating step of a conditional write is attempted once, with Curator's retry turned off for
 * that call alone ({@link SingleAttempt}), and a lost answer is settled by reading the znode back:
 *
 * <ul>
 *   <li><b>It carries this write's id:</b> the write applied, and its {@code mzxid} is the revision.
 *       The id is random per write, so no other writer can have left it — unlike comparing bytes, which
 *       two writers racing to create the same empty marker would both pass.</li>
 *   <li><b>It is untouched</b> — still at the {@code mzxid} the write was conditioned on: the write never
 *       applied, so it is attempted again. Only a setData has such a witness; a create does not.</li>
 *   <li><b>Anything else</b> cannot be told apart from a write that applied and was then overwritten, so
 *       it is {@link OutcomeUnknownException}.</li>
 * </ul>
 *
 * <p>A conditional delete could not be settled that way, since a deleted znode carries nothing. Its
 * decisive step is therefore a versioned overwrite with a <b>tombstone</b> — {@link #FRAME_TOMBSTONE} and
 * the write id, which is structure rather than a key — and removing the znode afterwards is cleanup. The
 * cleanup is versioned against the tombstone, so it cannot remove a key written since, and a failure of it
 * leaves a structure znode a later put reuses. It costs a delete one more round trip.
 *
 * <p>Reads, and the unconditional writes, keep Curator's retry: repeating them is harmless.
 *
 * <h2>Limitations</h2>
 *
 * <ul>
 *   <li><b>{@link #scanPrefix} is bounded by {@code jute.maxbuffer}.</b> A scan lists one parent's
 *       children, so it is scoped rather than global, but a single parent with enough children to exceed
 *       the ensemble's buffer limit (1 MB by default) will fail. That is a property of ZooKeeper, not of
 *       this mapping; a keyspace where one prefix holds tens of thousands of entries wants etcd.</li>
 *   <li><b>Values are capped for the same reason</b>, and this backend's limit is the one that sets
 *       {@link KeyValueStore#MAX_VALUE_BYTES}: {@code jute.maxbuffer} bounds the whole request, path and
 *       frame included, so the interface promises 960 KiB rather than 1 MiB.</li>
 *   <li><b>Reads from another client may lag.</b> A follower serves reads from its own copy, and this store
 *       never calls {@code sync()}, so one process can briefly miss another's acknowledged write. That is
 *       exactly the latitude the interface's consistency rules allow; a session still sees its own writes
 *       in order.</li>
 *   <li><b>A {@link #watch} does not survive session expiry.</b> The persistent watch belongs to the
 *       ZooKeeper session and is not re-registered on a new one, so after an expiry it stops firing with
 *       nothing to indicate it. {@link Watch} permits missed notifications, but unlike etcd's this one
 *       does not recover; a consumer must keep polling rather than rely on it.</li>
 * </ul>
 */
public final class ZooKeeperKeyValueStore implements KeyValueStore {

    /**
     * Marks a znode's data as a stored value, followed by {@link #WRITE_ID_BYTES} of write id and the
     * payload. Also a format version: {@code 0x01} was the frame before write ids, and reads as structure.
     */
    static final byte FRAME_KEY = 0x02;

    /** Marks a deleted key's tombstone, followed by the deleting write's id. Structure, not a key. */
    static final byte FRAME_TOMBSTONE = 0x00;

    /** A random id per write, so a lost answer can be settled by reading back. See the class javadoc. */
    static final int WRITE_ID_BYTES = 16;

    private static final int FRAME_HEADER_BYTES = 1 + WRITE_ID_BYTES;

    private final CuratorFramework client;
    private final String root;
    private final boolean ownsClient;

    /** Test seam: a lost answer on demand. Nothing in production. */
    interface AttemptFaults {
        AttemptFaults NONE = new AttemptFaults() {};

        /** Before the mutating call: an answer lost before the request reached ZooKeeper. */
        default void beforeAttempt(String path) throws KeeperException {}

        /** After it succeeded: an answer lost after ZooKeeper applied the request. */
        default void afterAttempt(String path) throws KeeperException {}
    }

    volatile AttemptFaults faults = AttemptFaults.NONE;

    /**
     * @param client a started Curator client
     * @param root   the znode path all keys live beneath, e.g. {@code /controlplane}
     * @param ownsClient whether {@link #close()} should close the client
     */
    ZooKeeperKeyValueStore(CuratorFramework client, String root, boolean ownsClient) {
        this.client = Objects.requireNonNull(client, "client");
        this.root = Objects.requireNonNull(root, "root");
        this.ownsClient = ownsClient;
        SingleAttempt.install(client.getZookeeperClient());
    }

    @Override
    public Optional<Versioned> get(String key) {
        String path = pathOf(key);
        try {
            Stat stat = new Stat();
            byte[] framed = client.getData().storingStatIn(stat).forPath(path);
            return unframe(framed).map(value -> new Versioned(value, stat.getMzxid()));
        } catch (KeeperException.NoNodeException absent) {
            return Optional.empty();
        } catch (Exception e) {
            throw failure("get", key, e);
        }
    }

    @Override
    public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
        // A prefix is not necessarily a path boundary: "tenant/acme/x" has to match the key of that name
        // and anything beneath it, while not matching the sibling "tenant/acme/xyz"... which it does match,
        // textually, and must. So the parent is everything up to the last slash, and the remainder is a
        // filter on that parent's child names.
        int lastSlash = keyPrefix.lastIndexOf('/');
        String parentKey = lastSlash < 0 ? "" : keyPrefix.substring(0, lastSlash);
        String nameFilter = keyPrefix.substring(lastSlash + 1);
        String parentPath = lastSlash < 0 ? root : ZooKeeperKeys.pathOf(root, parentKey);

        NavigableMap<String, Versioned> found = new TreeMap<>();
        for (String childName : childrenOf(parentPath)) {
            String decoded = ZooKeeperKeys.decodeSegment(childName);
            if (decoded.startsWith(nameFilter) == false) {
                continue;
            }
            String childKey = parentKey.isEmpty() && lastSlash < 0 ? decoded : parentKey + "/" + decoded;
            collectSubtree(childKey, parentPath + "/" + childName, found);
        }
        return found;
    }

    /** Everything at or beneath one key, iteratively so a deep keyspace cannot overflow the stack. */
    private void collectSubtree(String key, String path, NavigableMap<String, Versioned> into) {
        Deque<String[]> pending = new ArrayDeque<>();
        pending.push(new String[] { key, path });
        while (pending.isEmpty() == false) {
            String[] current = pending.pop();
            String currentKey = current[0];
            String currentPath = current[1];

            try {
                Stat stat = new Stat();
                byte[] framed = client.getData().storingStatIn(stat).forPath(currentPath);
                unframe(framed).ifPresent(value -> into.put(currentKey, new Versioned(value, stat.getMzxid())));
            } catch (KeeperException.NoNodeException deletedMeanwhile) {
                continue;
            } catch (Exception e) {
                throw failure("scan", currentKey, e);
            }

            for (String childName : childrenOf(currentPath)) {
                pending.push(new String[] {
                    currentKey + "/" + ZooKeeperKeys.decodeSegment(childName),
                    currentPath + "/" + childName
                });
            }
        }
    }

    private List<String> childrenOf(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException absent) {
            return List.of();
        } catch (Exception e) {
            throw failure("list", path, e);
        }
    }

    @Override
    public long put(String key, byte[] value) {
        StoreLimits.checkWrite(key, value);
        String path = pathOf(key);
        byte[] framed = frame(newWriteId(), value);
        // Two writers can race between the create and the setData, so this retries rather than assuming
        // whichever branch it chose is still the right one.
        for (int attempt = 0; attempt < RACE_ATTEMPTS; attempt++) {
            try {
                Stat stat = client.checkExists().forPath(path);
                if (stat == null) {
                    Stat created = new Stat();
                    client.create().storingStatIn(created).creatingParentsIfNeeded()
                        .forPath(path, framed);
                    return created.getMzxid();
                }
                return client.setData().withVersion(stat.getVersion()).forPath(path, framed).getMzxid();
            } catch (KeeperException.NodeExistsException | KeeperException.BadVersionException raced) {
                continue;
            } catch (KeeperException.NoNodeException raced) {
                continue;
            } catch (Exception e) {
                throw failure("put", key, e);
            }
        }
        throw new IllegalStateException(
            "put [" + key + "] lost " + RACE_ATTEMPTS + " races in a row; something is writing it in a hot loop");
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        StoreLimits.checkWrite(key, value);
        String path = pathOf(key);
        byte[] writeId = newWriteId();
        byte[] framed = frame(writeId, value);
        try {
            // Data and stat from one read, so the frame check and the revision describe the same write.
            Stat stat = new Stat();
            byte[] current = dataAt(path, stat);

            if (current == null) {
                if (expectedRevision != ABSENT) {
                    throw new RevisionConflictException(key, expectedRevision, ABSENT);
                }
                try {
                    return settle("put", key, path, writeId, null, () -> {
                        Stat created = new Stat();
                        client.create().storingStatIn(created).creatingParentsIfNeeded()
                            .forPath(path, framed);
                        return created;
                    }).getMzxid();
                } catch (KeeperException.NodeExistsException raced) {
                    throw new RevisionConflictException(key, expectedRevision, revisionOf(path));
                }
            }

            // A znode that is only structure holds no key, so ABSENT is satisfied there and the frame goes on.
            long actual = isKey(current) ? stat.getMzxid() : ABSENT;
            if (actual != expectedRevision) {
                throw new RevisionConflictException(key, expectedRevision, actual);
            }
            return setDataChecked("put", key, path, writeId, framed, stat, expectedRevision).getMzxid();
        } catch (RevisionConflictException | OutcomeUnknownException settled) {
            throw settled;
        } catch (Exception e) {
            throw failure("put", key, e);
        }
    }

    /**
     * The write half of a conditional put or delete.
     *
     * <p>{@code withVersion} is what makes the whole operation atomic despite being read-then-write: if
     * anything touched the znode after the {@link Stat} was read, its version has moved and ZooKeeper
     * refuses. A refusal here is a lost race, which is the same outcome as a stale revision, and so is
     * the znode having been deleted meanwhile.
     */
    private Stat setDataChecked(String operation, String key, String path, byte[] writeId, byte[] data, Stat checked,
        long expectedRevision) throws Exception {
        try {
            return settle(operation, key, path, writeId, checked,
                () -> client.setData().withVersion(checked.getVersion()).forPath(path, data));
        } catch (KeeperException.BadVersionException | KeeperException.NoNodeException raced) {
            throw new RevisionConflictException(key, expectedRevision, revisionOf(path));
        }
    }

    /** One mutating call to ZooKeeper, returning the znode's stat after it. */
    @FunctionalInterface
    interface Mutation {
        Stat apply() throws Exception;
    }

    /**
     * Runs the mutating step of a conditional write without Curator's retry, and settles a lost answer by
     * reading the znode back. See the class javadoc for the three outcomes.
     *
     * @param checked the stat the write was conditioned on, or null for a create, which has none
     * @throws KeeperException whatever the attempt itself answered, such as {@code BadVersion}, for the caller
     *                         to turn into a conflict
     */
    private Stat settle(String operation, String key, String path, byte[] writeId, Stat checked, Mutation mutation)
        throws Exception {
        KeeperException lost = null;
        for (int attempt = 0; attempt < RACE_ATTEMPTS; attempt++) {
            try {
                return SingleAttempt.run(() -> {
                    faults.beforeAttempt(path);
                    Stat applied = mutation.apply();
                    faults.afterAttempt(path);
                    return applied;
                });
            } catch (KeeperException e) {
                if (answerLost(e) == false) {
                    throw e;
                }
                lost = e;
            }
            // Retried by Curator as usual, which is what waits out the reconnection.
            Stat now = new Stat();
            byte[] data = dataAt(path, now);
            if (carries(data, writeId)) {
                return now;
            }
            boolean untouched = checked != null && data != null && now.getMzxid() == checked.getMzxid();
            if (untouched == false) {
                throw new OutcomeUnknownException(
                    "ZooKeeper " + operation + " of [" + key + "] lost its answer, and [" + path
                        + "] has changed since, so whether it applied cannot be told", lost);
            }
        }
        throw new OutcomeUnknownException(
            "ZooKeeper " + operation + " of [" + key + "] lost its answer " + RACE_ATTEMPTS + " times in a row", lost);
    }

    /** The failures after which a request may or may not have been applied. */
    private static boolean answerLost(KeeperException e) {
        return e instanceof KeeperException.ConnectionLossException
            || e instanceof KeeperException.OperationTimeoutException
            || e instanceof KeeperException.SessionExpiredException
            || e instanceof KeeperException.SessionMovedException;
    }

    @Override
    public boolean delete(String key) {
        String path = pathOf(key);
        try {
            byte[] data = dataAt(path);
            if (isKey(data) == false) {
                return false;
            }
            removeOrUnframe(path);
            return true;
        } catch (KeeperException.NoNodeException absent) {
            return false;
        } catch (Exception e) {
            throw failure("delete", key, e);
        }
    }

    @Override
    public boolean delete(String key, long expectedRevision) {
        String path = pathOf(key);
        try {
            Stat stat = new Stat();
            byte[] current = dataAt(path, stat);

            if (isKey(current) == false) {
                if (expectedRevision != ABSENT) {
                    throw new RevisionConflictException(key, expectedRevision, ABSENT);
                }
                return false;
            }
            if (stat.getMzxid() != expectedRevision) {
                throw new RevisionConflictException(key, expectedRevision, stat.getMzxid());
            }
            byte[] writeId = newWriteId();
            Stat tombstoned = setDataChecked("delete", key, path, writeId, tombstone(writeId), stat, expectedRevision);
            removeTombstone(path, tombstoned.getVersion());
            return true;
        } catch (RevisionConflictException | OutcomeUnknownException settled) {
            throw settled;
        } catch (Exception e) {
            throw failure("delete", key, e);
        }
    }

    /**
     * Cleanup after a conditional delete, best effort. The key is already gone — a tombstone is structure — so
     * nothing here can fail the delete. Versioned against the tombstone, so a key put since is left alone, and a
     * znode with children stays as their parent.
     */
    private void removeTombstone(String path, int tombstoneVersion) {
        try {
            client.delete().withVersion(tombstoneVersion).forPath(path);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception leftAsStructure) {
            // NotEmpty, BadVersion, NoNode, or a lost connection: the znode stays as structure, which a later put
            // reuses and every read already ignores.
        }
    }

    /**
     * Removes a key, keeping any subtree beneath it.
     *
     * <p>ZooKeeper will not delete a znode with children, and a key can legitimately be the parent of
     * another key. Clearing the frame makes it structure again, which is indistinguishable from deleted
     * through this interface.
     */
    private void removeOrUnframe(String path) throws Exception {
        try {
            client.delete().forPath(path);
        } catch (KeeperException.NotEmptyException hasDescendants) {
            client.setData().forPath(path, new byte[0]);
        } catch (KeeperException.NoNodeException alreadyGone) {
            // Someone else removed it; the postcondition holds either way.
        }
    }

    private byte[] dataAt(String path) throws Exception {
        return dataAt(path, new Stat());
    }

    /** A znode's data, with its stat stored in {@code stat}, or null if it does not exist. */
    private byte[] dataAt(String path, Stat stat) throws Exception {
        try {
            return client.getData().storingStatIn(stat).forPath(path);
        } catch (KeeperException.NoNodeException absent) {
            return null;
        }
    }

    /** The key's revision, or {@link #ABSENT} for a znode that is only structure. */
    private long revisionOf(String path) throws Exception {
        try {
            Stat stat = new Stat();
            return isKey(client.getData().storingStatIn(stat).forPath(path)) ? stat.getMzxid() : ABSENT;
        } catch (KeeperException.NoNodeException absent) {
            return ABSENT;
        }
    }

    /**
     * The znode for a key.
     *
     * <p>The empty key is not special: it has one empty segment, which encodes to {@code %}, so it lands
     * at {@code <root>/%} like any other key rather than at the root itself.
     */
    private String pathOf(String key) {
        return ZooKeeperKeys.pathOf(root, key);
    }

    private static boolean isKey(byte[] framed) {
        return framed != null && framed.length >= FRAME_HEADER_BYTES && framed[0] == FRAME_KEY;
    }

    private static byte[] newWriteId() {
        UUID id = UUID.randomUUID();
        return ByteBuffer.allocate(WRITE_ID_BYTES)
            .putLong(id.getMostSignificantBits())
            .putLong(id.getLeastSignificantBits())
            .array();
    }

    private static byte[] frame(byte[] writeId, byte[] value) {
        byte[] framed = new byte[FRAME_HEADER_BYTES + value.length];
        framed[0] = FRAME_KEY;
        System.arraycopy(writeId, 0, framed, 1, WRITE_ID_BYTES);
        System.arraycopy(value, 0, framed, FRAME_HEADER_BYTES, value.length);
        return framed;
    }

    private static byte[] tombstone(byte[] writeId) {
        byte[] data = new byte[FRAME_HEADER_BYTES];
        data[0] = FRAME_TOMBSTONE;
        System.arraycopy(writeId, 0, data, 1, WRITE_ID_BYTES);
        return data;
    }

    /** Whether a znode's data, key or tombstone, was left by the write with this id. */
    private static boolean carries(byte[] data, byte[] writeId) {
        return data != null && data.length >= FRAME_HEADER_BYTES
            && Arrays.equals(data, 1, FRAME_HEADER_BYTES, writeId, 0, WRITE_ID_BYTES);
    }

    private static Optional<byte[]> unframe(byte[] framed) {
        if (isKey(framed) == false) {
            return Optional.empty();
        }
        return Optional.of(Arrays.copyOfRange(framed, FRAME_HEADER_BYTES, framed.length));
    }

    private static RuntimeException failure(String operation, String key, Exception cause) {
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new IllegalStateException(
            "ZooKeeper " + operation + " of [" + key + "] failed: " + cause, cause);
    }

    /**
     * A persistent recursive watch, which is exactly the shape {@link Watch} describes.
     *
     * <p>ZooKeeper 3.6 added {@code addWatch} with {@link AddWatchMode#PERSISTENT_RECURSIVE}: one
     * registration covers a znode and every descendant, and it does not need re-arming after firing. Before
     * that, watching a subtree meant a watch per node plus re-registration on every event, which is the
     * reason ZooKeeper watches have a reputation for being unusable — that reputation is out of date.
     *
     * <p>Two details make this fit the contract rather than fight it:
     *
     * <ul>
     *   <li><b>Prefixes need not be segment-aligned</b>, but a watch can only be placed on a path. So the
     *       prefix is rounded <em>down</em> to its last complete segment and the watch is placed there.
     *       {@code tenant/acme/x} therefore also fires for {@code tenant/acme/xyz} and its siblings —
     *       which is legal, because {@link Watch} explicitly permits spurious notifications. Over-notifying
     *       is a performance question; under-notifying would be a correctness one.</li>
     *   <li><b>The path need not exist.</b> Unlike a one-shot watch, {@code addWatch} accepts a znode that
     *       has never been created and fires when it is — so a watch on a prefix nothing has written yet is
     *       granted, and no znode is created as a side effect to make it possible. This was expected to be
     *       the other way round and a test asserting a decline is what proved otherwise; it is now asserted
     *       in the direction ZooKeeper actually behaves.</li>
     * </ul>
     */
    @Override
    public Optional<Watch> watch(String keyPrefix, Runnable onChange) {
        int lastSlash = keyPrefix.lastIndexOf('/');
        String alignedPrefix = lastSlash < 0 ? "" : keyPrefix.substring(0, lastSlash);
        String path = alignedPrefix.isEmpty() ? root : ZooKeeperKeys.pathOf(root, alignedPrefix);

        // Any event under the path is the same signal: re-read. The event carries a type and a path and
        // both are deliberately ignored -- see Watch on why the payload is withheld.
        Watcher watcher = event -> onChange.run();
        try {
            client.watchers().add()
                .withMode(AddWatchMode.PERSISTENT_RECURSIVE)
                .usingWatcher(watcher)
                .forPath(path);
        } catch (Exception e) {
            // Empty rather than thrown, so watchOrPoll degrades to polling instead of failing a consumer
            // that would have been happy either way. A watch is an optimisation; not getting one is not an
            // error the caller can act on.
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return Optional.empty();
        }

        return Optional.of(() -> {
            try {
                client.watchers().remove(watcher).ofType(Watcher.WatcherType.Any).forPath(path);
            } catch (Exception ignored) {
                // Closing a watch is best effort: the session may already be gone, in which case
                // ZooKeeper has forgotten the watch anyway. Nothing a caller could do about it.
            }
        });
    }

    @Override
    public void close() {
        if (ownsClient) {
            client.close();
        }
    }

    @Override
    public String toString() {
        return "ZooKeeperKeyValueStore[" + client.getZookeeperClient().getCurrentConnectionString() + root + "]";
    }

    /** Enough to ride out contention, few enough that a genuine livelock still surfaces. */
    private static final int RACE_ATTEMPTS = 16;

    /**
     * The client's retry policy, except during {@link #run}, when the calling thread's operation gets one attempt.
     *
     * <p>Curator has no per-operation retry setting, but it asks the policy on the calling thread whether to retry
     * a foreground operation, so a thread-local flag scopes "no retry" to exactly one call. Installed over whatever
     * policy the client was built with, which every other operation keeps using.
     */
    static final class SingleAttempt implements RetryPolicy {

        private static final ThreadLocal<Boolean> ACTIVE = ThreadLocal.withInitial(() -> false);

        private final RetryPolicy delegate;

        SingleAttempt(RetryPolicy delegate) {
            this.delegate = delegate;
        }

        static void install(CuratorZookeeperClient zookeeper) {
            RetryPolicy current = zookeeper.getRetryPolicy();
            if (current instanceof SingleAttempt == false) {
                zookeeper.setRetryPolicy(new SingleAttempt(current));
            }
        }

        static Stat run(Mutation mutation) throws Exception {
            ACTIVE.set(true);
            try {
                return mutation.apply();
            } finally {
                ACTIVE.set(false);
            }
        }

        @Override
        public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper) {
            return ACTIVE.get() == false && delegate.allowRetry(retryCount, elapsedTimeMs, sleeper);
        }

        @Override
        public boolean allowRetry(Throwable exception) {
            return ACTIVE.get() == false && delegate.allowRetry(exception);
        }
    }
}
