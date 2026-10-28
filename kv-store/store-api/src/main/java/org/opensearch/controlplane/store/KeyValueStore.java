/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.util.NavigableMap;
import java.util.Optional;

/**
 * The control plane's persistence seam: an ordered key/value store of opaque byte payloads.
 *
 * <p>The control plane and the other layers communicate only through this store. A create-index
 * request writes index configuration and registers an index management task here; the shard
 * manager reads those tasks and writes per-node configuration; data nodes consume their own
 * configuration. No layer calls another directly.
 *
 * <h2>Revisions</h2>
 *
 * <p>Every key carries a revision, and writes can be made conditional on it. This is not speculative:
 * {@code indices.create} must fail with {@code resource_already_exists_exception} rather than clobbering
 * an existing index, and operations like {@code indices.update_aliases} and {@code indices.put_mapping}
 * are read-modify-write cycles that lose updates without a compare-and-set.
 *
 * <p>Consumers use revisions only for equality: as the token for a conditional write, and as a change
 * signal — a reader that parsed a key at revision R and sees R again may skip the parse, and liveness and
 * lease expiry are judged by how long a revision has gone unchanged. Revisions are also stored inside other
 * values and compared across processes ({@code configRevision}). A backend must therefore guarantee:
 *
 * <ol>
 *   <li><b>A revision is never reused for a key</b>, including after that key has been deleted and written
 *       again, and including when a write repeats the previous bytes exactly. A backend which restarts a
 *       key's revision after deletion breaks readers silently: the key comes back at a revision already
 *       seen, carrying different content, and a reader keeps the stale value for ever with nothing
 *       anywhere to indicate it. This went unstated once, and the consequence was a supervisor whose
 *       desired state froze on a deleted index's uuid while the data node correctly declined to build it.
 *       Content hashes, such as S3 ETags, fail the second half.</li>
 *   <li><b>Revisions strictly increase per key.</b> No consumer relies on the order, but it is what makes
 *       the first rule implementable and testable.</li>
 *   <li><b>{@code put} returns the revision later reads report</b>, in every process.</li>
 *   <li><b>A revision is positive and at most {@link #MAX_REVISION}</b>, 2<sup>53</sup> − 1, so it survives
 *       a JSON parser that reads numbers as doubles.</li>
 * </ol>
 *
 * <p>Nothing is promised <em>across</em> keys: two keys may share a revision, and a write to one key may
 * get a lower revision than an earlier write to another. That freedom is what lets a backend without a
 * store-wide transaction id — DynamoDB, S3 — issue revisions per key instead of funnelling every write
 * through one counter. etcd's {@code mod_revision}, ZooKeeper's {@code mzxid} and the filesystem store's
 * counter happen to be store-wide; consumers must not depend on it. Revisions need not be dense either, so
 * recording a counter before using it, and leaving a gap on a crash, is a legitimate way to keep rule 1.
 *
 * <h2>Consistency</h2>
 *
 * <ul>
 *   <li><b>Conditional writes are linearizable per key.</b> Leases, task claims and assignments depend on
 *       exactly one of two racing writers winning.</li>
 *   <li><b>Through one store instance, reads see that instance's own writes and never go backwards</b>, and
 *       after a conditional write loses, a read sees the winner.</li>
 *   <li><b>Across instances, reads may lag by a bounded amount</b>: seconds at most, well under the ~20 s
 *       at which a stale heartbeat reads as a lost node. One process may briefly miss another's
 *       acknowledged write. This is deliberately weaker than linearizable reads, which etcd provides and
 *       ZooKeeper, reading from followers without {@code sync()}, does not.</li>
 *   <li><b>{@link #scanPrefix} is not a snapshot.</b> Writes during a scan may or may not appear. A key
 *       that exists throughout the scan is always returned, and a value is never partial.</li>
 *   <li><b>An acknowledged write survives failover of the backend.</b> A deployment that can lose
 *       acknowledged writes — an asynchronously replicated database, say — can also reissue revisions,
 *       which breaks rule 1.</li>
 *   <li><b>No multi-key atomicity.</b> Consumers order their writes instead.</li>
 * </ul>
 *
 * <h2>Limits</h2>
 *
 * <p>Every backend accepts keys up to {@link #MAX_KEY_BYTES} and values up to {@link #MAX_VALUE_BYTES}, and
 * rejects anything larger with {@link LimitExceededException} — even a backend that could store more, so
 * that a consumer which works on one backend works on all of them. Reads of a longer key simply find
 * nothing. The value limit is ZooKeeper's: its default {@code jute.maxbuffer} is just under 1 MiB for a
 * whole request, path included. See {@link StoreLimits}.
 *
 * <h2>Implementations</h2>
 *
 * <p>etcd, ZooKeeper, and a filesystem store for development. {@code BACKENDS.md} in this library sketches
 * DynamoDB, relational, OpenSearch and S3 backends, and is where the rules above came from.
 *
 * <p>Implementations must be safe for concurrent use.
 */
public interface KeyValueStore extends AutoCloseable {

    /**
     * Revision meaning "this key must not exist". Pass as the expected revision to a conditional
     * write to get create-if-absent semantics.
     */
    long ABSENT = 0L;

    /** The largest revision a backend may issue: 2<sup>53</sup> − 1, the largest integer a double holds exactly. */
    long MAX_REVISION = (1L << 53) - 1;

    /** The longest key, in UTF-8 bytes, that every backend accepts. */
    int MAX_KEY_BYTES = 512;

    /** The largest value, in bytes, that every backend accepts. */
    int MAX_VALUE_BYTES = 960 * 1024;

    /**
     * A stored payload and the revision it was written at.
     *
     * <p>{@code value} is not copied on the way in or out. Treat it as immutable; callers must not
     * mutate the array. Being a record, equality is array identity rather than contents — compare
     * with {@link java.util.Arrays#equals} when that matters.
     */
    record Versioned(byte[] value, long revision) {}

    Optional<Versioned> get(String key);

    /** Every key beginning with {@code keyPrefix}, in ascending key order. */
    NavigableMap<String, Versioned> scanPrefix(String keyPrefix);

    /**
     * Unconditional upsert.
     *
     * @return the revision the value was written at
     * @throws LimitExceededException if the key or value is over its limit
     */
    long put(String key, byte[] value);

    /**
     * Conditional upsert.
     *
     * @param expectedRevision the revision the key must currently be at, or {@link #ABSENT} to
     *                         require that it does not exist
     * @return the revision the value was written at
     * @throws RevisionConflictException if the key is not at {@code expectedRevision}
     * @throws LimitExceededException if the key or value is over its limit
     */
    long put(String key, byte[] value, long expectedRevision);

    /**
     * Unconditional delete.
     *
     * @return whether the key existed
     */
    boolean delete(String key);

    /**
     * Conditional delete.
     *
     * @return whether the key existed
     * @throws RevisionConflictException if the key exists but is not at {@code expectedRevision}
     */
    boolean delete(String key, long expectedRevision);

    /**
     * Asks to be notified when anything under {@code keyPrefix} changes.
     *
     * <p>Optional. A backend that cannot watch returns {@link Optional#empty()} rather than throwing, and
     * <b>callers should not branch on that</b> — use {@link Watches#watchOrPoll} instead, which falls back
     * to polling so that a consumer works on every backend including the filesystem one.
     *
     * <p>Read {@link Watch} before using this. A notification is a hint that something may have changed and
     * carries no payload; it may be coalesced, spurious, or missed. Correctness must come from re-reading.
     *
     * @param onChange invoked on a backend-owned thread; must not block or call back into the store
     * @return the registered watch, or empty if this backend does not support watching
     */
    default Optional<Watch> watch(String keyPrefix, Runnable onChange) {
        return Optional.empty();
    }

    /**
     * Releases whatever the implementation holds.
     *
     * <p>Defaulted to nothing so the JDK-only implementations need not care, and declared at all because
     * a remote backend does: an etcd or ZooKeeper store owns a client with connection threads, and a
     * control plane that shut down without closing it would hang. {@code Main} closes the store in its
     * shutdown hook.
     *
     * <p>Narrowed from {@link AutoCloseable} to throw nothing, because there is nothing a caller could
     * usefully do about a failure to close and every call site would otherwise need a try/catch.
     */
    @Override
    default void close() {}
}
