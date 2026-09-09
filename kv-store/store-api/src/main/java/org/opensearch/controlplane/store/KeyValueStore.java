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
 * <p>Every key carries a monotonically increasing revision, and writes can be made conditional on
 * it. This is not speculative: {@code indices.create} must fail with
 * {@code resource_already_exists_exception} rather than clobbering an existing index, and
 * operations like {@code indices.update_aliases} and {@code indices.put_mapping} are
 * read-modify-write cycles that lose updates without a compare-and-set. All three likely backends
 * support this directly — etcd compares {@code ModRevision}, DynamoDB uses a conditional
 * expression on a version attribute, and S3 has conditional writes via {@code If-Match} /
 * {@code If-None-Match} on ETags.
 *
 * <p>Revisions are opaque and only comparable for equality. Do not assume they are dense,
 * global, or meaningful across keys.
 *
 * <h2>Implementations</h2>
 *
 * <p>Expected production backends are etcd or DynamoDB depending on the environment. Payloads are
 * small — index and pipeline configuration, not documents — which is worth keeping in mind when
 * evaluating object stores for this role.
 *
 * <p>Implementations must be safe for concurrent use.
 */
public interface KeyValueStore extends AutoCloseable {

    /**
     * Revision meaning "this key must not exist". Pass as the expected revision to a conditional
     * write to get create-if-absent semantics.
     */
    long ABSENT = 0L;

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
     */
    long put(String key, byte[] value);

    /**
     * Conditional upsert.
     *
     * @param expectedRevision the revision the key must currently be at, or {@link #ABSENT} to
     *                         require that it does not exist
     * @return the revision the value was written at
     * @throws RevisionConflictException if the key is not at {@code expectedRevision}
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
