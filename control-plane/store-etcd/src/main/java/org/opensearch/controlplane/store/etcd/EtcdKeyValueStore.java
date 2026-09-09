/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;

import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link KeyValueStore} backed by etcd.
 *
 * <p>Much the shorter of the two remote backends, because etcd's model is the interface's model. A flat
 * keyspace of opaque byte keys needs no encoding — no {@code ..} to escape, no empty segment to represent,
 * no reserved names. An absent key and an empty value are natively distinct, so no framing byte. And
 * {@code mod_revision} is exactly the revision this interface describes: monotonic, never reused, and
 * zero precisely when the key does not exist, which is what {@link KeyValueStore#ABSENT} means.
 *
 * <p>Every conditional operation is therefore <b>one round trip</b>, where the ZooKeeper backend needs
 * two. A transaction compares {@code mod_revision} and, in its else branch, reads the key — so a failed
 * compare-and-set comes back already carrying the actual revision for the exception, without a second
 * call.
 *
 * <h2>Keys</h2>
 *
 * <p>{@code <root>/<key>}, UTF-8. The root keeps a control plane's keys from colliding with anything else
 * on the cluster and makes {@code scanPrefix("")} a bounded range rather than the whole keyspace.
 *
 * <h2>Limitations</h2>
 *
 * <ul>
 *   <li><b>Values are capped by etcd's request limit</b>, 1.5 MB by default. Control plane payloads are
 *       configuration, so there is room to spare.</li>
 *   <li><b>{@link #scanPrefix} is one range request</b> and returns everything under the prefix in one
 *       response, so a very large prefix is bounded by that same request limit rather than paged. Paging
 *       would change the interface, which returns a materialised map.</li>
 *   <li><b>Compaction is irrelevant here</b>, unlike most etcd uses: revisions are only ever compared for
 *       equality against a key's current {@code mod_revision}, never read as historical versions.</li>
 * </ul>
 */
public final class EtcdKeyValueStore implements KeyValueStore {

    private final Client client;
    private final KV kv;
    private final String root;
    private final long timeoutMillis;
    private final boolean ownsClient;

    EtcdKeyValueStore(Client client, String root, long timeoutMillis, boolean ownsClient) {
        this.client = Objects.requireNonNull(client, "client");
        this.kv = client.getKVClient();
        this.root = Objects.requireNonNull(root, "root");
        this.timeoutMillis = timeoutMillis;
        this.ownsClient = ownsClient;
    }

    @Override
    public Optional<Versioned> get(String key) {
        GetResponse response = await("get", key, kv.get(keyOf(key)));
        if (response.getKvs().isEmpty()) {
            return Optional.empty();
        }
        KeyValue found = response.getKvs().get(0);
        return Optional.of(new Versioned(found.getValue().getBytes(), found.getModRevision()));
    }

    @Override
    public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
        ByteSequence from = keyOf(keyPrefix);
        GetResponse response = await("scan", keyPrefix,
            kv.get(from, GetOption.builder().isPrefix(true).build()));

        NavigableMap<String, Versioned> found = new TreeMap<>();
        int rootLength = root.length() + 1;
        for (KeyValue entry : response.getKvs()) {
            String fullKey = entry.getKey().toString(StandardCharsets.UTF_8);
            found.put(
                fullKey.substring(rootLength),
                new Versioned(entry.getValue().getBytes(), entry.getModRevision())
            );
        }
        return found;
    }

    @Override
    public long put(String key, byte[] value) {
        // Every put moves the store revision, even one writing an identical value, so the returned
        // revision always differs from the previous one -- which the contract requires.
        return await("put", key, kv.put(keyOf(key), valueOf(value), PutOption.DEFAULT))
            .getHeader().getRevision();
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        ByteSequence target = keyOf(key);
        // ABSENT is zero, and a key that does not exist has mod_revision zero, so one comparison covers
        // both create-if-absent and update-at-revision without a special case.
        Cmp atExpectedRevision = new Cmp(target, Cmp.Op.EQUAL, CmpTarget.modRevision(expectedRevision));

        TxnResponse response = await("put", key, kv.txn()
            .If(atExpectedRevision)
            .Then(Op.put(target, valueOf(value), PutOption.DEFAULT))
            // Reads the key only when the comparison failed, so the conflict can name the actual
            // revision without a second round trip.
            .Else(Op.get(target, GetOption.DEFAULT))
            .commit());

        if (response.isSucceeded()) {
            return response.getHeader().getRevision();
        }
        throw new RevisionConflictException(key, expectedRevision, actualRevisionFrom(response));
    }

    @Override
    public boolean delete(String key) {
        DeleteResponse response = await("delete", key, kv.delete(keyOf(key), DeleteOption.DEFAULT));
        return response.getDeleted() > 0;
    }

    @Override
    public boolean delete(String key, long expectedRevision) {
        ByteSequence target = keyOf(key);
        Cmp atExpectedRevision = new Cmp(target, Cmp.Op.EQUAL, CmpTarget.modRevision(expectedRevision));

        TxnResponse response = await("delete", key, kv.txn()
            .If(atExpectedRevision)
            .Then(Op.delete(target, DeleteOption.DEFAULT))
            .Else(Op.get(target, GetOption.DEFAULT))
            .commit());

        if (response.isSucceeded() == false) {
            throw new RevisionConflictException(key, expectedRevision, actualRevisionFrom(response));
        }
        // Succeeded with ABSENT expected and nothing deleted is the "key was not there" case, which the
        // contract wants reported as false rather than as a conflict.
        return response.getDeleteResponses().isEmpty() == false
            && response.getDeleteResponses().get(0).getDeleted() > 0;
    }

    /** The revision the else-branch read found, or {@link #ABSENT} if the key is not there. */
    private static long actualRevisionFrom(TxnResponse response) {
        if (response.getGetResponses().isEmpty()) {
            return ABSENT;
        }
        GetResponse found = response.getGetResponses().get(0);
        return found.getKvs().isEmpty() ? ABSENT : found.getKvs().get(0).getModRevision();
    }

    private ByteSequence keyOf(String key) {
        return ByteSequence.from(root + "/" + key, StandardCharsets.UTF_8);
    }

    private static ByteSequence valueOf(byte[] value) {
        return ByteSequence.from(value);
    }

    /**
     * jetcd is asynchronous throughout; this interface is not.
     *
     * <p>A timeout is applied rather than waiting indefinitely, because a control plane blocking forever
     * on an unreachable etcd is worse than a request failing: the caller can retry, and an operator can
     * see what is wrong.
     */
    private <T> T await(String operation, String key, java.util.concurrent.CompletableFuture<T> future) {
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("etcd " + operation + " of [" + key + "] was interrupted", e);
        } catch (TimeoutException e) {
            throw new IllegalStateException(
                "etcd " + operation + " of [" + key + "] timed out after " + timeoutMillis + "ms", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IllegalStateException(
                "etcd " + operation + " of [" + key + "] failed: " + cause, cause);
        }
    }

    @Override
    public void close() {
        if (ownsClient) {
            client.close();
        }
    }

    @Override
    public String toString() {
        return "EtcdKeyValueStore[" + root + "]";
    }
}
