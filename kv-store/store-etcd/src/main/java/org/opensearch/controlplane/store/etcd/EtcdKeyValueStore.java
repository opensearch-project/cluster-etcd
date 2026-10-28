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
import io.etcd.jetcd.options.WatchOption;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;
import org.opensearch.controlplane.store.StoreLimits;
import org.opensearch.controlplane.store.Watch;

import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

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
 * <h2>What was checked against a real cluster</h2>
 *
 * <p>Every assumption below was probed against etcd 3.7.1 directly, through its HTTP/JSON gateway, rather
 * than inferred from the documentation or from the tests passing:
 *
 * <ul>
 *   <li>A put's <b>header revision equals the key's new {@code mod_revision}</b>, which is what
 *       {@link #put(String, byte[])} returns.</li>
 *   <li><b>Comparing {@code mod_revision} to 0 means "absent"</b> — the transaction succeeds on a key that
 *       does not exist and fails once it does. This is what lets {@link KeyValueStore#ABSENT} be an
 *       ordinary revision rather than a special case.</li>
 *   <li><b>Rewriting an identical value still moves the revision.</b> The contract requires this, and etcd
 *       only skips the bump under {@code IGNORE_VALUE}, which is not used here.</li>
 *   <li>A failed transaction's else-branch range <b>carries the key's current {@code mod_revision}</b>, so
 *       a conflict reports the real revision without a second round trip.</li>
 *   <li><b>A prefix range returns keys in ascending order</b>, so the {@code TreeMap} agrees with etcd
 *       rather than reordering behind its back.</li>
 *   <li>A delete of an absent key reports <b>zero deleted</b>, and a delete transaction comparing
 *       {@code mod_revision} to 0 on an absent key <b>succeeds</b> with nothing deleted — which is
 *       precisely why {@code delete(key, ABSENT)} answers {@code false} instead of throwing.</li>
 * </ul>
 *
 * <h2>Limitations</h2>
 *
 * <ul>
 *   <li><b>Values are capped by the interface</b> at {@link KeyValueStore#MAX_VALUE_BYTES}, inside etcd's
 *       1.5 MiB default request limit.</li>
 *   <li><b>{@link #scanPrefix} is paged</b>, {@link #SCAN_PAGE_KEYS} keys per request, so it is not a
 *       snapshot: a write between pages may or may not appear. The interface permits that, and one
 *       unpaged range would fail at gRPC's message limit once a prefix held a few large values.</li>
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
        byte[] prefix = keyOf(keyPrefix).getBytes();
        ByteSequence end = ByteSequence.from(prefixEnd(prefix));
        ByteSequence from = ByteSequence.from(prefix);

        NavigableMap<String, Versioned> found = new TreeMap<>();
        int rootLength = root.length() + 1;
        while (true) {
            // Each page reads at its own revision, so a write between pages may or may not appear. The contract
            // permits exactly that, and pinning the first page's revision instead would fail the scan whenever
            // compaction overtook it.
            GetResponse page = await("scan", keyPrefix,
                kv.get(from, GetOption.builder().withRange(end).withLimit(SCAN_PAGE_KEYS).build()));
            for (KeyValue entry : page.getKvs()) {
                String fullKey = entry.getKey().toString(StandardCharsets.UTF_8);
                found.put(
                    fullKey.substring(rootLength),
                    new Versioned(entry.getValue().getBytes(), entry.getModRevision())
                );
            }
            if (page.isMore() == false || page.getKvs().isEmpty()) {
                return found;
            }
            // The smallest key after the last one returned: the same bytes with a zero byte appended.
            byte[] last = page.getKvs().get(page.getKvs().size() - 1).getKey().getBytes();
            from = ByteSequence.from(java.util.Arrays.copyOf(last, last.length + 1));
        }
    }

    /**
     * The exclusive end of the range of keys starting with {@code prefix}: the prefix with its last byte
     * incremented, after dropping trailing {@code 0xff} bytes, which cannot be incremented. The root is never
     * empty and is UTF-8, so there is always a byte to increment.
     */
    static byte[] prefixEnd(byte[] prefix) {
        for (int i = prefix.length - 1; i >= 0; i--) {
            if (prefix[i] != (byte) 0xff) {
                byte[] end = java.util.Arrays.copyOf(prefix, i + 1);
                end[i]++;
                return end;
            }
        }
        throw new IllegalArgumentException("a prefix of only 0xff bytes has no end");
    }

    /**
     * Keys per scan page. The page has to fit the client's inbound message limit at the worst case, every value
     * at {@link KeyValueStore#MAX_VALUE_BYTES}, which {@link #MAX_INBOUND_MESSAGE_BYTES} is sized for. Large
     * enough that a fleet-wide scan of small documents, such as heartbeats, takes a handful of round trips.
     */
    static final int SCAN_PAGE_KEYS = 256;

    /**
     * gRPC's inbound limit for this store's client: one full page of maximum-size entries, plus a margin for
     * protobuf framing and the root. gRPC's default of 4 MiB would fail a scan returning five large values.
     * The limit costs nothing until a response is that large, and the scan is materialised whole regardless.
     */
    static final int MAX_INBOUND_MESSAGE_BYTES =
        SCAN_PAGE_KEYS * (KeyValueStore.MAX_VALUE_BYTES + KeyValueStore.MAX_KEY_BYTES + 1024) + (1 << 20);

    @Override
    public long put(String key, byte[] value) {
        StoreLimits.checkWrite(key, value);
        // Every put moves the store revision, even one writing an identical value, so the returned
        // revision always differs from the previous one -- which the contract requires.
        return await("put", key, kv.put(keyOf(key), valueOf(value), PutOption.DEFAULT))
            .getHeader().getRevision();
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        StoreLimits.checkWrite(key, value);
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

    /**
     * A native range watch over the prefix.
     *
     * <p>The simplest of the three implementations, because etcd's watch is a superset of what {@link Watch}
     * asks for: a reliable ordered stream of events from a revision, delivered over the existing connection
     * and resumed by the client across reconnects. All of that is discarded here — the response is read only
     * for the fact that it arrived.
     *
     * <p>Throwing away a reliable stream to honour a lossy contract looks wasteful and is the right call.
     * The alternative is an SPI whose guarantees depend on which backend is configured, and consumers that
     * quietly stop working when someone switches from etcd to ZooKeeper. If a consumer ever genuinely needs
     * the event detail, that is a new method on the interface with an explicit capability check, not a
     * silent difference in what {@code watch} means.
     *
     * <p>Unlike ZooKeeper this never declines: etcd watches a range, and a range that is currently empty is
     * an ordinary range.
     *
     * @param onChange invoked for every batch of events; errors are counted rather than delivered, since a
     *                 caller cannot act on them and the contract already permits missed notifications
     */
    @Override
    public Optional<Watch> watch(String keyPrefix, Runnable onChange) {
        io.etcd.jetcd.Watch.Watcher watcher;
        try {
            watcher = client.getWatchClient().watch(
                keyOf(keyPrefix),
                WatchOption.builder().isPrefix(true).build(),
                response -> onChange.run(),
                error -> WATCH_FAILURES.incrementAndGet()
            );
        } catch (RuntimeException e) {
            // Empty rather than thrown, so watchOrPoll degrades to polling. A watch is an optimisation.
            WATCH_FAILURES.incrementAndGet();
            return Optional.empty();
        }
        return Optional.of(watcher::close);
    }

    /**
     * Watch stream errors since the process started.
     *
     * <p>Same reasoning as {@code Watches.pollFailures}: a watch that has silently stopped delivering is
     * indistinguishable from a quiet keyspace, so the count needs to be observable. jetcd re-establishes a
     * broken stream itself, so a non-zero value here is a hint rather than an outage.
     */
    public static long watchFailures() {
        return WATCH_FAILURES.get();
    }

    private static final AtomicLong WATCH_FAILURES = new AtomicLong();

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
