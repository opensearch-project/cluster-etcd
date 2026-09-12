package org.opensearch.supervisor.nodecontract;

import org.opensearch.controlplane.store.KeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;

/**
 * A store that remembers the order of writes.
 *
 * <p>The contract has one ordering requirement — the inventory is written before the heartbeat announcing
 * its digest — and it cannot be asserted from the store's final contents, only from the sequence that
 * produced them. It also makes "this node wrote a key it does not own" observable, which is otherwise
 * invisible: a node writing into a tenant's subtree would leave a document that looks like somebody else's.
 */
public final class RecordingStore implements KeyValueStore {

    private final KeyValueStore delegate;
    private final List<String> writes = new ArrayList<>();

    public RecordingStore(KeyValueStore delegate) {
        this.delegate = delegate;
    }

    /** Every key written, in order, including repeats. */
    public synchronized List<String> writes() {
        return List.copyOf(writes);
    }

    public synchronized void forgetWrites() {
        writes.clear();
    }

    /**
     * The same store, unrecorded, for writes the harness makes on the supervisor's behalf.
     *
     * <p>An adapter may have to put the node into a state by writing something a supervisor would write —
     * {@code fake-node}'s only way to hold a copy is to be assigned it. Those writes are the test's, not
     * the node's, and recording them would make {@code aNodeWritesOnlyItsOwnThreeKeys} fail on a node that
     * had done nothing wrong. Anything an adapter reaches for this for should be a key the node under test
     * <em>reads</em>.
     */
    public KeyValueStore asSupervisor() {
        return delegate;
    }

    @Override
    public Optional<Versioned> get(String key) {
        return delegate.get(key);
    }

    @Override
    public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
        return delegate.scanPrefix(keyPrefix);
    }

    @Override
    public long put(String key, byte[] value) {
        record(key);
        return delegate.put(key, value);
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        record(key);
        return delegate.put(key, value, expectedRevision);
    }

    @Override
    public boolean delete(String key) {
        record(key);
        return delegate.delete(key);
    }

    @Override
    public boolean delete(String key, long expectedRevision) {
        record(key);
        return delegate.delete(key, expectedRevision);
    }

    private synchronized void record(String key) {
        writes.add(key);
    }
}
