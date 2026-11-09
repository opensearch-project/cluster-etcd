package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.OutcomeUnknownException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

/**
 * A store that loses the answer to chosen conditional writes, as a dropped connection or a timeout does: the caller
 * is told {@link OutcomeUnknownException} whether or not the write applied.
 *
 * <p>A decorator rather than a real backend's fault, because what matters is each of the three outcomes on demand:
 * applied, not applied, and applied only after the caller has read the key back.
 */
final class LostAnswerStore implements KeyValueStore {

    enum Loss {
        /** Applied, and the answer lost on the way back. */
        APPLIED,
        /** Lost before it reached the store. */
        NOT_APPLIED,
        /** Still in flight when the caller gives up on it; applied by {@link #land()}. */
        LATE
    }

    private final KeyValueStore delegate;
    private final Map<String, Loss> next = new HashMap<>();
    private final List<Runnable> inFlight = new ArrayList<>();
    private boolean readsFail;

    LostAnswerStore(KeyValueStore delegate) {
        this.delegate = delegate;
    }

    /** Loses the answer to the next conditional write of this key, in the way given. */
    void loseNext(String key, Loss loss) {
        next.put(key, loss);
    }

    /** Makes every read fail, as an unreachable store does, until called again with false. */
    void readsFail(boolean fail) {
        readsFail = fail;
    }

    /** Applies the writes still in flight. */
    void land() {
        inFlight.forEach(Runnable::run);
        inFlight.clear();
    }

    private void lose(String key, Runnable write) {
        Loss loss = next.remove(key);
        if (loss == null) {
            write.run();
            return;
        }
        switch (loss) {
            case APPLIED -> write.run();
            case NOT_APPLIED -> {}
            case LATE -> inFlight.add(write);
        }
        throw new OutcomeUnknownException("lost the answer to a write of [" + key + "] (" + loss + ")", null);
    }

    private void read() {
        if (readsFail) {
            throw new IllegalStateException("store unreachable");
        }
    }

    @Override
    public Optional<Versioned> get(String key) {
        read();
        return delegate.get(key);
    }

    @Override
    public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
        read();
        return delegate.scanPrefix(keyPrefix);
    }

    @Override
    public long put(String key, byte[] value) {
        return delegate.put(key, value);
    }

    @Override
    public long put(String key, byte[] value, long expectedRevision) {
        long[] revision = new long[1];
        lose(key, () -> revision[0] = delegate.put(key, value, expectedRevision));
        return revision[0];
    }

    @Override
    public boolean delete(String key) {
        return delegate.delete(key);
    }

    @Override
    public boolean delete(String key, long expectedRevision) {
        boolean[] deleted = new boolean[1];
        lose(key, () -> deleted[0] = delegate.delete(key, expectedRevision));
        return deleted[0];
    }
}
