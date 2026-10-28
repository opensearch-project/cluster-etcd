package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Parsed values, keyed by the revision they were parsed at.
 *
 * <p>R2.6 asks that a pass over ten thousand indices not be one giant piece of work, and the cheapest way
 * to bound it is to not re-parse what has not changed. {@code scanPrefix} returns each value with its
 * revision, and revisions compare for equality on the same key, so a changed revision is exactly the signal
 * needed — no digest, no timestamp, no guessing.
 *
 * <p><b>Parsed values only, never a substitute for reading.</b> Losing this cache costs CPU on one pass and
 * nothing else, which R2.3 requires: no state may live only in memory if losing it leaves the system stuck.
 */
final class RevisionCache<T> {

    record Entry<T>(long revision, T value) {}

    private final Map<String, Entry<T>> entries = new HashMap<>();
    private int hits;
    private int misses;

    /** The cached value if the revision matches, otherwise parses and caches. */
    T get(String key, long revision, Function<String, T> parse) {
        Entry<T> entry = entries.get(key);
        if (entry != null && entry.revision() == revision) {
            hits++;
            return entry.value();
        }
        misses++;
        T value = parse.apply(key);
        entries.put(key, new Entry<>(revision, value));
        return value;
    }

    /** Drop anything no longer present, so the cache cannot outgrow the keyspace it mirrors. */
    void retain(Set<String> keys) {
        entries.keySet().retainAll(keys);
    }

    /** The last value parsed for this key and the revision it was parsed at, or null. Never parses. */
    Entry<T> peek(String key) {
        return entries.get(key);
    }

    void forget(String key) {
        entries.remove(key);
    }

    int size() {
        return entries.size();
    }

    int hits() {
        return hits;
    }

    int misses() {
        return misses;
    }
}
