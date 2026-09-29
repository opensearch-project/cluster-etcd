package org.opensearch.supervisor.model.supervisor;

import org.opensearch.supervisor.model.Json;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@code supervisor/terms/<tenant>/<index-uuid>}: the highest primary term issued for each shard of one index.
 *
 * <p><b>The first state this service owns that it cannot recompute</b> ({@code DESIGN.md} §26.4, §28.6). Everything
 * else it writes is derived from an observation or an instruction computed from one; this is input to a decision. So
 * it only ever increases: losing track of it can be repaired by skipping forward and never by reusing a value, because
 * a reused term lets two primaries of one shard be told the same one.
 *
 * @param shards by shard number. A shard with no entry has never been issued a term by this service.
 */
public record PrimaryTerms(Map<Integer, Long> shards) {

    public PrimaryTerms {
        shards = shards == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(shards));
    }

    public long termOf(int shard) {
        return shards.getOrDefault(shard, 1L);
    }

    /** The same counters with one shard advanced by exactly one. */
    public PrimaryTerms advanced(int shard) {
        Map<Integer, Long> next = new TreeMap<>(shards);
        next.put(shard, termOf(shard) + 1);
        return new PrimaryTerms(next);
    }

    /**
     * The same counters, never lower than {@code floor} for any shard in it.
     *
     * <p>How a lost or new counter is rebuilt from what nodes were last told: skip forward to cover every term already
     * in circulation, so nothing issued after this can collide with one.
     */
    public PrimaryTerms atLeast(Map<Integer, Long> floor) {
        Map<Integer, Long> next = new TreeMap<>(shards);
        floor.forEach((shard, term) -> next.merge(shard, term, Math::max));
        return new PrimaryTerms(next);
    }

    public byte[] toBytes() {
        return Json.write(this, "primary terms");
    }

    public static PrimaryTerms from(byte[] bytes) {
        return Json.read(bytes, PrimaryTerms.class, "primary terms");
    }
}
