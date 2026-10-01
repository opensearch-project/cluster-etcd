package org.opensearch.supervisor.model.supervisor;

import org.opensearch.supervisor.model.Json;

import java.util.Objects;

/**
 * {@code supervisor/pools/<pool-id>/lease}, and {@code supervisor/leases/housekeeping} — <b>the only
 * mutual-exclusion primitive in this system.</b>
 *
 * <p>Everything else has one writer by construction ({@code DESIGN.md} §2), so this is the single place two
 * supervisors ever contend, and in steady state they do not. Acquired and renewed by compare-and-set on the
 * key's own revision, which R6.2 prefers to a global lock for a specific reason: a stale supervisor loses
 * individual writes rather than corrupting shared state.
 *
 * @param epoch increments on each <em>acquisition</em>, never on renewal, and is stamped into every
 *     assignment the holder writes — so a split brain is diagnosable after the fact from data the system
 *     already wrote.
 * @param expiresAt diagnostic. <b>Expiry is not judged from this field</b>: a challenger decides a lease is
 *     claimable from how long <em>it</em> has observed the key's revision not changing, exactly as node
 *     liveness works and for the same reason — two clocks that disagree would otherwise let a challenger
 *     steal a healthy lease or leave a dead one held.
 */
public record Lease(String holder, long epoch, long acquiredAt, long expiresAt) {

    public Lease {
        Objects.requireNonNull(holder, "holder");
        if (holder.isBlank()) {
            throw new IllegalArgumentException("a lease with no holder is not a lease");
        }
    }

    /** A renewal keeps the epoch; only an acquisition advances it. */
    public Lease renewedAt(long now, long ttlMillis) {
        return new Lease(holder, epoch, acquiredAt, now + ttlMillis);
    }

    public byte[] toBytes() {
        return Json.write(this, "lease");
    }

    public static Lease from(byte[] bytes) {
        return Json.read(bytes, Lease.class, "lease");
    }
}
