package org.opensearch.supervisor.model.pool;

import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.PoolId;

import java.util.Objects;
import java.util.Optional;

/**
 * {@code pools/bindings/<tenant-id>} — which pool serves a tenant. One key per tenant, and that is the
 * whole point: rebinding is then a single compare-and-set.
 *
 * <p>A membership list inside the pool document would make a move two writes to two keys with no
 * transaction, and the torn state is "this tenant is in both pools" — which is two supervisors placing its
 * shards, the one thing the partition model exists to prevent.
 *
 * @param movingTo set by an operator to begin a rebind. While it is present the current owner keeps
 *     ownership and stops placing new shards; the handover itself is a single compare-and-set performed by
 *     the <em>outgoing</em> owner, which is the only ordering with no instant of two owners.
 */
public record PoolBinding(PoolId pool, PoolId movingTo, long generation) {

    public PoolBinding {
        Objects.requireNonNull(pool, "pool");
        if (pool.equals(movingTo)) {
            throw new IllegalArgumentException("pool [" + pool + "] cannot be moving to itself");
        }
    }

    public static PoolBinding to(PoolId pool) {
        return new PoolBinding(pool, null, 1);
    }

    public Optional<PoolId> movingToPool() {
        return Optional.ofNullable(movingTo);
    }

    public boolean isRebinding() {
        return movingTo != null;
    }

    public byte[] toBytes() {
        return Json.write(this, "pool binding");
    }

    public static PoolBinding from(byte[] bytes) {
        return Json.read(bytes, PoolBinding.class, "pool binding");
    }
}
