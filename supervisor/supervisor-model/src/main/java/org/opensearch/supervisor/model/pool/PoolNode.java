package org.opensearch.supervisor.model.pool;

import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.PoolId;

import java.util.Objects;

/**
 * {@code pools/nodes/<node-id>} — an operator's override of the pool a node declared for itself.
 *
 * <p>Absent for most nodes: the common case is a node deployed with a pool in its own configuration, which
 * needs no write here at all. This exists so a node can be moved between pools without being redeployed —
 * one key, one compare-and-set, and its shard copies move with it, which is the cheap form of migration.
 *
 * <p>Operator-owned precisely because a node must not be able to move itself into someone else's pool.
 */
public record PoolNode(PoolId pool) {

    public PoolNode {
        Objects.requireNonNull(pool, "pool");
    }

    public byte[] toBytes() {
        return Json.write(this, "pool node override");
    }

    public static PoolNode from(byte[] bytes) {
        return Json.read(bytes, PoolNode.class, "pool node override");
    }
}
