package org.opensearch.supervisor.model.diff;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;

import java.util.Objects;

/**
 * What a finding is about, in a form that is stable across passes.
 *
 * <p>Stability is the requirement. Finding age and oscillation detection both key on this, so a subject
 * that changed representation between passes would reset its own age every pass and never look old — which
 * would silently disable the only alarm this system has.
 *
 * @param indexUuid the uuid, never the name. A name is reused, and a subject keyed on one would carry an
 *     index's age over to its deleted predecessor's successor.
 */
public record Subject(TenantId tenant, String indexUuid, Integer shard, NodeId node, PoolId pool, String taskId) {

    public static Subject ofShard(TenantId tenant, String indexUuid, int shard) {
        return new Subject(tenant, indexUuid, shard, null, null, null);
    }

    public static Subject ofShardCopy(TenantId tenant, String indexUuid, int shard, NodeId node) {
        return new Subject(tenant, indexUuid, shard, node, null, null);
    }

    public static Subject ofIndex(TenantId tenant, String indexUuid) {
        return new Subject(tenant, indexUuid, null, null, null, null);
    }

    public static Subject ofNode(NodeId node) {
        return new Subject(null, null, null, node, null, null);
    }

    public static Subject ofTenant(TenantId tenant) {
        return new Subject(tenant, null, null, null, null, null);
    }

    public static Subject ofPool(PoolId pool) {
        return new Subject(null, null, null, null, pool, null);
    }

    public static Subject ofTask(TenantId tenant, String taskId) {
        return new Subject(tenant, null, null, null, null, taskId);
    }

    /** A short stable rendering, used as the key in projected documents and in oscillation history. */
    public String key() {
        StringBuilder key = new StringBuilder();
        if (tenant != null) {
            key.append(tenant.value());
        }
        if (indexUuid != null) {
            key.append('/').append(indexUuid);
        }
        if (shard != null) {
            key.append('/').append(shard);
        }
        if (node != null) {
            key.append('@').append(node.value());
        }
        if (pool != null) {
            key.append("pool:").append(pool.value());
        }
        if (taskId != null) {
            key.append("task:").append(taskId);
        }
        return key.isEmpty() ? "-" : key.toString();
    }

    @Override
    public String toString() {
        return key();
    }

    public Subject {
        // At least one dimension, or the subject cannot be told apart from any other.
        if (tenant == null && node == null && pool == null && taskId == null) {
            throw new IllegalArgumentException("a subject needs a tenant, a node or a pool");
        }
        Objects.requireNonNull(key());
    }
}
