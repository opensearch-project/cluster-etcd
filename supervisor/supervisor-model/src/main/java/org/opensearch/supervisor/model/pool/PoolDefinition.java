package org.opensearch.supervisor.model.pool;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.PoolId;

import java.util.Objects;

/**
 * {@code pools/definitions/<pool-id>} — operator-written.
 *
 * @param isDefault at most one pool may set this. A second is a finding, because the destination of an
 *     unbound tenant would otherwise depend on scan order. Convenient for a long tail and dangerous for a
 *     large tenant that lands in a shared pool unnoticed, so it is off unless configured.
 */
public record PoolDefinition(
    PoolId pool,
    PoolKind kind,
    @JsonProperty("default") boolean isDefault,
    String description,
    Integer maxShardCopiesPerNode
) {

    public PoolDefinition {
        Objects.requireNonNull(pool, "pool");
        kind = kind == null ? PoolKind.UNKNOWN : kind;
    }

    public byte[] toBytes() {
        return Json.write(this, "pool definition");
    }

    public static PoolDefinition from(byte[] bytes) {
        return Json.read(bytes, PoolDefinition.class, "pool definition");
    }
}
