package org.opensearch.supervisor.model.pool;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.PoolId;

import java.util.List;
import java.util.Objects;

/**
 * {@code pools/definitions/<pool-id>} — operator-written.
 *
 * @param awarenessAttributes node attributes a shard's copies are spread across — zones, racks, whatever
 *     this deployment's failure domains are called. Operator-owned because only an operator knows which node
 *     attribute corresponds to a thing that fails independently; the supervisor can see the attributes but not
 *     what they mean. Empty disables the rule, which is the right default: spreading across an attribute
 *     nobody chose would refuse placements for a reason nobody asked for.
 * @param isDefault at most one pool may set this. A second is a finding, because the destination of an
 *     unbound tenant would otherwise depend on scan order. Convenient for a long tail and dangerous for a
 *     large tenant that lands in a shared pool unnoticed, so it is off unless configured.
 */
public record PoolDefinition(
    PoolId pool,
    PoolKind kind,
    @JsonProperty("default") boolean isDefault,
    String description,
    Integer maxShardCopiesPerNode,
    List<String> awarenessAttributes
) {

    public PoolDefinition {
        Objects.requireNonNull(pool, "pool");
        kind = kind == null ? PoolKind.UNKNOWN : kind;
        awarenessAttributes = awarenessAttributes == null ? List.of() : List.copyOf(awarenessAttributes);
    }

    /** The common case: a pool with no special rules. */
    public static PoolDefinition of(PoolId pool, PoolKind kind) {
        return new PoolDefinition(pool, kind, false, null, null, List.of());
    }

    public byte[] toBytes() {
        return Json.write(this, "pool definition");
    }

    public static PoolDefinition from(byte[] bytes) {
        return Json.read(bytes, PoolDefinition.class, "pool definition");
    }
}
