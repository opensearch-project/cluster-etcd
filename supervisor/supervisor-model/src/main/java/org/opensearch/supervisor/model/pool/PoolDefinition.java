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
 * @param refuseCopiesSharingAHost whether two copies of one shard may live on nodes that report the same
 *     address. Off by default, matching OpenSearch's own
 *     {@code cluster.routing.allocation.same_shard.host}, and for the same reason: an address is a poor proxy
 *     for a failure domain — too strict for several nodes on one host, and useless in Kubernetes, where pods
 *     sharing a machine have different addresses. {@code awarenessAttributes} is the mechanism that expresses
 *     this properly; turn this on only where an address really is a machine. See
 *     {@code SameShardDecider}, and note that leaving it off is what allows a single-machine deployment to
 *     place a replica at all.
 */
public record PoolDefinition(
    PoolId pool,
    PoolKind kind,
    @JsonProperty("default") boolean isDefault,
    String description,
    Integer maxShardCopiesPerNode,
    List<String> awarenessAttributes,
    boolean refuseCopiesSharingAHost
) {

    public PoolDefinition {
        Objects.requireNonNull(pool, "pool");
        kind = kind == null ? PoolKind.UNKNOWN : kind;
        awarenessAttributes = awarenessAttributes == null ? List.of() : List.copyOf(awarenessAttributes);
    }

    /** The common case: a pool with no special rules. */
    public static PoolDefinition of(PoolId pool, PoolKind kind) {
        return new PoolDefinition(pool, kind, false, null, null, List.of(), false);
    }

    public byte[] toBytes() {
        return Json.write(this, "pool definition");
    }

    public static PoolDefinition from(byte[] bytes) {
        return Json.read(bytes, PoolDefinition.class, "pool definition");
    }
}
