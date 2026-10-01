package org.opensearch.supervisor.nodecontract;

import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.ShardRole;

import java.util.Objects;

/**
 * One shard copy, as the conformance suite asks a node to hold it.
 *
 * <p>Not {@code ShardInventory.ShardCopy}: that one is nested under the tenant and index it belongs to,
 * because the document is, and a flat form is what a test needs to say "hold exactly these three". The
 * fields are the ones that identify a copy in the contract's vocabulary — and the identity is
 * {@code (tenant, index uuid, shard)}, never the name.
 *
 * @param indexName carried alongside for humans, and because the inventory has to report it
 * @param configRevision opaque, and comparable only for equality
 */
public record ContractCopy(TenantId tenant, String indexUuid, String indexName, long configRevision, int shard, ShardRole role) {

    public ContractCopy {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(indexUuid, "indexUuid");
        Objects.requireNonNull(indexName, "indexName");
        Objects.requireNonNull(role, "role");
    }

    @Override
    public String toString() {
        return tenant + "/" + indexName + "[" + indexUuid + "][" + shard + "] " + role;
    }
}
