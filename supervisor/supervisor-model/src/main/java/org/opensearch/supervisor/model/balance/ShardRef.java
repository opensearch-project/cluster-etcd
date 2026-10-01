package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.TenantId;

import java.util.Objects;

/** One shard, identified the only way that is safe across a delete-and-recreate: by index uuid. */
public record ShardRef(TenantId tenant, String indexUuid, int shard) {

    public ShardRef {
        Objects.requireNonNull(tenant, "tenant");
        Objects.requireNonNull(indexUuid, "indexUuid");
    }

    @Override
    public String toString() {
        return tenant + "/" + indexUuid + "/" + shard;
    }
}
