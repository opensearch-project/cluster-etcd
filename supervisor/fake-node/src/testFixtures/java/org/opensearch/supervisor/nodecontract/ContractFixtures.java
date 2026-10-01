package org.opensearch.supervisor.nodecontract;

import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.ShardRole;

import java.util.List;

/**
 * The identifiers {@code NODE_CONTRACT.md}'s own examples use.
 *
 * <p>Shared so that two implementations of the contract are exercised with the same values, which matters
 * more than it looks: a uuid of {@code "index-1"} and a real base64 one differ in length by a factor of
 * three, and the inventory's size budget was measured against realistic ones.
 */
public final class ContractFixtures {

    public static final TenantId ACME = TenantId.of("acme");

    public static final String INDEX_UUID = "uzKxLWZu4nPSrTknEcDpoA";

    public static final String INDEX_NAME = "my-index";

    public static final long CONFIG_REVISION = 3041L;

    private ContractFixtures() {}

    /** One index, two shards, one of each of the roles a segment-replication deployment uses. */
    public static List<ContractCopy> twoShardsOfOneIndex() {
        return List.of(
            new ContractCopy(ACME, INDEX_UUID, INDEX_NAME, CONFIG_REVISION, 0, ShardRole.PRIMARY),
            new ContractCopy(ACME, INDEX_UUID, INDEX_NAME, CONFIG_REVISION, 1, ShardRole.PRIMARY)
        );
    }
}
