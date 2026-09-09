package org.opensearch.supervisor.model.node;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The size budgets from {@code SUPERVISOR_KEYS.md} §5, as tests.
 *
 * <p>A budget written in a document and checked by nothing is a comment. These generate documents at the
 * scale the budgets are meant to survive and assert the real serialised size, because the failure being
 * guarded against is silent: both backends cap value size, and a write that exceeds the cap fails at the
 * store rather than here, at a point where the node has already decided what its inventory is.
 */
class SizeBudgetTests {

    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");

    /** Realistic worst case: several thousand copies across many indices in several tenants. */
    private static ShardInventory inventoryWith(int tenants, int indicesPerTenant, int shardsPerIndex) {
        Map<TenantId, ShardInventory.TenantShards> byTenant = new LinkedHashMap<>();
        for (int t = 0; t < tenants; t++) {
            Map<String, ShardInventory.IndexShards> indices = new LinkedHashMap<>();
            for (int i = 0; i < indicesPerTenant; i++) {
                Map<Integer, ShardInventory.ShardCopy> shards = new LinkedHashMap<>();
                for (int s = 0; s < shardsPerIndex; s++) {
                    shards.put(s, ShardInventory.ShardCopy.of(
                        s % 2 == 0 ? ShardRole.PRIMARY : ShardRole.REPLICA,
                        ShardState.STARTED,
                        4_823_012L + s
                    ));
                }
                // Index uuids are 22 characters of base64url, so use something that length rather than "i1".
                indices.put("uzKxLWZu4nPSrTknEcD" + String.format("%03d", i), new ShardInventory.IndexShards(
                    "logs-application-" + i, 3041 + i, shards
                ));
            }
            byTenant.put(TenantId.of("tenant-" + t), new ShardInventory.TenantShards(indices));
        }
        return new ShardInventory(NODE, "OYidGJ1ITdC2mMrZEW0BNQ", 1788969977660L, byTenant);
    }

    @Test
    void fiveThousandShardCopiesFitTheInventoryBudget() {
        ShardInventory inventory = inventoryWith(10, 50, 10);
        assertEquals(5000, inventory.copyCount());

        int size = NodeJson.write(inventory).length;
        assertTrue(
            size <= NodeJson.INVENTORY_BUDGET_BYTES,
            "5000 copies serialised to " + size + " bytes, over the " + NodeJson.INVENTORY_BUDGET_BYTES + " budget"
        );
    }

    @Test
    void theInventoryBudgetIsWellClearOfWhatTheBackendsAllow() {
        // ZooKeeper's jute.maxbuffer defaults to 1 MiB and etcd caps a value at 1.5 MiB. The budget has to
        // leave room, because exceeding it at the store is a failure the node cannot do anything useful
        // about at that point.
        assertTrue(NodeJson.INVENTORY_BUDGET_BYTES < 1024 * 1024);
        assertTrue(NodeJson.ASSIGNMENT_BUDGET_BYTES < 1024 * 1024);
    }

    @Test
    void anInventoryOverBudgetRefusesToSerialiseRatherThanTruncating() {
        // NODE_CONTRACT.md: a node that would exceed the budget must write nothing and report
        // outcome: failed. A truncated inventory reads as "those copies are gone" and the supervisor starts
        // replacing shards that exist -- strictly worse than no inventory at all.
        ShardInventory enormous = inventoryWith(40, 100, 20);

        DocumentTooLargeException thrown = assertThrows(
            DocumentTooLargeException.class,
            () -> NodeJson.write(enormous)
        );
        assertEquals(NodeJson.INVENTORY_BUDGET_BYTES, thrown.budgetBytes());
        assertTrue(thrown.sizeBytes() > thrown.budgetBytes());
    }

    @Test
    void aFullyLoadedHeartbeatIsOrdersOfMagnitudeUnderItsBudget() {
        Heartbeat heartbeat = new Heartbeat(
            NODE,
            "OYidGJ1ITdC2mMrZEW0BNQ",
            1788969977663L,
            new Heartbeat.Applied(Long.MAX_VALUE, AppliedOutcome.FAILED, "x".repeat(Heartbeat.Applied.MAX_DETAIL_LENGTH)),
            "sha256:" + "f".repeat(64),
            Integer.MAX_VALUE,
            new Heartbeat.Capacity(Long.MAX_VALUE, Long.MAX_VALUE, 100, 100, 999.999)
        );

        int size = NodeJson.write(heartbeat).length;
        assertTrue(size < NodeJson.HEARTBEAT_BUDGET_BYTES, "worst-case heartbeat was " + size + " bytes");
        // The number that matters for R3.5: 200 nodes at this size every 5 seconds is the store's write load.
        assertTrue(size < 1024, "heartbeat should stay around a kilobyte, was " + size);
    }

    @Test
    void aFullyLoadedRegistrationFitsItsBudget() {
        Map<String, String> attributes = new LinkedHashMap<>();
        for (int i = 0; i < Registration.MAX_ATTRIBUTES; i++) {
            attributes.put("attribute-name-" + i, "v".repeat(60));
        }
        Registration registration = new Registration(
            NODE, "OYidGJ1ITdC2mMrZEW0BNQ", "data-node-with-a-long-name-3",
            PoolId.of("shared-pool-us-west-2a"),
            java.util.EnumSet.of(NodeRole.DATA, NodeRole.COORDINATOR),
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334", 9300, "3.7.0-SNAPSHOT",
            attributes, 5000, 1788969977933L
        );

        int size = NodeJson.write(registration).length;
        assertTrue(size < NodeJson.REGISTRATION_BUDGET_BYTES, "worst-case registration was " + size + " bytes");
    }

    @Test
    void fiveThousandAssignedCopiesFitTheAssignmentBudget() {
        // The assignment is the mirror of the inventory and is smaller per copy -- a bare state string
        // rather than an object -- so if the inventory fits, this must too. Asserted rather than assumed,
        // because the supervisor refusing to write an assignment strands a node.
        Map<TenantId, Assignment.TenantAssignment> byTenant = new LinkedHashMap<>();
        for (int t = 0; t < 10; t++) {
            Map<String, Assignment.IndexAssignment> indices = new LinkedHashMap<>();
            for (int i = 0; i < 50; i++) {
                Map<Integer, ShardRole> shards = new LinkedHashMap<>();
                for (int s = 0; s < 10; s++) {
                    shards.put(s, s % 2 == 0 ? ShardRole.PRIMARY : ShardRole.REPLICA);
                }
                indices.put("uzKxLWZu4nPSrTknEcD" + String.format("%03d", i),
                    new Assignment.IndexAssignment("logs-application-" + i, 3041 + i, shards));
            }
            byTenant.put(TenantId.of("tenant-" + t), new Assignment.TenantAssignment(NodeRole.DATA, indices));
        }
        Assignment assignment = new Assignment(42, PoolId.of("shared-a"), 7, NODE, 1L, byTenant);

        assertEquals(5000, assignment.copyCount());
        int size = NodeJson.write(assignment).length;
        assertTrue(size <= NodeJson.ASSIGNMENT_BUDGET_BYTES, "5000 assigned copies was " + size + " bytes");
        assertTrue(size < NodeJson.write(inventoryWith(10, 50, 10)).length, "assignment should be the smaller of the two");
    }
}
