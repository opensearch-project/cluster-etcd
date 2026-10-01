/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;


import org.opensearch.test.OpenSearchTestCase;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * The bytes, examined directly.
 *
 * <p>The shared conformance suite already proves the supervisor can read these documents, which is the
 * property that matters. What it cannot see is anything about the bytes themselves — determinism, the
 * difference between an omitted field and a zero, the budget — and those are exactly the properties that
 * fail silently. A digest that changed with map iteration order would still parse perfectly.
 */
public class NodeDocumentsTests extends OpenSearchTestCase {

    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String INDEX_UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private static String json(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // ---------------------------------------------------------------- registration

    public void testTheRegistrationHasTheFieldNamesTheContractSpecifies() {
        Registration registration = new Registration(
            NODE,
            "OYidGJ1ITdC2mMrZEW0BNQ",
            "data-3",
            PoolId.of("shared-a"),
            EnumSet.of(NodeRole.DATA, NodeRole.COORDINATOR),
            "10.0.3.14",
            9300,
            "3.9.0",
            Map.of("zone", "z1", "rack", "r7"),
            5000,
            1788969977933L
        );

        assertEquals(
            "{\"node_id\":\"pv1sDMfZRxSTMiW5VrbN9w\",\"ephemeral_id\":\"OYidGJ1ITdC2mMrZEW0BNQ\",\"name\":\"data-3\","
                + "\"pool\":\"shared-a\",\"roles\":[\"data\",\"coordinator\"],\"address\":\"10.0.3.14\",\"port\":9300,"
                + "\"version\":\"3.9.0\",\"attributes\":{\"rack\":\"r7\",\"zone\":\"z1\"},"
                + "\"heartbeat_interval_millis\":5000,\"registered_at\":1788969977933}",
            json(NodeDocuments.write(registration))
        );
    }

    public void testANodeThatDeclaredNoPoolOmitsTheFieldRatherThanSendingNull() {
        // Absent means "this node declared no pool", which pools/nodes/<id> may still answer. A null would
        // be a third state that means the same thing and that the supervisor would have to handle twice.
        Registration registration = registration(null);
        assertFalse(json(NodeDocuments.write(registration)).contains("pool"));
    }

    public void testAttributesAreSortedSoTheDocumentDoesNotChangeWhenNothingDid() {
        Map<String, String> oneOrder = new LinkedHashMap<>();
        oneOrder.put("zone", "z1");
        oneOrder.put("rack", "r7");
        Map<String, String> theOther = new LinkedHashMap<>();
        theOther.put("rack", "r7");
        theOther.put("zone", "z1");

        assertArrayEquals(
            NodeDocuments.write(registration(PoolId.of("p"), oneOrder)),
            NodeDocuments.write(registration(PoolId.of("p"), theOther))
        );
    }

    public void testTooManyAttributesIsRejectedRatherThanTruncated() {
        Map<String, String> tooMany = new LinkedHashMap<>();
        for (int i = 0; i <= Registration.MAX_ATTRIBUTES; i++) {
            tooMany.put("attribute-" + i, "value");
        }
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> registration(null, tooMany));
        assertTrue(e.getMessage().contains("over the contract's limit"));
    }

    // ---------------------------------------------------------------- heartbeat

    public void testTheHeartbeatHasTheFieldNamesTheContractSpecifies() {
        Heartbeat heartbeat = new Heartbeat(
            NODE,
            "OYidGJ1ITdC2mMrZEW0BNQ",
            1788969977663L,
            new Heartbeat.Applied(42, AppliedOutcome.APPLIED, null),
            "sha256:9f2c1b7e",
            137,
            new Heartbeat.Capacity(994663170048L, 856882249728L, 41, 5, 1.2)
        );

        assertEquals(
            "{\"node_id\":\"pv1sDMfZRxSTMiW5VrbN9w\",\"ephemeral_id\":\"OYidGJ1ITdC2mMrZEW0BNQ\","
                + "\"timestamp\":1788969977663,\"applied\":{\"assignment_version\":42,\"outcome\":\"applied\"},"
                + "\"shards_digest\":\"sha256:9f2c1b7e\",\"shard_copies\":137,"
                + "\"capacity\":{\"disk_total_bytes\":994663170048,\"disk_available_bytes\":856882249728,"
                + "\"heap_used_percent\":41,\"cpu_used_percent\":5,\"load_1m\":1.2}}",
            json(NodeDocuments.write(heartbeat))
        );
    }

    public void testANodeThatHasNeverWrittenAnInventoryOmitsTheDigest() {
        Heartbeat heartbeat = new Heartbeat(NODE, "eph", 1L, Heartbeat.Applied.NONE, null, 0, Heartbeat.Capacity.UNKNOWN);
        assertFalse(json(NodeDocuments.write(heartbeat)).contains("shards_digest"));
    }

    public void testFailureDetailIsTruncatedRatherThanBlowingTheBudget() {
        // The incident that produces failure detail is exactly the incident that produces a lot of it, and
        // this document has a hard 4 KiB budget.
        String enormous = "x".repeat(10_000);
        Heartbeat.Applied applied = Heartbeat.Applied.failed(3, enormous);

        assertEquals(Heartbeat.Applied.MAX_DETAIL_LENGTH, applied.detail().length());
        byte[] bytes = NodeDocuments.write(
            new Heartbeat(NODE, "eph", 1L, applied, null, 0, Heartbeat.Capacity.UNKNOWN)
        );
        assertTrue(bytes.length < NodeDocuments.HEARTBEAT_BUDGET_BYTES);
    }

    // ---------------------------------------------------------------- inventory

    public void testTheInventoryNestsCopiesByTenantAndIndexUuid() {
        ShardInventory inventory = new ShardInventory(
            NODE,
            "OYidGJ1ITdC2mMrZEW0BNQ",
            1788969977660L,
            List.of(
                ShardInventory.HeldCopy.started(ACME, INDEX_UUID, "my-index", 3041, 0, ShardRole.PRIMARY, 4823012L),
                ShardInventory.HeldCopy.initializing(ACME, INDEX_UUID, "my-index", 3041, 1, ShardRole.SEARCH_REPLICA)
            )
        );

        assertEquals(
            "{\"node_id\":\"pv1sDMfZRxSTMiW5VrbN9w\",\"ephemeral_id\":\"OYidGJ1ITdC2mMrZEW0BNQ\","
                + "\"generated_at\":1788969977660,\"tenants\":{\"acme\":{\"indices\":{\"uzKxLWZu4nPSrTknEcDpoA\":{"
                + "\"name\":\"my-index\",\"config_revision\":3041,\"shards\":{"
                + "\"0\":{\"role\":\"PRIMARY\",\"state\":\"STARTED\",\"size_bytes\":4823012},"
                + "\"1\":{\"role\":\"SEARCH_REPLICA\",\"state\":\"INITIALIZING\"}}}}}}}",
            json(NodeDocuments.write(inventory))
        );
    }

    public void testADocrepCopyCarriesItsAllocationIdAndAPrimaryItsInSyncSet() {
        ShardInventory inventory = inventoryOf(
            docrep(0, ShardRole.PRIMARY, "ULHF36LvSIa6sReNU-zHoA", List.of("yfU2ZTYYQEWWtXEy7SDTZQ", "ULHF36LvSIa6sReNU-zHoA")),
            docrep(1, ShardRole.REPLICA, "3YLXESN2TkSC5FYhEDzvNg", null)
        );

        String json = json(NodeDocuments.write(inventory));

        assertTrue(json, json.contains("\"0\":{\"role\":\"PRIMARY\",\"state\":\"STARTED\",\"size_bytes\":4823012,"
            + "\"allocation_id\":\"ULHF36LvSIa6sReNU-zHoA\",\"in_sync\":[\"ULHF36LvSIa6sReNU-zHoA\",\"yfU2ZTYYQEWWtXEy7SDTZQ\"]}"));
        assertTrue(json, json.contains("\"1\":{\"role\":\"REPLICA\",\"state\":\"STARTED\",\"size_bytes\":4823012,"
            + "\"allocation_id\":\"3YLXESN2TkSC5FYhEDzvNg\"}"));
    }

    public void testTheInSyncSetIsWrittenInOneOrderWhateverOrderItWasReadIn() {
        // The inventory is rewritten only when its bytes change, and a set read from the tracker has no order.
        byte[] one = NodeDocuments.write(inventoryOf(docrep(0, ShardRole.PRIMARY, "b", List.of("b", "a", "c"))));
        byte[] other = NodeDocuments.write(inventoryOf(docrep(0, ShardRole.PRIMARY, "b", List.of("c", "b", "a"))));

        assertArrayEquals(one, other);
    }

    public void testOnlyAPrimaryHasAnInSyncSetAndItIsNeverEmpty() {
        expectThrows(IllegalArgumentException.class, () -> docrep(0, ShardRole.REPLICA, "a", List.of("a")));
        expectThrows(IllegalArgumentException.class, () -> docrep(0, ShardRole.PRIMARY, "a", List.of()));
    }

    private static ShardInventory.HeldCopy docrep(int shard, ShardRole role, String allocationId, List<String> inSync) {
        return new ShardInventory.HeldCopy(ACME, INDEX_UUID, "my-index", 3041, shard, role, ShardState.STARTED, 4_823_012L, null, allocationId, inSync);
    }

    public void testAnInitializingCopyOmitsItsSizeRatherThanReportingZero() {
        // Zero is a real size, and it would tell a disk-aware decider the copy costs nothing. Enforced at
        // construction rather than at serialisation, so the mistake cannot be made anywhere else either.
        expectThrows(
            IllegalArgumentException.class,
            () -> new ShardInventory.HeldCopy(
                ACME,
                INDEX_UUID,
                "my-index",
                3041,
                0,
                ShardRole.PRIMARY,
                ShardState.INITIALIZING,
                0L,
                null
            )
        );
        assertFalse(
            json(NodeDocuments.write(inventoryOf(ShardInventory.HeldCopy.initializing(ACME, INDEX_UUID, "my-index", 3041, 0,
                ShardRole.PRIMARY)))).contains("size_bytes")
        );
    }

    public void testAnEmptyInventoryIsAnEmptyTenantsMapRatherThanAnAbsentOne() {
        assertTrue(json(NodeDocuments.write(ShardInventory.empty(NODE, "eph", 1L))).contains("\"tenants\":{}"));
    }

    public void testTheSameCopiesAlwaysProduceTheSameBytes() {
        // The heartbeat announces a digest of these bytes, so serialisation that varied with insertion
        // order would announce a change every pass and put the document that scales with shard count back
        // onto the hot path.
        List<ShardInventory.HeldCopy> copies = new ArrayList<>();
        copies.add(ShardInventory.HeldCopy.started(TenantId.of("zzz"), "u2", "i2", 1, 3, ShardRole.PRIMARY, 1L));
        copies.add(ShardInventory.HeldCopy.started(ACME, INDEX_UUID, "my-index", 3041, 11, ShardRole.PRIMARY, 2L));
        copies.add(ShardInventory.HeldCopy.started(ACME, INDEX_UUID, "my-index", 3041, 2, ShardRole.PRIMARY, 3L));

        List<ShardInventory.HeldCopy> shuffled = new ArrayList<>(copies);
        java.util.Collections.reverse(shuffled);

        byte[] one = NodeDocuments.write(new ShardInventory(NODE, "eph", 7L, copies));
        byte[] other = NodeDocuments.write(new ShardInventory(NODE, "eph", 7L, shuffled));
        assertArrayEquals(one, other);
        assertEquals(NodeDocuments.digest(one), NodeDocuments.digest(other));
    }

    public void testTwoCopiesOfOneShardOnOneNodeIsRejected() {
        // Impossible in the routing table, and worth rejecting rather than emitting: the document nests
        // shards in a map, so the second would silently overwrite the first.
        expectThrows(
            IllegalArgumentException.class,
            () -> new ShardInventory(
                NODE,
                "eph",
                1L,
                List.of(
                    ShardInventory.HeldCopy.started(ACME, INDEX_UUID, "my-index", 3041, 0, ShardRole.PRIMARY, 1L),
                    ShardInventory.HeldCopy.started(ACME, INDEX_UUID, "my-index", 3041, 0, ShardRole.REPLICA, 1L)
                )
            )
        );
    }

    public void testAnInventoryOverTheBudgetIsRefusedRatherThanTruncated() {
        // D3.6, and the reason is worth restating: a truncated inventory is worse than a missing one,
        // because the supervisor reads copies absent from it as gone and starts rebuilding shards that
        // exist. The publisher turns this into outcome: failed.
        List<ShardInventory.HeldCopy> tooMany = new ArrayList<>();
        for (int shard = 0; shard < 20_000; shard++) {
            tooMany.add(ShardInventory.HeldCopy.started(ACME, INDEX_UUID, "my-index", 3041, shard, ShardRole.PRIMARY, 4823012L));
        }

        DocumentTooLargeException e = expectThrows(
            DocumentTooLargeException.class,
            () -> NodeDocuments.write(new ShardInventory(NODE, "eph", 1L, tooMany))
        );
        assertEquals(NodeDocuments.INVENTORY_BUDGET_BYTES, e.budgetBytes());
        assertTrue(e.actualBytes() > e.budgetBytes());
    }

    public void testTheMeasuredCostPerCopyIsStillWhatTheContractBudgetedFor() {
        // NODE_CONTRACT.md measured 93 bytes per copy and derived a ceiling of about 5,600 copies per node
        // from it, and says there is no headroom left for another per-copy field. This is that measurement,
        // run against the second implementation: a codec that quietly costs more would move the ceiling
        // without anyone noticing until a large node could not publish.
        int copies = 5_000;
        List<ShardInventory.HeldCopy> many = new ArrayList<>();
        for (int shard = 0; shard < copies; shard++) {
            many.add(ShardInventory.HeldCopy.started(ACME, INDEX_UUID, "my-index", 3041, shard, ShardRole.PRIMARY, 4823012L));
        }
        byte[] bytes = NodeDocuments.write(new ShardInventory(NODE, "eph", 1L, many));

        assertTrue(bytes.length + " bytes for " + copies + " copies", bytes.length < NodeDocuments.INVENTORY_BUDGET_BYTES);
        assertTrue(
            "the contract budgeted 93 bytes per copy; this codec uses " + (bytes.length / copies),
            bytes.length / copies <= 93
        );
    }

    // ---------------------------------------------------------------- the digest

    public void testTheDigestIsOverBytesAndIsPrefixedWithItsAlgorithm() {
        // Over stored bytes rather than a canonical form, which is what lets two implementations with
        // different serialisers agree without agreeing on a serialisation.
        byte[] bytes = NodeDocuments.write(ShardInventory.empty(NODE, "eph", 1L));
        assertTrue(NodeDocuments.digest(bytes).startsWith("sha256:"));
        assertEquals(7 + 64, NodeDocuments.digest(bytes).length());
        assertNotEquals(NodeDocuments.digest(bytes), NodeDocuments.digest(new byte[] { 1 }));
    }

    // ---------------------------------------------------------------- helpers

    private static ShardInventory inventoryOf(ShardInventory.HeldCopy... copies) {
        return new ShardInventory(NODE, "eph", 1L, List.of(copies));
    }

    private static Registration registration(PoolId pool) {
        return registration(pool, Map.of("zone", "z1"));
    }

    private static Registration registration(PoolId pool, Map<String, String> attributes) {
        return new Registration(
            NODE,
            "eph",
            "data-3",
            pool,
            EnumSet.of(NodeRole.DATA),
            "10.0.3.14",
            9300,
            "3.9.0",
            attributes,
            5000,
            1L
        );
    }
}
