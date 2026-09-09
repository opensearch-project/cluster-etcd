package org.opensearch.supervisor.model.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wire format, checked against the examples in {@code NODE_CONTRACT.md} rather than against itself.
 *
 * <p>A round-trip test proves the codec agrees with the codec, which it always will. These assert the
 * <em>field names</em>, because the data plane is a different codebase and the document is the only thing
 * the two sides share.
 */
class NodeJsonTests {

    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");
    private static final TenantId ACME = TenantId.of("acme");
    private static final ObjectMapper PLAIN = new ObjectMapper();

    private static JsonNode parse(byte[] bytes) throws Exception {
        return PLAIN.readTree(bytes);
    }

    // ---------------------------------------------------------------- registration

    private static Registration registration() {
        return new Registration(
            NODE,
            "OYidGJ1ITdC2mMrZEW0BNQ",
            "data-3",
            PoolId.of("shared-a"),
            EnumSet.of(NodeRole.DATA, NodeRole.COORDINATOR),
            "10.0.3.14",
            9300,
            "3.7.0",
            Map.of("zone", "z1", "rack", "r7"),
            5000,
            1788969977933L
        );
    }

    @Test
    void registrationFieldNamesMatchTheContract() throws Exception {
        JsonNode json = parse(NodeJson.write(registration()));

        assertEquals("pv1sDMfZRxSTMiW5VrbN9w", json.get("node_id").asText());
        assertEquals("OYidGJ1ITdC2mMrZEW0BNQ", json.get("ephemeral_id").asText());
        assertEquals("data-3", json.get("name").asText());
        assertEquals("shared-a", json.get("pool").asText());
        assertEquals("10.0.3.14", json.get("address").asText());
        assertEquals(9300, json.get("port").asInt());
        assertEquals("3.7.0", json.get("version").asText());
        assertEquals(5000, json.get("heartbeat_interval_millis").asLong());
        assertEquals(1788969977933L, json.get("registered_at").asLong());
        assertEquals("z1", json.get("attributes").get("zone").asText());

        // Roles are lowercase in the document, uppercase in the enum.
        Set<String> roles = new java.util.HashSet<>();
        json.get("roles").forEach(node -> roles.add(node.asText()));
        assertEquals(Set.of("data", "coordinator"), roles);
    }

    @Test
    void registrationRoundTrips() {
        Registration original = registration();
        assertEquals(original, NodeJson.readRegistration(NodeJson.write(original)));
    }

    @Test
    void aNodeMayDeclareNoPool() throws Exception {
        Registration noPool = new Registration(
            NODE, "eph", "data-3", null, EnumSet.of(NodeRole.DATA),
            "10.0.3.14", 9300, "3.7.0", Map.of(), 5000, 1L
        );
        JsonNode json = parse(NodeJson.write(noPool));

        // Omitted rather than null: absent from both the registration and pools/nodes/<id> is
        // NODE_UNPOOLED, a finding rather than a default.
        assertFalse(json.has("pool"));
        assertTrue(NodeJson.readRegistration(NodeJson.write(noPool)).declaredPool().isEmpty());
    }

    @Test
    void registrationEnforcesTheAttributeBounds() {
        Map<String, String> tooMany = new LinkedHashMap<>();
        for (int i = 0; i < Registration.MAX_ATTRIBUTES + 1; i++) {
            tooMany.put("attr-" + i, "v");
        }
        assertThrows(IllegalArgumentException.class, () -> new Registration(
            NODE, "eph", "n", null, EnumSet.of(NodeRole.DATA), "a", 1, "v", tooMany, 5000, 1L
        ));

        assertThrows(IllegalArgumentException.class, () -> new Registration(
            NODE, "eph", "n", null, EnumSet.of(NodeRole.DATA), "a", 1, "v",
            Map.of("zone", "z".repeat(Registration.MAX_ATTRIBUTE_LENGTH + 1)), 5000, 1L
        ));
    }

    @Test
    void aHeartbeatIntervalOfZeroIsRejected() {
        // The supervisor derives this node's staleness threshold from it. Zero would mean a threshold of
        // the floor for a node that may be heartbeating far more slowly, and the node would be declared
        // lost while perfectly healthy.
        assertThrows(IllegalArgumentException.class, () -> new Registration(
            NODE, "eph", "n", null, EnumSet.of(NodeRole.DATA), "a", 1, "v", Map.of(), 0, 1L
        ));
    }

    // ---------------------------------------------------------------- heartbeat

    private static Heartbeat heartbeat() {
        return new Heartbeat(
            NODE,
            "OYidGJ1ITdC2mMrZEW0BNQ",
            1788969977663L,
            new Heartbeat.Applied(42, AppliedOutcome.APPLIED, null),
            "sha256:9f2c1b7e",
            137,
            new Heartbeat.Capacity(994663170048L, 856882249728L, 41, 5, 1.2)
        );
    }

    @Test
    void heartbeatFieldNamesMatchTheContract() throws Exception {
        JsonNode json = parse(NodeJson.write(heartbeat()));

        assertEquals("pv1sDMfZRxSTMiW5VrbN9w", json.get("node_id").asText());
        assertEquals(1788969977663L, json.get("timestamp").asLong());
        assertEquals("sha256:9f2c1b7e", json.get("shards_digest").asText());
        assertEquals(137, json.get("shard_copies").asInt());

        assertEquals(42, json.get("applied").get("assignment_version").asLong());
        assertEquals("applied", json.get("applied").get("outcome").asText());
        assertFalse(json.get("applied").has("detail"), "null detail should be omitted, not written as null");

        JsonNode capacity = json.get("capacity");
        assertEquals(994663170048L, capacity.get("disk_total_bytes").asLong());
        assertEquals(856882249728L, capacity.get("disk_available_bytes").asLong());
        assertEquals(41, capacity.get("heap_used_percent").asInt());
        assertEquals(5, capacity.get("cpu_used_percent").asInt());

        // SNAKE_CASE would render load1m as "load1m"; the contract says load_1m. A naming strategy is not a
        // substitute for checking the document.
        assertEquals(1.2, capacity.get("load_1m").asDouble(), 0.0001);
    }

    @Test
    void heartbeatRoundTrips() {
        Heartbeat original = heartbeat();
        assertEquals(original, NodeJson.readHeartbeat(NodeJson.write(original)));
    }

    @Test
    void theHeartbeatDoesNotGrowWithShardCount() {
        // The entire reason the inventory is a separate document. R3.5: 200 nodes writing every few seconds
        // is the store's dominant write load, so this document must cost the same for a node holding one
        // copy and a node holding ten thousand.
        int small = NodeJson.write(heartbeat()).length;

        Heartbeat huge = new Heartbeat(
            NODE, "eph", 1L, new Heartbeat.Applied(9999, AppliedOutcome.APPLIED, null),
            "sha256:" + "a".repeat(64), 10_000, heartbeat().capacity()
        );
        int large = NodeJson.write(huge).length;

        assertTrue(Math.abs(large - small) < 100, "heartbeat size varied by " + Math.abs(large - small) + " bytes");
        assertTrue(large < NodeJson.HEARTBEAT_BUDGET_BYTES);
    }

    @Test
    void appliedDetailIsTruncatedRatherThanBlowingTheBudget() {
        // The document has a hard budget and a node may want to report a stack trace. Truncating one bounded
        // diagnostic string is safe; letting it push the heartbeat over its budget would stop the node
        // heartbeating at all, turning a shard failure into a lost node.
        Heartbeat withDetail = new Heartbeat(
            NODE, "eph", 1L,
            new Heartbeat.Applied(1, AppliedOutcome.FAILED, "x".repeat(5000)),
            null, 0, Heartbeat.Capacity.UNKNOWN
        );
        assertEquals(Heartbeat.Applied.MAX_DETAIL_LENGTH, withDetail.applied().detail().length());
        assertTrue(NodeJson.write(withDetail).length < NodeJson.HEARTBEAT_BUDGET_BYTES);
    }

    @Test
    void aNodeThatHasNeverWrittenAnInventoryHasNoDigest() throws Exception {
        Heartbeat fresh = new Heartbeat(
            NODE, "eph", 1L, Heartbeat.Applied.NONE, null, 0, Heartbeat.Capacity.UNKNOWN
        );
        JsonNode json = parse(NodeJson.write(fresh));

        assertFalse(json.has("shards_digest"));
        assertTrue(NodeJson.readHeartbeat(NodeJson.write(fresh)).inventoryDigest().isEmpty());
        assertEquals(Heartbeat.NO_ASSIGNMENT, fresh.applied().assignmentVersion());
    }

    // ---------------------------------------------------------------- inventory

    private static ShardInventory inventory() {
        return new ShardInventory(NODE, "OYidGJ1ITdC2mMrZEW0BNQ", 1788969977660L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                "uzKxLWZu4nPSrTknEcDpoA", new ShardInventory.IndexShards("my-index", 3041, Map.of(
                    0, ShardInventory.ShardCopy.of(ShardState.PRIMARY, 4823012L),
                    1, ShardInventory.ShardCopy.of(ShardState.INITIALIZING)
                ))
            ))
        ));
    }

    @Test
    void inventoryFieldNamesMatchTheContract() throws Exception {
        JsonNode json = parse(NodeJson.write(inventory()));

        assertEquals("pv1sDMfZRxSTMiW5VrbN9w", json.get("node_id").asText());
        assertEquals(1788969977660L, json.get("generated_at").asLong());

        JsonNode index = json.get("tenants").get("acme").get("indices").get("uzKxLWZu4nPSrTknEcDpoA");
        assertEquals("my-index", index.get("name").asText());
        assertEquals(3041, index.get("config_revision").asLong());
        assertEquals("PRIMARY", index.get("shards").get("0").get("state").asText());
        assertEquals(4823012L, index.get("shards").get("0").get("size_bytes").asLong());

        // A copy still initializing has no size yet, and the field is omitted rather than zero: zero is a
        // real size and would tell a disk-aware decider the copy costs nothing.
        assertEquals("INITIALIZING", index.get("shards").get("1").get("state").asText());
        assertFalse(index.get("shards").get("1").has("size_bytes"));
    }

    @Test
    void inventoryRoundTripsIncludingTenantMapKeys() {
        ShardInventory original = inventory();
        ShardInventory parsed = NodeJson.readInventory(NodeJson.write(original));

        assertEquals(original, parsed);
        // TenantId as a map key goes through a KeyDeserializer rather than @JsonValue, so it is worth
        // asserting that the key really came back as a validated TenantId.
        assertTrue(parsed.tenants().containsKey(ACME));
    }

    @Test
    void copyCountMatchesWhatTheHeartbeatWouldReport() {
        assertEquals(2, inventory().copyCount());
    }

    // ---------------------------------------------------------------- assignment

    private static Assignment assignment() {
        return new Assignment(42, PoolId.of("shared-a"), 7, NODE, 1788969977500L, Map.of(
            ACME, new Assignment.TenantAssignment(NodeRole.DATA, Map.of(
                "uzKxLWZu4nPSrTknEcDpoA", new Assignment.IndexAssignment("my-index", 3041, Map.of(
                    0, ShardState.PRIMARY,
                    1, ShardState.REPLICA
                ))
            )),
            TenantId.of("globex"), Assignment.TenantAssignment.coordinator()
        ));
    }

    @Test
    void assignmentFieldNamesMatchTheContract() throws Exception {
        JsonNode json = parse(NodeJson.write(assignment()));

        assertEquals(42, json.get("version").asLong());
        assertEquals("shared-a", json.get("pool").asText());
        assertEquals(7, json.get("epoch").asLong());
        assertEquals(1788969977500L, json.get("generated_at").asLong());

        JsonNode index = json.get("tenants").get("acme").get("indices").get("uzKxLWZu4nPSrTknEcDpoA");
        assertEquals("my-index", index.get("name").asText());
        assertEquals(3041, index.get("config_revision").asLong());
        // Bare strings, not objects: the assignment says what to be, the inventory says what is.
        assertEquals("PRIMARY", index.get("shards").get("0").asText());
        assertEquals("REPLICA", index.get("shards").get("1").asText());

        JsonNode globex = json.get("tenants").get("globex");
        assertEquals("coordinator", globex.get("role").asText());
        assertTrue(globex.get("indices").isEmpty());
    }

    @Test
    void assignmentRoundTrips() {
        Assignment original = assignment();
        assertEquals(original, NodeJson.readAssignment(NodeJson.write(original)));
    }

    @Test
    void onlyServingStatesCanBeAssigned() {
        // INITIALIZING and FAILED are things a node reports, not things it can be told to be. Allowing them
        // would let a supervisor ask for a state the node can never converge to, and the diff would then
        // never empty.
        for (ShardState state : new ShardState[] { ShardState.INITIALIZING, ShardState.FAILED, ShardState.UNKNOWN }) {
            assertThrows(
                IllegalArgumentException.class,
                () -> new Assignment.IndexAssignment("my-index", 1, Map.of(0, state)),
                state + " should not be assignable"
            );
        }
    }

    @Test
    void assignmentVersionsStartAtOne() {
        // Zero is reserved: the heartbeat uses it for "nothing applied yet", so an assignment numbered zero
        // would be indistinguishable from never having been given one.
        assertThrows(IllegalArgumentException.class,
            () -> new Assignment(0, null, 1, NODE, 1L, Map.of()));
        assertEquals(Heartbeat.NO_ASSIGNMENT, 0L);
    }

    // ---------------------------------------------------------------- the digest handshake

    @Test
    void theDigestIsOverTheExactBytesStored() {
        byte[] bytes = NodeJson.write(inventory());
        assertEquals(NodeJson.digest(bytes), NodeJson.digest(bytes.clone()));
        assertTrue(NodeJson.digest(bytes).startsWith("sha256:"));
        assertEquals("sha256:".length() + 64, NodeJson.digest(bytes).length());
    }

    @Test
    void serialisationIsDeterministicSoAnUnchangedInventoryKeepsItsDigest() {
        // If re-serialising the same inventory produced different bytes, the node would announce a new
        // digest on every heartbeat and the supervisor would re-read every inventory on every pass — the
        // exact cost the digest exists to avoid.
        Map<TenantId, ShardInventory.TenantShards> forward = new LinkedHashMap<>();
        forward.put(TenantId.of("aaa"), new ShardInventory.TenantShards(Map.of()));
        forward.put(TenantId.of("zzz"), new ShardInventory.TenantShards(Map.of()));

        Map<TenantId, ShardInventory.TenantShards> reverse = new LinkedHashMap<>();
        reverse.put(TenantId.of("zzz"), new ShardInventory.TenantShards(Map.of()));
        reverse.put(TenantId.of("aaa"), new ShardInventory.TenantShards(Map.of()));

        String a = NodeJson.digest(NodeJson.write(new ShardInventory(NODE, "eph", 1L, forward)));
        String b = NodeJson.digest(NodeJson.write(new ShardInventory(NODE, "eph", 1L, reverse)));

        assertEquals(a, b, "map iteration order must not change the bytes");
    }

    @Test
    void aChangedInventoryChangesTheDigest() {
        String before = NodeJson.digest(NodeJson.write(inventory()));

        ShardInventory after = new ShardInventory(NODE, "OYidGJ1ITdC2mMrZEW0BNQ", 1788969977660L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                "uzKxLWZu4nPSrTknEcDpoA", new ShardInventory.IndexShards("my-index", 3041, Map.of(
                    0, ShardInventory.ShardCopy.of(ShardState.PRIMARY, 4823012L),
                    1, ShardInventory.ShardCopy.of(ShardState.REPLICA, 51L)
                ))
            ))
        ));

        assertNotEquals(before, NodeJson.digest(NodeJson.write(after)));
    }

    // ---------------------------------------------------------------- forward compatibility

    @Test
    void unknownFieldsFromANewerNodeAreIgnored() {
        // A rolling upgrade makes this the normal case. Failing the parse would lose the whole document,
        // and for an inventory that reads as "this node has no shards" — the most destructive possible
        // misreading of an unrecognised field.
        String json = """
            {"node_id":"pv1sDMfZRxSTMiW5VrbN9w","ephemeral_id":"eph","generated_at":1,
             "tenants":{},"something_from_the_future":{"nested":true}}
            """;
        ShardInventory parsed = NodeJson.readInventory(json.getBytes(StandardCharsets.UTF_8));
        assertEquals(NODE, parsed.nodeId());
        assertTrue(parsed.tenants().isEmpty());
    }

    @Test
    void anUnknownShardStateIsNotMistakenForAHealthyCopy() {
        String json = """
            {"node_id":"pv1sDMfZRxSTMiW5VrbN9w","ephemeral_id":"eph","generated_at":1,"tenants":
             {"acme":{"indices":{"uu":{"name":"i","config_revision":1,
              "shards":{"0":{"state":"SOMETHING_NEW"}}}}}}}
            """;
        ShardInventory parsed = NodeJson.readInventory(json.getBytes(StandardCharsets.UTF_8));

        ShardInventory.ShardCopy copy = parsed.tenants().get(ACME).indices().get("uu").shards().get(0);
        assertEquals(ShardState.UNKNOWN, copy.state());
        assertFalse(copy.state().isServing(), "an unrecognised state must never count as serving");
        // Still counted as a copy: believing it absent would have the supervisor place a duplicate.
        assertEquals(1, parsed.copyCount());
    }

    @Test
    void theUnknownSentinelCannotBeWrittenBack() {
        // Round-tripping an unrecognised value would fabricate a document claiming a state that no node
        // reported. The guard is on the serializer rather than on @JsonValue: Jackson builds an enum's
        // deserializer by calling @JsonValue on every constant, so a throwing accessor would have broken
        // reading too — which is the one thing the sentinel exists to keep working.
        assertThrows(IllegalStateException.class, () -> ShardState.UNKNOWN.wireName());
        assertThrows(IllegalStateException.class, () -> NodeRole.UNKNOWN.wireName());
        assertThrows(IllegalStateException.class, () -> AppliedOutcome.UNKNOWN.wireName());
    }

    @Test
    void aDocumentCarryingTheSentinelCannotBeWritten() {
        // The guarantee that actually matters, as opposed to the accessor-level one above: reading a
        // document with an unknown state must work, and writing that document straight back out must not.
        String json = """
            {"node_id":"pv1sDMfZRxSTMiW5VrbN9w","ephemeral_id":"eph","generated_at":1,"tenants":
             {"acme":{"indices":{"uu":{"name":"i","config_revision":1,
              "shards":{"0":{"state":"SOMETHING_NEW"}}}}}}}
            """;
        ShardInventory parsed = NodeJson.readInventory(json.getBytes(StandardCharsets.UTF_8));

        assertThrows(IllegalArgumentException.class, () -> NodeJson.write(parsed));
    }

    @Test
    void malformedJsonIsReportedRatherThanReturningAnEmptyDocument() {
        assertThrows(
            NodeJson.MalformedDocumentException.class,
            () -> NodeJson.readInventory("this is not json".getBytes(StandardCharsets.UTF_8))
        );
    }

    @Test
    void aMissingOptionalFieldIsNullRatherThanAFailure() {
        String json = """
            {"node_id":"pv1sDMfZRxSTMiW5VrbN9w","ephemeral_id":"eph","timestamp":1,"shard_copies":0}
            """;
        Heartbeat parsed = NodeJson.readHeartbeat(json.getBytes(StandardCharsets.UTF_8));
        assertNull(parsed.shardsDigest());
        assertNull(parsed.applied());
        assertNull(parsed.capacity());
    }
}
