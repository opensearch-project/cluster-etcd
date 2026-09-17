/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

/**
 * Reading an assignment, including the ways it can be wrong.
 *
 * <p>The failure cases carry most of the weight here. A node that misreads an assignment does not fail
 * visibly — it holds the wrong shards, or drops the right ones, and reports that it did so confidently.
 */
public class AssignmentTests extends OpenSearchTestCase {

    public void testTheRolesArrayIsReadAndSoIsTheRoleStringThatPrecededIt() {
        // Found by a coordinator that read nothing at all. The supervisor moved from `role` to `roles` so that a
        // node could be told it holds data *and* coordinates; a reader that understood only one spelling treated
        // every tenant of a differently-versioned supervisor as unintelligible. The two services are deployed and
        // upgraded separately, so one of them being older is a state to survive rather than an accident.
        Assignment current = read("""
            {"version": 1, "tenants": {"default": {"roles": ["coordinator"], "indices": {}}}}""");
        assertEquals(java.util.Set.of(NodeRole.COORDINATOR), current.tenants().get(TenantId.of("default")).roles());
        assertEquals(java.util.List.of(TenantId.of("default")), current.coordinatorTenants());

        Assignment older = read("""
            {"version": 1, "tenants": {"default": {"role": "coordinator", "indices": {}}}}""");
        assertEquals(java.util.Set.of(NodeRole.COORDINATOR), older.tenants().get(TenantId.of("default")).roles());
        assertEquals(java.util.List.of(TenantId.of("default")), older.coordinatorTenants());
    }

    public void testBothRolesAtOnceIsReadAsBoth() {
        Assignment assignment = read("""
            {"version": 1, "tenants": {"default": {"roles": ["data", "coordinator"],
              "indices": {"uzKxLWZu4nPSrTknEcDpoA": {"name": "my-index", "config_revision": 3041,
                "shards": {"0": "PRIMARY"}}}}}}""");

        Assignment.TenantAssignment tenant = assignment.tenants().get(TenantId.of("default"));
        assertTrue(tenant.holdsData());
        assertTrue(tenant.coordinates());
        assertEquals(1, assignment.copyCount());
        assertEquals(java.util.List.of(TenantId.of("default")), assignment.coordinatorTenants());
    }

    public void testARoleThisBuildDoesNotKnowLeavesTheEntryUnderstoodByNothing() {
        Assignment assignment = read("""
            {"version": 1, "tenants": {"default": {"roles": ["archivist"], "indices": {}}}}""");

        Assignment.TenantAssignment tenant = assignment.tenants().get(TenantId.of("default"));
        assertFalse(tenant.understood());
        assertFalse(tenant.holdsData());
        assertFalse(tenant.coordinates());
    }

    private static Assignment read(String json) {
        return Assignment.read(json.getBytes(StandardCharsets.UTF_8));
    }

    /** The contract document's own example, field for field. */
    private static final String EXAMPLE = """
        {
          "version": 42,
          "pool": "shared-a",
          "epoch": 7,
          "node_id": "pv1sDMfZRxSTMiW5VrbN9w",
          "generated_at": 1788969977500,
          "tenants": {
            "acme": {
              "role": "data",
              "indices": {
                "uzKxLWZu4nPSrTknEcDpoA": {
                  "name": "my-index",
                  "config_revision": 3041,
                  "shards": { "0": "PRIMARY", "1": "REPLICA" }
                }
              }
            },
            "globex": { "role": "coordinator" }
          }
        }""";

    public void testTheContractsExampleReadsAsItLooks() {
        Assignment assignment = read(EXAMPLE);

        assertEquals(42, assignment.version());
        assertEquals(1788969977500L, assignment.generatedAt());
        assertEquals(2, assignment.tenants().size());
        assertEquals(2, assignment.copyCount());

        Assignment.TenantAssignment acme = assignment.tenants().get(TenantId.of("acme"));
        assertEquals(java.util.Set.of(NodeRole.DATA), acme.roles());
        Assignment.IndexAssignment index = acme.indices().get("uzKxLWZu4nPSrTknEcDpoA");
        assertEquals("my-index", index.name());
        assertEquals(3041L, index.configRevision());
        assertEquals(ShardRole.PRIMARY, index.shards().get(0));
        assertEquals(ShardRole.REPLICA, index.shards().get(1));

        assertEquals(java.util.Set.of(NodeRole.COORDINATOR), assignment.tenants().get(TenantId.of("globex")).roles());
        assertEquals(java.util.List.of(TenantId.of("globex")), assignment.coordinatorTenants());
    }

    public void testPoolAndEpochAreReadPastRatherThanActedOn() {
        // They exist so an assignment written by a supervisor that had already lost the pool is identifiable
        // after the fact. That is the supervisor's business: a node that acted on them would be deciding which
        // supervisor owns it, which is the one decision it must never make.
        Assignment assignment = read(EXAMPLE);
        assertEquals(42, assignment.version());
        // Nothing on the record exposes either field, which is the assertion -- if one appears, this fails to
        // compile and whoever added it has to justify it.
        assertEquals(3, Assignment.class.getRecordComponents().length);
    }

    public void testAnAssignmentThatNamesNothingIsLegalAndMeansDropEverything() {
        // The one document that means "hold nothing". Distinct from an absent key, which the caller handles and
        // which means "no instruction" -- see AssignmentPollerTests.
        Assignment assignment = read("{\"version\": 7, \"tenants\": {}}");
        assertEquals(7, assignment.version());
        assertEquals(0, assignment.copyCount());
        assertTrue(assignment.tenants().isEmpty());
    }

    public void testVersionZeroIsRefused() {
        // Zero is the heartbeat's "nothing applied yet", so an assignment numbered zero could not be
        // acknowledged distinguishably. Refused at the boundary rather than carried inward.
        MalformedDocumentException e = expectThrows(MalformedDocumentException.class, () -> read("{\"version\": 0, \"tenants\": {}}"));
        assertTrue(e.getMessage(), e.getMessage().contains("must start at 1"));
    }

    public void testUnknownFieldsAreIgnored() {
        // A rolling upgrade routinely puts the supervisor ahead of the node. Failing the parse would lose the
        // whole desired state over a field the node did not need.
        Assignment assignment = read("""
            {
              "version": 3,
              "something_new": {"nested": [1, 2, 3]},
              "tenants": {
                "acme": {
                  "role": "data",
                  "future_field": "ignored",
                  "indices": {
                    "uuid-1": {"name": "i", "config_revision": 1, "shards": {"0": "PRIMARY"}, "extra": true}
                  }
                }
              }
            }""");

        assertEquals(3, assignment.version());
        assertEquals(1, assignment.copyCount());
    }

    public void testAnUnknownShardRoleBecomesASentinelRatherThanFailingTheDocument() {
        // The forward-compatibility rule that matters most: one unrecognised role must not cost the node every
        // other shard in the assignment.
        Assignment assignment = read("""
            {
              "version": 3,
              "tenants": {"acme": {"role": "data", "indices": {
                "uuid-1": {"name": "i", "config_revision": 1, "shards": {"0": "PRIMARY", "1": "TIME_TRAVELLING_REPLICA"}}
              }}}
            }""");

        Assignment.IndexAssignment index = assignment.tenants().get(TenantId.of("acme")).indices().get("uuid-1");
        assertEquals(ShardRole.PRIMARY, index.shards().get(0));
        assertEquals(ShardRole.UNKNOWN, index.shards().get(1));
        assertEquals(java.util.List.of("acme/uuid-1/1"), assignment.unreadableRoles());
    }

    public void testASentinelCannotBeWrittenBack() {
        // Reading one is required; re-serialising it would fabricate a document claiming a value no supervisor
        // ever sent.
        expectThrows(IllegalStateException.class, ShardRole.UNKNOWN::wireName);
        expectThrows(IllegalStateException.class, NodeRole.UNKNOWN::wireName);
    }

    public void testAnUnknownTenantRoleBecomesASentinel() {
        Assignment assignment = read("{\"version\": 1, \"tenants\": {\"acme\": {\"role\": \"warehouse\"}}}");
        assertEquals(java.util.Set.of(NodeRole.UNKNOWN), assignment.tenants().get(TenantId.of("acme")).roles());
        assertTrue(assignment.coordinatorTenants().isEmpty());
    }

    public void testGarbageIsRejectedRatherThanReadAsEmpty() {
        // The caller must treat this as "no instruction". An empty assignment means "drop everything", so a
        // parse failure read that way would let one corrupt write empty a node.
        expectThrows(MalformedDocumentException.class, () -> read("{ not json"));
        expectThrows(MalformedDocumentException.class, () -> Assignment.read(new byte[0]));
    }

    public void testAConfigRevisionMayArriveAsAStringOrANumber() {
        // The supervisor writes a number. Nothing forbids a string, and reading one as zero would have the node
        // report a config revision it never built against.
        assertEquals(3041L, read("""
            {"version": 1, "tenants": {"acme": {"role": "data", "indices": {
              "u": {"name": "i", "config_revision": "3041", "shards": {"0": "PRIMARY"}}}}}}""").tenants()
            .get(TenantId.of("acme"))
            .indices()
            .get("u")
            .configRevision());
    }

    public void testAShardKeyThatIsNotANumberIsRefused() {
        MalformedDocumentException e = expectThrows(
            MalformedDocumentException.class,
            () -> read("""
                {"version": 1, "tenants": {"acme": {"role": "data", "indices": {
                  "u": {"name": "i", "config_revision": 1, "shards": {"first": "PRIMARY"}}}}}}""")
        );
        assertTrue(e.getMessage(), e.getMessage().contains("not a shard number"));
    }

    public void testAnIndexWithoutANameIsRefused() {
        // The name is how the index document is keyed, so an assignment without one names an index that cannot
        // be looked up. Better to reject the document than to resolve part of it.
        expectThrows(
            MalformedDocumentException.class,
            () -> read("""
                {"version": 1, "tenants": {"acme": {"role": "data", "indices": {
                  "u": {"config_revision": 1, "shards": {"0": "PRIMARY"}}}}}}""")
        );
    }

    public void testAnOversizedAssignmentIsRefused() {
        // The supervisor writes nothing rather than truncating, so an oversized document means something else
        // went wrong. Parsing half a desired state is the one outcome worse than refusing it.
        StringBuilder padding = new StringBuilder("{\"version\": 1, \"tenants\": {}, \"pad\": \"");
        padding.append("x".repeat(Assignment.BUDGET_BYTES));
        padding.append("\"}");

        MalformedDocumentException e = expectThrows(MalformedDocumentException.class, () -> read(padding.toString()));
        assertTrue(e.getMessage(), e.getMessage().contains("over the budget"));
    }

    // ---------------------------------------------------------------- the one instruction in the document

    public void testAnIngestionResetIsReadWithItsTaskId() {
        Assignment assignment = read("""
            {"version": 1, "tenants": {"acme": {"roles": ["data"], "indices": {
              "u": {"name": "logs", "config_revision": 3041, "shards": {"0": "PRIMARY"},
                    "ingestion_reset": {"id": "task-7",
                                        "shards": {"0": {"mode": "offset", "value": "1000"}}}}}}}}""");

        IngestionReset reset = assignment.tenants().get(TenantId.of("acme")).indices().get("u").reset();
        assertNotNull(reset);
        // The id is what idempotence keys on, so losing it would mean re-applying the reset every pass for as
        // long as the supervisor kept writing it.
        assertEquals("task-7", reset.id());
        assertTrue(reset.names(0));
        assertEquals(IngestionReset.Mode.OFFSET, reset.shards().get(0).mode());
        assertEquals("1000", reset.shards().get(0).value());
    }

    public void testTheModeIsReadInTheOneSpellingTheContractPins() {
        // The supervisor normalises to lower case on the way in, so lower case is what arrives. Read
        // case-insensitively anyway: the alternative is that a spelling change three repositories away silently
        // becomes an unknown mode, and the cost of tolerating it here is nothing.
        assertEquals(IngestionReset.Mode.OFFSET, IngestionReset.Mode.from("offset"));
        assertEquals(IngestionReset.Mode.OFFSET, IngestionReset.Mode.from("OFFSET"));
        assertEquals(IngestionReset.Mode.TIMESTAMP, IngestionReset.Mode.from("timestamp"));
        assertEquals(IngestionReset.Mode.UNKNOWN, IngestionReset.Mode.from("by_sequence_number"));
        assertEquals(IngestionReset.Mode.UNKNOWN, IngestionReset.Mode.from(null));
    }

    public void testAnUnknownModeIsKeptRatherThanDroppedAtParseTime() {
        // The opposite of an unknown shard role, which is a sentinel the resolver then leaves out. This has to
        // survive parsing so that the node can report it: nothing else would make an unapplied reset visible,
        // since the supervisor completes the task on this node's own acknowledgement.
        Assignment assignment = read("""
            {"version": 1, "tenants": {"acme": {"roles": ["data"], "indices": {
              "u": {"name": "logs", "config_revision": 1, "shards": {"0": "PRIMARY"},
                    "ingestion_reset": {"id": "task-7",
                                        "shards": {"0": {"mode": "by_sequence_number", "value": "5"}}}}}}}}""");

        IngestionReset reset = assignment.tenants().get(TenantId.of("acme")).indices().get("u").reset();
        assertNotNull("an unreadable mode must not lose the reset", reset);
        assertEquals(IngestionReset.Mode.UNKNOWN, reset.shards().get(0).mode());
    }

    public void testAnIndexWithNoResetHasNoneRatherThanAnEmptyOne() {
        // Which is every index almost all of the time, and null rather than an empty reset so that "nothing to
        // do" cannot be confused with "a reset naming no shards", which would be an instruction nobody can
        // acknowledge.
        Assignment assignment = read("""
            {"version": 1, "tenants": {"acme": {"roles": ["data"], "indices": {
              "u": {"name": "logs", "config_revision": 1, "shards": {"0": "PRIMARY"}}}}}}""");

        assertNull(assignment.tenants().get(TenantId.of("acme")).indices().get("u").reset());
    }

    public void testAResetWithNothingUsableInItIsDroppedRatherThanFailingTheDocument() {
        // One malformed field must not cost the node the rest of its desired state -- the rule every other
        // tolerant read here follows. Not silent: a reset nobody can acknowledge leaves the supervisor's task
        // incomplete, with its own reason, which is the visible outcome.
        String noId = """
            {"version": 1, "tenants": {"acme": {"roles": ["data"], "indices": {
              "u": {"name": "logs", "config_revision": 1, "shards": {"0": "PRIMARY"},
                    "ingestion_reset": {"shards": {"0": {"mode": "offset", "value": "1000"}}}}}}}}""";
        assertNull(read(noId).tenants().get(TenantId.of("acme")).indices().get("u").reset());

        String noShards = """
            {"version": 1, "tenants": {"acme": {"roles": ["data"], "indices": {
              "u": {"name": "logs", "config_revision": 1, "shards": {"0": "PRIMARY"},
                    "ingestion_reset": {"id": "task-7", "shards": {}}}}}}}""";
        assertNull(read(noShards).tenants().get(TenantId.of("acme")).indices().get("u").reset());

        // A pointer with no value is a position nobody asked for, which is worse than no reset at all.
        String noValue = """
            {"version": 1, "tenants": {"acme": {"roles": ["data"], "indices": {
              "u": {"name": "logs", "config_revision": 1, "shards": {"0": "PRIMARY"},
                    "ingestion_reset": {"id": "task-7", "shards": {"0": {"mode": "offset"}}}}}}}}""";
        assertNull(read(noValue).tenants().get(TenantId.of("acme")).indices().get("u").reset());
    }

    public void testTheRestOfTheAssignmentSurvivesAnUnusableReset() {
        // The point of dropping it rather than throwing: the shards are still built.
        Assignment assignment = read("""
            {"version": 1, "tenants": {"acme": {"roles": ["data"], "indices": {
              "u": {"name": "logs", "config_revision": 3041, "shards": {"0": "PRIMARY", "1": "PRIMARY"},
                    "ingestion_reset": {"id": ""}}}}}}""");

        Assignment.IndexAssignment index = assignment.tenants().get(TenantId.of("acme")).indices().get("u");
        assertNull(index.reset());
        assertEquals(2, index.shards().size());
        assertEquals(3041L, index.configRevision());
    }
}
