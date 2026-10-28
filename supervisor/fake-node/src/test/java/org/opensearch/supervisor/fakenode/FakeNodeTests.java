package org.opensearch.supervisor.fakenode;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.AppliedOutcome;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives a node through the lifecycle a supervisor would put it through, and checks the store afterwards.
 *
 * <p>These are as much a review of {@code NODE_CONTRACT.md} as of the code: each one corresponds to a
 * sentence in that document, and the two that were hardest to write are the two the document had not
 * decided (an absent assignment, and what a node does when it cannot write).
 */
class FakeNodeTests {

    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String INDEX_UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private InMemoryKeyValueStore store;
    private MutableClock clock;
    private FakeNode node;

    /** Time only moves when a test says so. */
    private static final class MutableClock extends Clock {
        private Instant now = Instant.ofEpochMilli(1788969977000L);

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }

        void advance(Duration by) {
            now = now.plus(by);
        }
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        clock = new MutableClock();
        node = new FakeNode(store, template(), clock, "OYidGJ1ITdC2mMrZEW0BNQ");
    }

    private static Registration template() {
        return new Registration(
            NODE, "placeholder", "data-3", PoolId.of("shared-a"),
            EnumSet.of(NodeRole.DATA), "10.0.3.14", 9300, "3.7.0",
            Map.of("zone", "z1"), 5000, 0L
        );
    }

    private void assign(Map<Integer, ShardRole> shards, long version) {
        Assignment assignment = new Assignment(version, PoolId.of("shared-a"), 7, NODE, clock.millis(), Map.of(
            ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                INDEX_UUID, new Assignment.IndexAssignment("my-index", 3041, shards)
            ))
        ));
        store.put(Keys.nodeAssignment(NODE), NodeJson.write(assignment));
    }

    private Heartbeat heartbeat() {
        return NodeJson.readHeartbeat(store.get(Keys.nodeHeartbeat(NODE)).orElseThrow().value());
    }

    private ShardInventory storedInventory() {
        return NodeJson.readInventory(store.get(Keys.nodeShards(NODE)).orElseThrow().value());
    }

    // ---------------------------------------------------------------- registration

    @Test
    void startingWritesARegistrationTheSupervisorCanRead() {
        node.start();

        Registration written = NodeJson.readRegistration(
            store.get(Keys.nodeRegistration(NODE)).orElseThrow().value()
        );
        assertEquals(NODE, written.nodeId());
        assertEquals("OYidGJ1ITdC2mMrZEW0BNQ", written.ephemeralId());
        assertEquals(PoolId.of("shared-a"), written.declaredPool().orElseThrow());
        assertEquals(1788969977000L, written.registeredAt());
    }

    @Test
    void aRestartIsVisibleAsANewEphemeralId() {
        node.start();
        node.tick();
        String before = heartbeat().ephemeralId();

        FakeNode restarted = new FakeNode(store, template(), clock, "NEW_EPHEMERAL_ID_xxxxx");
        restarted.start();
        restarted.tick();

        assertNotEquals(before, heartbeat().ephemeralId());
        assertEquals("NEW_EPHEMERAL_ID_xxxxx", heartbeat().ephemeralId());
    }

    // ---------------------------------------------------------------- the heartbeat / inventory handshake

    @Test
    void aNodeWithNoAssignmentHeartbeatsAndHoldsNothing() {
        node.start();
        node.tick();

        Heartbeat beat = heartbeat();
        assertEquals(0, beat.shardCopies());
        assertEquals(Heartbeat.NO_ASSIGNMENT, beat.applied().assignmentVersion());

        // An *empty* inventory is still published. "I have nothing" and "I have not said" are different
        // facts about a node, and only one of them is safe to place shards against -- so the node states
        // the first explicitly rather than leaving the supervisor to infer it from shard_copies.
        assertEquals(0, storedInventory().copyCount());
        assertEquals(
            NodeJson.digest(store.get(Keys.nodeShards(NODE)).orElseThrow().value()),
            beat.inventoryDigest().orElseThrow()
        );
    }

    @Test
    void theDigestInTheHeartbeatMatchesTheBytesOfTheInventory() {
        // The handshake the whole three-document split rests on: the supervisor re-reads an inventory only
        // when this digest differs from the one it computed over the bytes it last read.
        node.start();
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();

        byte[] storedBytes = store.get(Keys.nodeShards(NODE)).orElseThrow().value();
        assertEquals(NodeJson.digest(storedBytes), heartbeat().inventoryDigest().orElseThrow());
        assertEquals(storedInventory().copyCount(), heartbeat().shardCopies());
    }

    @Test
    void anUnchangedInventoryIsNotRewritten() {
        // The reason the digest exists. Rewriting an unchanged inventory every few seconds would put the
        // document that scales with shard count back onto the hot write path.
        node.start();
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();

        long revisionAfterConverged = store.get(Keys.nodeShards(NODE)).orElseThrow().revision();
        clock.advance(Duration.ofSeconds(5));
        node.tick();
        node.tick();

        assertEquals(revisionAfterConverged, store.get(Keys.nodeShards(NODE)).orElseThrow().revision());
    }

    @Test
    void theHeartbeatIsRewrittenEveryTickEvenWhenNothingChanged() {
        // Liveness is derived from this key's revision advancing, so a heartbeat that stopped being written
        // because nothing changed would look exactly like a dead node. The timestamp guarantees the value
        // differs on every write, which is why NODE_CONTRACT.md makes it required rather than optional.
        node.start();
        node.tick();
        long first = store.get(Keys.nodeHeartbeat(NODE)).orElseThrow().revision();

        clock.advance(Duration.ofSeconds(5));
        node.tick();
        long second = store.get(Keys.nodeHeartbeat(NODE)).orElseThrow().revision();

        assertNotEquals(first, second);
        assertNotEquals(
            NodeJson.digest(store.get(Keys.nodeHeartbeat(NODE)).orElseThrow().value()),
            "unchanged"
        );
    }

    @Test
    void theInventoryIsWrittenBeforeTheHeartbeatThatAnnouncesIt() {
        // Ordering from the contract. A mismatch must always mean "the heartbeat is behind the inventory",
        // which is safe; the reverse would have the supervisor chase an inventory that does not exist.
        node.start();
        assign(Map.of(0, ShardRole.PRIMARY), 1);

        RecordingStore recording = new RecordingStore(store);
        FakeNode watched = new FakeNode(recording, template(), clock, "eph");
        watched.tick();

        int inventoryWrite = recording.writes.indexOf(Keys.nodeShards(NODE));
        int heartbeatWrite = recording.writes.indexOf(Keys.nodeHeartbeat(NODE));
        assertTrue(inventoryWrite >= 0, "inventory should have been written");
        assertTrue(inventoryWrite < heartbeatWrite, "inventory must be written before the heartbeat");
    }

    // ---------------------------------------------------------------- converging on the assignment

    @Test
    void aNewCopyAppearsAsInitializingBeforeItServes() {
        node.start();
        assign(Map.of(0, ShardRole.PRIMARY), 1);

        node.tick();
        // The copy already knows it is going to be a primary; only the state has to catch up. Before role
        // and state were split this was inexpressible, and an initializing primary was indistinguishable
        // from a serving one.
        assertCopy(0, ShardRole.PRIMARY, ShardState.INITIALIZING);
        assertFalse(copyOf(0).isActivePrimary());
        assertEquals(AppliedOutcome.APPLYING, heartbeat().applied().outcome());
        assertEquals(1, heartbeat().applied().assignmentVersion());

        node.tick();
        assertCopy(0, ShardRole.PRIMARY, ShardState.STARTED);
        assertTrue(copyOf(0).isActivePrimary());
        assertEquals(AppliedOutcome.APPLIED, heartbeat().applied().outcome());
    }

    @Test
    void aCopyTheAssignmentNoLongerNamesIsRemoved() {
        // The assignment is the complete desired set, never a delta. A data plane applying deltas would
        // have the same edge-triggered bug as an event-driven supervisor, one layer lower.
        node.start();
        assign(Map.of(0, ShardRole.PRIMARY, 1, ShardRole.REPLICA), 1);
        node.tick();
        node.tick();
        assertEquals(2, storedInventory().copyCount());

        assign(Map.of(0, ShardRole.PRIMARY), 2);
        node.tick();

        assertEquals(1, storedInventory().copyCount());
        assertCopy(0, ShardRole.PRIMARY, ShardState.STARTED);
        assertEquals(2, heartbeat().applied().assignmentVersion());
    }

    @Test
    void anEmptyAssignmentDropsEverything() {
        node.start();
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();

        store.put(Keys.nodeAssignment(NODE),
            NodeJson.write(new Assignment(2, PoolId.of("shared-a"), 7, NODE, clock.millis(), Map.of())));
        node.tick();

        assertEquals(0, storedInventory().copyCount());
        assertEquals(0, heartbeat().shardCopies());
    }

    @Test
    void aReplicaCanBePromotedToPrimary() {
        node.start();
        assign(Map.of(0, ShardRole.REPLICA), 1);
        node.tick();
        node.tick();
        assertCopy(0, ShardRole.REPLICA, ShardState.STARTED);

        assign(Map.of(0, ShardRole.PRIMARY), 2);
        node.tick();

        assertCopy(0, ShardRole.PRIMARY, ShardState.STARTED);
    }

    // ---------------------------------------------------------------- the two gaps in the contract

    private void writeIndex(String uuid, boolean open) {
        store.put(Keys.indicesPrefix(ACME) + "my-index", org.opensearch.supervisor.model.index.IndexConfig.toDocument(
            "my-index", uuid, open, org.opensearch.supervisor.model.index.IndexConfig.settingsOf(
                1, 0, org.opensearch.supervisor.model.status.Replication.SEGMENT)));
    }

    @Test
    void aClosedIndexIsReportedAsHoldingNoCopiesAndReopensByRecovering() {
        // NODE_CONTRACT.md: a closed index is reported as having no copies. Its entry stays in the assignment, and
        // the node still counts that assignment as applied.
        writeIndex(INDEX_UUID, true);
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();
        assertEquals(ShardState.STARTED, storedInventory().tenants().get(ACME).indices().get(INDEX_UUID)
            .shards().get(0).state());

        writeIndex(INDEX_UUID, false);
        node.tick();

        assertEquals(0, storedInventory().copyCount(), "a closed index's copies are not reported");
        assertEquals(AppliedOutcome.APPLIED, heartbeat().applied().outcome(), "and that is the assignment applied");

        writeIndex(INDEX_UUID, true);
        node.tick();
        assertEquals(ShardState.INITIALIZING, storedInventory().tenants().get(ACME).indices().get(INDEX_UUID)
            .shards().get(0).state(), "reopened, it recovers before it serves");
        node.tick();
        assertEquals(ShardState.STARTED, storedInventory().tenants().get(ACME).indices().get(INDEX_UUID)
            .shards().get(0).state());
    }

    @Test
    void aClosedNamesakeDoesNotCloseTheIndexTheAssignmentNames() {
        // Names are reused. A closed index of the same name and a different uuid is a different index.
        writeIndex("someOtherUuidAAAAAAAAA", false);
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();

        assertEquals(1, storedInventory().copyCount());
    }

    private void writeDocrepIndex() {
        store.put(Keys.indicesPrefix(ACME) + "my-index", org.opensearch.supervisor.model.index.IndexConfig.toDocument(
            "my-index", INDEX_UUID, true, org.opensearch.supervisor.model.index.IndexConfig.settingsOf(
                2, 1, org.opensearch.supervisor.model.status.Replication.DOCUMENT)));
    }

    private ShardInventory.ShardCopy storedCopy(int shard) {
        return storedInventory().tenants().get(ACME).indices().get(INDEX_UUID).shards().get(shard);
    }

    @Test
    void aDocumentReplicationCopyPublishesAStableAllocationId() {
        // Finding 12: a docrep primary's node builds this copy's routing entry from the id, so it must not change
        // while the copy exists -- through a promotion too.
        writeDocrepIndex();
        assign(Map.of(0, ShardRole.REPLICA), 1);
        node.tick();
        String minted = storedCopy(0).allocationId();
        node.tick();
        assertTrue(minted != null, "a docrep copy carries an id from the moment it exists");
        assertEquals(minted, storedCopy(0).allocationId(), "and keeps it once started");
        assertEquals(null, storedCopy(0).inSync(), "a replica has no in-sync set");

        assign(Map.of(0, ShardRole.PRIMARY), 2);
        node.tick();
        assertEquals(minted, storedCopy(0).allocationId(), "promotion keeps the id, as OpenSearch does");
        assertEquals(java.util.List.of(minted), storedCopy(0).inSync(), "a primary is in sync with itself");
    }

    @Test
    void aDocumentReplicationPrimaryCallsInSyncTheReplicasTheProjectionShowsStarted() {
        // Standing in for a primary's tracker: a replica it would have finished recovering.
        writeDocrepIndex();
        assign(Map.of(0, ShardRole.PRIMARY, 1, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();
        String own = storedCopy(0).allocationId();
        NodeId other = NodeId.of("72ZSKeRtQmuqOGyPvYzmLw");
        store.put(Keys.indexStatus(ACME, INDEX_UUID), new org.opensearch.supervisor.model.status.IndexStatus(
            "my-index", INDEX_UUID, 3041, org.opensearch.supervisor.model.status.HealthStatus.GREEN,
            org.opensearch.supervisor.model.status.Replication.DOCUMENT, 2, 1, 0, false, Map.of(
                0, java.util.List.of(
                    new org.opensearch.supervisor.model.status.IndexStatus.ShardPlacement(
                        NODE, ShardRole.PRIMARY, ShardState.STARTED, own, java.util.List.of(own)),
                    new org.opensearch.supervisor.model.status.IndexStatus.ShardPlacement(
                        other, ShardRole.REPLICA, ShardState.STARTED, "zzz-started-replica", null)),
                1, java.util.List.of(
                    new org.opensearch.supervisor.model.status.IndexStatus.ShardPlacement(
                        other, ShardRole.REPLICA, ShardState.INITIALIZING, "zzz-recovering-replica", null))),
            clock.millis(), 1).toBytes());

        node.tick();

        assertEquals(java.util.List.of(own, "zzz-started-replica"), storedCopy(0).inSync(), "sorted, itself included");
        assertEquals(java.util.List.of(storedCopy(1).allocationId()), storedCopy(1).inSync(),
            "a replica still recovering is not in sync");
    }

    @Test
    void aSegmentReplicationCopyPublishesNeitherField() {
        writeIndex(INDEX_UUID, true);
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();

        assertEquals(null, storedCopy(0).allocationId());
        assertEquals(null, storedCopy(0).inSync());
    }

    @Test
    void anAbsentAssignmentIsNotAnEmptyOne() {
        // GAP in NODE_CONTRACT.md: it says the assignment is the complete desired set, and says nothing
        // about a node that has no assignment key at all. Read as "the complete desired set is empty", a
        // node would drop every shard it holds the moment its assignment went missing -- which is what a
        // supervisor that has not yet written one, or a store that lost a key, both look like.
        //
        // Decided here as: absent means no instruction. The node keeps what it has.
        node.start();
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();
        assertEquals(1, storedInventory().copyCount());

        store.delete(Keys.nodeAssignment(NODE));
        node.tick();

        assertEquals(1, storedInventory().copyCount(), "an absent assignment must not drop shards");
        assertEquals(1, heartbeat().applied().assignmentVersion(), "still applying the last one it was given");
    }

    @Test
    void anUnreadableAssignmentIsReportedAndChangesNothing() {
        // The same hazard from the other direction: a corrupt write must not be read as "hold nothing".
        node.start();
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        node.tick();
        node.tick();

        store.put(Keys.nodeAssignment(NODE), "{ not json".getBytes(StandardCharsets.UTF_8));
        node.tick();

        assertEquals(1, storedInventory().copyCount());
        assertEquals(AppliedOutcome.FAILED, heartbeat().applied().outcome());
        assertTrue(heartbeat().applied().detail().contains("could not parse assignment"));
    }

    // ---------------------------------------------------------------- what the supervisor will read

    @Test
    void aNodeWritesOnlyItsOwnThreeKeys() {
        RecordingStore recording = new RecordingStore(store);
        FakeNode watched = new FakeNode(recording, template(), clock, "eph");
        watched.start();
        assign(Map.of(0, ShardRole.PRIMARY), 1);
        watched.tick();
        watched.tick();

        assertEquals(
            java.util.Set.of(Keys.nodeRegistration(NODE), Keys.nodeHeartbeat(NODE), Keys.nodeShards(NODE)),
            java.util.Set.copyOf(recording.writes)
        );

        // A node that wrote anywhere else -- into a tenant's subtree, or over its own assignment -- would
        // break the single-writer property the whole partition model rests on.
        assertFalse(recording.writes.contains(Keys.nodeAssignment(NODE)),
            "the node must never write its own assignment");
    }

    private ShardInventory.ShardCopy copyOf(int shard) {
        return storedInventory().tenants().get(ACME).indices().get(INDEX_UUID).shards().get(shard);
    }

    private void assertCopy(int shard, ShardRole role, ShardState state) {
        assertEquals(role, copyOf(shard).role(), "role of shard " + shard);
        assertEquals(state, copyOf(shard).state(), "state of shard " + shard);
    }

    /** Records the order of writes, which is the only way to assert the contract's ordering requirement. */
    private static final class RecordingStore implements KeyValueStore {
        private final KeyValueStore delegate;
        private final java.util.List<String> writes = new java.util.ArrayList<>();

        RecordingStore(KeyValueStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public java.util.Optional<Versioned> get(String key) {
            return delegate.get(key);
        }

        @Override
        public java.util.NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
            return delegate.scanPrefix(keyPrefix);
        }

        @Override
        public long put(String key, byte[] value) {
            writes.add(key);
            return delegate.put(key, value);
        }

        @Override
        public long put(String key, byte[] value, long expectedRevision) {
            writes.add(key);
            return delegate.put(key, value, expectedRevision);
        }

        @Override
        public boolean delete(String key) {
            return delegate.delete(key);
        }

        @Override
        public boolean delete(String key, long expectedRevision) {
            return delegate.delete(key, expectedRevision);
        }
    }
}
