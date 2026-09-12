package org.opensearch.supervisor.nodecontract;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What every implementation of {@code NODE_CONTRACT.md}'s publishing half must do.
 *
 * <p>Extended by {@code fake-node}, which is the contract's reference reader, and by the real plugin in
 * {@code ../opensearch-data-plane}, which is its first implementation with a running OpenSearch node
 * behind it. The two share no code at all — the plugin re-implements the codec on XContent because the
 * server does not expose Jackson databind — so this suite is the only thing keeping them one format
 * rather than two.
 *
 * <p><b>Every assertion here reads the store with the supervisor's own reader</b>, deliberately. A suite
 * that checked each implementation against its own parser would pass on two mutually unreadable
 * documents, which is precisely the failure it exists to prevent. What is being tested is not "can this
 * node round-trip its own JSON" but "can the supervisor read what this node wrote".
 *
 * <p>Each test corresponds to a sentence in the contract, and the comment says which. A test here that
 * cannot be traced to one is either a missing paragraph in the document or an assertion about an
 * implementation detail; both are worth catching.
 */
public abstract class NodeContractTests {

    /**
     * A node under test, writing into the given store.
     *
     * <p>The store is supplied rather than obtained, because the suite wraps it to observe write order.
     * An adapter that has to plant something a supervisor would have written should do so through
     * {@link RecordingStore#asSupervisor()}, so that the harness's own writes are not attributed to the
     * node.
     */
    protected abstract ContractNode newNode(RecordingStore store);

    /**
     * Copies for a node to hold. Overridable so an implementation can use identifiers it understands;
     * the defaults are the contract document's own examples.
     */
    protected List<ContractCopy> exampleCopies() {
        return ContractFixtures.twoShardsOfOneIndex();
    }

    private InMemoryKeyValueStore backing;
    private RecordingStore store;
    private ContractNode node;

    @BeforeEach
    void createNode() {
        backing = new InMemoryKeyValueStore();
        store = new RecordingStore(backing);
        node = newNode(store);
    }

    // ---------------------------------------------------------------- registration

    @Test
    void startingPublishesARegistrationTheSupervisorCanRead() {
        node.start();

        Registration written = NodeJson.readRegistration(valueAt(Keys.nodeRegistration(node.nodeId())));
        assertEquals(node.nodeId(), written.nodeId());
        assertEquals(node.ephemeralId(), written.ephemeralId());
        // The supervisor derives this node's staleness threshold from the interval the node declares, so a
        // registration that does not carry one is a node it cannot decide is alive.
        assertTrue(written.heartbeatIntervalMillis() > 0, "a node must declare the interval it heartbeats at");
        assertNotNull(written.name());
        assertNotNull(written.address());
    }

    @Test
    void theRegistrationIsNotRewrittenWhenNothingAboutTheNodeChanged() {
        // "Written once when the node starts, and rewritten only if something in it changes." Rewriting it
        // every pass would be harmless for one node and 200 unnecessary writes per interval for a fleet --
        // and it would make a genuine change invisible in the revision history.
        node.start();
        node.pass();
        long revision = revisionAt(Keys.nodeRegistration(node.nodeId()));

        node.advanceClock(Duration.ofSeconds(30));
        node.pass();
        node.pass();

        assertEquals(revision, revisionAt(Keys.nodeRegistration(node.nodeId())));
    }

    // ---------------------------------------------------------------- the heartbeat / inventory handshake

    @Test
    void anEmptyInventoryIsStillPublished() {
        // "I have nothing" and "I have not yet said" are different facts, and only one of them is safe to
        // place shards against. A node with no copies publishes an empty document rather than leaving the
        // key absent, so the supervisor has one code path instead of two.
        node.start();
        node.pass();

        ShardInventory inventory = NodeJson.readInventory(valueAt(Keys.nodeShards(node.nodeId())));
        assertEquals(0, inventory.copyCount());
        assertEquals(node.nodeId(), inventory.nodeId());
        assertEquals(node.ephemeralId(), inventory.ephemeralId());
    }

    @Test
    void theHeartbeatsDigestIsTheDigestOfTheStoredInventoryBytes() {
        // The handshake the whole three-document split rests on: the supervisor re-reads an inventory only
        // when this digest differs from the one it computed over the bytes it last read. Note that the
        // digest is over stored bytes rather than a canonical form, which is what lets two implementations
        // with different serialisers agree.
        node.start();
        node.holdServing(exampleCopies());

        assertEquals(NodeJson.digest(valueAt(Keys.nodeShards(node.nodeId()))), heartbeat().inventoryDigest().orElseThrow());
    }

    @Test
    void shardCopiesOnTheHeartbeatMatchesTheInventory() {
        // The count is what lets an inconsistency be spotted without re-reading the inventory. It only
        // works if the two are computed from the same observation.
        node.start();
        node.holdServing(exampleCopies());

        ShardInventory inventory = NodeJson.readInventory(valueAt(Keys.nodeShards(node.nodeId())));
        assertEquals(inventory.copyCount(), heartbeat().shardCopies());
        assertEquals(exampleCopies().size(), heartbeat().shardCopies());
    }

    @Test
    void theInventoryIsWrittenBeforeTheHeartbeatThatAnnouncesIt() {
        // Stated as an ordering requirement in the contract. In this order a mismatch always means "the
        // heartbeat is behind the inventory", which is safe for the supervisor to act on; the reverse
        // would have it chase an inventory that does not exist yet.
        node.start();
        store.forgetWrites();
        node.holdServing(exampleCopies());

        List<String> writes = store.writes();
        int lastInventory = writes.lastIndexOf(Keys.nodeShards(node.nodeId()));
        int lastHeartbeat = writes.lastIndexOf(Keys.nodeHeartbeat(node.nodeId()));
        assertTrue(lastInventory >= 0, "the inventory should have been written: " + writes);
        assertTrue(lastHeartbeat >= 0, "the heartbeat should have been written: " + writes);
        assertTrue(lastInventory < lastHeartbeat, "the inventory must be written before the heartbeat: " + writes);
    }

    @Test
    void anUnchangedInventoryIsNotRewritten() {
        // The reason the digest exists, and the mistake found by implementing this once already: a node
        // that stamps generated_at on every recomputation produces new bytes and a new digest every
        // interval, so the supervisor re-reads the document that scales with shard count on every pass --
        // and the three-document split has bought nothing.
        node.start();
        node.holdServing(exampleCopies());
        long revision = revisionAt(Keys.nodeShards(node.nodeId()));
        long generatedAt = NodeJson.readInventory(valueAt(Keys.nodeShards(node.nodeId()))).generatedAt();

        node.advanceClock(Duration.ofSeconds(30));
        node.pass();
        node.pass();
        node.pass();

        assertEquals(revision, revisionAt(Keys.nodeShards(node.nodeId())), "the inventory must not be rewritten");
        assertEquals(
            generatedAt,
            NodeJson.readInventory(valueAt(Keys.nodeShards(node.nodeId()))).generatedAt(),
            "generated_at means when the inventory last changed, not when it was last computed"
        );
    }

    @Test
    void theHeartbeatIsRewrittenEveryPassEvenWhenNothingChanged() {
        // Liveness is measured from this key's revision advancing rather than from the node's own clock,
        // so a heartbeat suppressed as unchanged would make a healthy node look dead. The timestamp is
        // what guarantees the bytes differ, which is why the contract makes it required rather than
        // optional.
        node.start();
        node.pass();
        long first = revisionAt(Keys.nodeHeartbeat(node.nodeId()));

        node.advanceClock(Duration.ofSeconds(5));
        node.pass();

        assertNotEquals(first, revisionAt(Keys.nodeHeartbeat(node.nodeId())));
    }

    @Test
    void aChangedInventoryIsRewrittenAndTheDigestFollowsIt() {
        node.start();
        node.holdServing(exampleCopies());
        String firstDigest = heartbeat().inventoryDigest().orElseThrow();

        node.advanceClock(Duration.ofSeconds(5));
        node.holdServing(List.of(exampleCopies().get(0)));

        assertNotEquals(firstDigest, heartbeat().inventoryDigest().orElseThrow());
        assertEquals(NodeJson.digest(valueAt(Keys.nodeShards(node.nodeId()))), heartbeat().inventoryDigest().orElseThrow());
        assertEquals(1, heartbeat().shardCopies());
    }

    // ---------------------------------------------------------------- what is in the inventory

    @Test
    void theInventoryReportsEachCopyUnderItsTenantAndIndexUuid() {
        // Keyed by uuid, with the name alongside for humans. A reconciler keyed on the name will
        // eventually confuse an index with its deleted predecessor, because names are reused.
        node.start();
        List<ContractCopy> copies = exampleCopies();
        node.holdServing(copies);

        ShardInventory inventory = NodeJson.readInventory(valueAt(Keys.nodeShards(node.nodeId())));
        for (ContractCopy expected : copies) {
            ShardInventory.TenantShards tenant = inventory.tenants().get(expected.tenant());
            assertNotNull(tenant, "no entry for tenant " + expected.tenant());
            ShardInventory.IndexShards index = tenant.indices().get(expected.indexUuid());
            assertNotNull(index, "no entry for index uuid " + expected.indexUuid());
            assertEquals(expected.indexName(), index.name());
            assertEquals(expected.configRevision(), index.configRevision());

            ShardInventory.ShardCopy copy = index.shards().get(expected.shard());
            assertNotNull(copy, "no entry for shard " + expected.shard());
            assertEquals(expected.role(), copy.role());
            // Role and state are separate dimensions. A serving copy is STARTED whatever its role, and
            // active_primary_shards cannot be computed unless the two can be read apart.
            assertTrue(copy.state().isServing(), "a copy the node reports as held and serving must be STARTED");
        }
    }

    @Test
    void everyDocumentCarriesTheSameEphemeralId() {
        // The supervisor prefers the inventory over the heartbeat when the two disagree about this,
        // because the pair can straddle a restart. That rule only works if a single process is consistent.
        node.start();
        node.holdServing(exampleCopies());

        Registration registration = NodeJson.readRegistration(valueAt(Keys.nodeRegistration(node.nodeId())));
        ShardInventory inventory = NodeJson.readInventory(valueAt(Keys.nodeShards(node.nodeId())));
        assertEquals(node.ephemeralId(), registration.ephemeralId());
        assertEquals(node.ephemeralId(), inventory.ephemeralId());
        assertEquals(node.ephemeralId(), heartbeat().ephemeralId());
    }

    // ---------------------------------------------------------------- what a node may write

    @Test
    void aNodeWritesOnlyItsOwnThreeKeys() {
        // One key segment decides the writer, and the whole partition model rests on it: a node that wrote
        // its own assignment, or anything under a tenant prefix, would make a single-writer document have
        // two. Note that this is checkable by reading a key, which is why the layout is arranged this way.
        node.start();
        node.holdServing(exampleCopies());
        node.advanceClock(Duration.ofSeconds(5));
        node.pass();

        Set<String> permitted = Set.of(
            Keys.nodeRegistration(node.nodeId()),
            Keys.nodeHeartbeat(node.nodeId()),
            Keys.nodeShards(node.nodeId())
        );
        for (String written : store.writes()) {
            assertTrue(permitted.contains(written), "a node must not write [" + written + "]");
        }
        assertFalse(store.writes().contains(Keys.nodeAssignment(node.nodeId())), "a node must never write its own assignment");
    }

    @Test
    void nothingOutsideTheNodePrefixIsTouched() {
        node.start();
        node.holdServing(exampleCopies());

        NodeId other = NodeId.of("some-other-node");
        assertTrue(backing.get(Keys.nodeHeartbeat(other)).isEmpty());
        assertTrue(backing.scanPrefix("tenant/").isEmpty(), "a node writes nothing under a tenant prefix");
        assertTrue(backing.scanPrefix("supervisor/").isEmpty(), "a node writes nothing the supervisor owns");
    }

    // ---------------------------------------------------------------- helpers

    private Heartbeat heartbeat() {
        return NodeJson.readHeartbeat(valueAt(Keys.nodeHeartbeat(node.nodeId())));
    }

    private byte[] valueAt(String key) {
        return backing.get(key).orElseThrow(() -> new AssertionError("nothing was written to [" + key + "]")).value();
    }

    private long revisionAt(String key) {
        return backing.get(key).orElseThrow(() -> new AssertionError("nothing was written to [" + key + "]")).revision();
    }
}
