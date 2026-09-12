/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.clusterless.contract.AppliedOutcome;
import org.opensearch.clusterless.contract.Heartbeat;
import org.opensearch.clusterless.contract.Keys;
import org.opensearch.clusterless.contract.NodeDocuments;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.NodeRole;
import org.opensearch.clusterless.contract.PoolId;
import org.opensearch.clusterless.contract.Registration;
import org.opensearch.clusterless.contract.ShardInventory;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.node.NodeJson;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The publisher's behaviour under conditions the shared conformance suite cannot reach.
 *
 * <p>The suite in the supervisor repository covers what a healthy node must publish, and it covers it for
 * both implementations at once. What is here is the other half: what this one does when the store is
 * unreachable, when the inventory will not fit, and when the node cannot work out what it holds. Those are
 * failure modes of an implementation rather than clauses of a contract — and every one of them has a wrong
 * answer that looks reasonable.
 *
 * <p>Documents are read back with the <b>supervisor's</b> reader here too, for the reason the conformance
 * suite gives: this repository writes these documents and does not read them, so a parser written here to
 * match the writer would only prove the two agree with each other.
 */
class NodePublisherTests {

    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String INDEX_UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private InMemoryKeyValueStore backing;
    private FailableStore store;
    private AtomicLong now;
    private AtomicReference<List<ShardInventory.HeldCopy>> held;
    private AtomicReference<Registration> registration;
    private NodePublisher publisher;

    @BeforeEach
    void setUp() {
        backing = new InMemoryKeyValueStore();
        store = new FailableStore(backing);
        now = new AtomicLong(1788969977000L);
        held = new AtomicReference<>(List.of());
        registration = new AtomicReference<>(registrationWith(Map.of("zone", "z1")));
        publisher = new NodePublisher(store, NODE, EPHEMERAL, registration::get, () -> held.get(), CapacityProbe.UNKNOWN, now::get);
    }

    private static final String EPHEMERAL = "OYidGJ1ITdC2mMrZEW0BNQ";

    private static Registration registrationWith(Map<String, String> attributes) {
        return new Registration(
            NODE,
            EPHEMERAL,
            "data-3",
            PoolId.of("shared-a"),
            EnumSet.of(NodeRole.DATA),
            "10.0.3.14",
            9300,
            "3.9.0",
            attributes,
            5000,
            0L
        );
    }

    // ---------------------------------------------------------------- registration

    @Test
    void aChangedAttributeRewritesTheRegistrationAndKeepsTheOriginalRegisteredAt() {
        // "Written on start and when an attribute changes." The timestamp is deliberately not refreshed: a
        // node that had been up for a day should not look as though it had just arrived because somebody
        // relabelled its rack.
        publisher.publish();
        long registeredAt = publisher.status().registeredAt();
        long revision = revisionOf(Keys.nodeRegistration(NODE));

        now.addAndGet(60_000);
        registration.set(registrationWith(Map.of("zone", "z1", "rack", "r7")));
        publisher.publish();

        assertNotEquals(revision, revisionOf(Keys.nodeRegistration(NODE)));
        assertEquals(registeredAt, publisher.status().registeredAt());
        assertEquals("r7", NodeJson.readRegistration(bytesAt(Keys.nodeRegistration(NODE))).attributes().get("rack"));
        assertTrue(new String(bytesAt(Keys.nodeRegistration(NODE)), StandardCharsets.UTF_8).contains("\"registered_at\":" + registeredAt));
    }

    // ---------------------------------------------------------------- the inventory that will not fit

    @Test
    void anOversizedInventoryWritesNothingAndReportsFailed() {
        // D3.6. The wrong answer that looks reasonable is to write as much as fits: the supervisor reads
        // copies absent from an inventory as gone, so a truncated document is an instruction to rebuild
        // thousands of shards that exist.
        publisher.publish();
        held.set(copies(2));
        publisher.publish();
        long revisionBefore = revisionOf(Keys.nodeShards(NODE));
        String digestBefore = heartbeatDigest();

        now.addAndGet(5000);
        held.set(copies(20_000));
        publisher.publish();

        assertEquals(revisionBefore, revisionOf(Keys.nodeShards(NODE)), "nothing may be written");
        assertEquals("failed", outcome());
        assertTrue(appliedDetail().contains("over the contract's budget"));

        // The digest still describes the bytes that are in the store, because that is what the field
        // means. shard_copies reports what the node actually holds -- and the disagreement between the
        // two is precisely the signal that this node's inventory is stale.
        assertEquals(digestBefore, heartbeatDigest());
        assertEquals(20_000, shardCopies());
        assertEquals(2, storedCopyCount());
    }

    @Test
    void aFailedInventoryDoesNotStopTheNodeHeartbeating() {
        // A node whose inventory is too large is a node with a problem. A node that stopped heartbeating
        // because of it would be declared lost and every shard it holds rebuilt elsewhere, turning a
        // reporting problem into an outage.
        publisher.publish();
        held.set(copies(20_000));
        now.addAndGet(5000);
        publisher.publish();

        assertEquals(now.get(), heartbeatTimestamp());
    }

    // ---------------------------------------------------------------- a node that cannot read itself

    @Test
    void anInventoryThatCannotBeComputedLeavesTheLastOneStanding() {
        // The most destructive available reading of "I could not work out what I hold" is "I hold
        // nothing", and it is also the easiest one to write by accident: return an empty list on error.
        publisher.publish();
        held.set(copies(3));
        publisher.publish();
        long revision = revisionOf(Keys.nodeShards(NODE));

        NodePublisher failing = new NodePublisher(store, NODE, EPHEMERAL, registration::get, () -> {
            throw new IllegalStateException("cluster state unavailable");
        }, CapacityProbe.UNKNOWN, now::get);
        now.addAndGet(5000);
        failing.publish();

        assertEquals(revision, revisionOf(Keys.nodeShards(NODE)));
        assertEquals(3, storedCopyCount());
        assertEquals("failed", outcome());
        assertTrue(appliedDetail().contains("cluster state unavailable"));
        assertEquals(now.get(), heartbeatTimestamp(), "the heartbeat is written anyway");
    }

    // ---------------------------------------------------------------- an unreachable store

    @Test
    void aStoreOutageIsSurvivedAndTheNextPassRecovers() {
        // D6.5: losing the store is losing the ability to change, not the ability to work. Nothing here
        // may throw, because a scheduled task that throws stops being rescheduled on some executors, and a
        // node that silently stopped heartbeating would have its shards rebuilt elsewhere.
        publisher.publish();
        held.set(copies(1));

        store.failEverything("the store is unreachable");
        now.addAndGet(5000);
        publisher.publish();
        assertEquals(1, publisher.status().consecutiveFailures());
        assertTrue(publisher.status().lastFailure().contains("unreachable"));

        store.recover();
        now.addAndGet(5000);
        publisher.publish();

        assertEquals(0, publisher.status().consecutiveFailures());
        assertEquals(1, storedCopyCount());
        assertEquals(NodeDocuments.digest(bytesAt(Keys.nodeShards(NODE))), heartbeatDigest());
    }

    @Test
    void anInventoryWriteThatFailedIsRetriedRatherThanRememberedAsWritten() {
        // The subtle half of the previous test. If the publisher recorded the inventory as written before
        // the write landed, it would see the copies as unchanged for ever afterwards and never retry --
        // while the heartbeat went on announcing a digest of bytes that are not in the store.
        publisher.publish();
        store.failWritesTo(Keys.nodeShards(NODE));
        held.set(copies(1));
        now.addAndGet(5000);
        publisher.publish();

        assertEquals(0, storedCopyCount(), "the write must not have landed");

        store.recover();
        now.addAndGet(5000);
        publisher.publish();

        assertEquals(1, storedCopyCount());
        assertEquals(NodeDocuments.digest(bytesAt(Keys.nodeShards(NODE))), heartbeatDigest());
    }

    @Test
    void theHeartbeatNeverAnnouncesADigestOfBytesThatAreNotInTheStore() {
        // The invariant behind both of the above, asserted across a sequence that includes a failure. A
        // supervisor re-reads on a digest mismatch, so a digest describing a document that does not exist
        // sends it chasing one for ever.
        publisher.publish();
        for (int copies = 0; copies < 4; copies++) {
            held.set(copies(copies));
            if (copies == 2) {
                store.failWritesTo(Keys.nodeShards(NODE));
            } else {
                store.recover();
            }
            now.addAndGet(5000);
            publisher.publish();

            assertEquals(NodeDocuments.digest(bytesAt(Keys.nodeShards(NODE))), heartbeatDigest(), "after " + copies + " copies");
        }
    }

    // ---------------------------------------------------------------- what the applier reports

    @Test
    void anAppliedOutcomeIsEchoedOnTheNextHeartbeat() {
        publisher.publish();
        assertEquals(Heartbeat.NO_ASSIGNMENT, appliedVersion());

        publisher.reportApplied(new Heartbeat.Applied(42, AppliedOutcome.APPLYING, null));
        now.addAndGet(5000);
        publisher.publish();

        assertEquals(42, appliedVersion());
        assertEquals("applying", outcome());
    }

    @Test
    void anInventoryFailureOutranksTheApplierButKeepsItsVersion() {
        publisher.reportApplied(new Heartbeat.Applied(42, AppliedOutcome.APPLIED, null));
        held.set(copies(20_000));
        publisher.publish();

        assertEquals("failed", outcome());
        assertEquals(42, appliedVersion(), "the ack still says which assignment was applied");
    }

    // ---------------------------------------------------------------- diagnostics

    @Test
    void theStatusSeparatesWhenTheInventoryChangedFromWhenItWasWritten() {
        // D7.3, and the pair is the point: a large gap between them is the system working as designed, and
        // it is the fastest way to confirm from outside that a document is being left alone deliberately
        // rather than failing to be written.
        publisher.publish();
        held.set(copies(1));
        now.addAndGet(5000);
        publisher.publish();
        long changedAt = publisher.status().inventoryGeneratedAt();

        now.addAndGet(600_000);
        publisher.publish();

        assertEquals(changedAt, publisher.status().inventoryGeneratedAt());
        assertEquals(changedAt, publisher.status().inventoryWrittenAt());
        assertEquals(now.get(), publisher.status().heartbeatWrittenAt());
    }

    // ---------------------------------------------------------------- helpers

    private List<ShardInventory.HeldCopy> copies(int count) {
        List<ShardInventory.HeldCopy> copies = new ArrayList<>();
        for (int shard = 0; shard < count; shard++) {
            copies.add(ShardInventory.HeldCopy.started(ACME, INDEX_UUID, "my-index", 3041, shard, ShardRole.PRIMARY, 4823012L));
        }
        return copies;
    }

    private byte[] bytesAt(String key) {
        return backing.get(key).orElseThrow(() -> new AssertionError("nothing at [" + key + "]")).value();
    }

    private long revisionOf(String key) {
        return backing.get(key).orElseThrow(() -> new AssertionError("nothing at [" + key + "]")).revision();
    }

    private int storedCopyCount() {
        return NodeJson.readInventory(bytesAt(Keys.nodeShards(NODE))).copyCount();
    }

    private String heartbeatDigest() {
        return NodeJson.readHeartbeat(bytesAt(Keys.nodeHeartbeat(NODE))).shardsDigest();
    }

    private int shardCopies() {
        return NodeJson.readHeartbeat(bytesAt(Keys.nodeHeartbeat(NODE))).shardCopies();
    }

    private long heartbeatTimestamp() {
        return NodeJson.readHeartbeat(bytesAt(Keys.nodeHeartbeat(NODE))).timestamp();
    }

    /** The wire name, so this file does not have to import two enums that mean the same thing. */
    private String outcome() {
        return NodeJson.readHeartbeat(bytesAt(Keys.nodeHeartbeat(NODE))).applied().outcome().wireName();
    }

    private long appliedVersion() {
        return NodeJson.readHeartbeat(bytesAt(Keys.nodeHeartbeat(NODE))).applied().assignmentVersion();
    }

    private String appliedDetail() {
        return NodeJson.readHeartbeat(bytesAt(Keys.nodeHeartbeat(NODE))).applied().detail();
    }

    /** A store that can be made to fail, so the publisher's failure handling is reachable from a test. */
    private static final class FailableStore implements KeyValueStore {

        private final KeyValueStore delegate;
        private String failureMessage;
        private String failingKey;

        FailableStore(KeyValueStore delegate) {
            this.delegate = delegate;
        }

        void failEverything(String message) {
            failureMessage = message;
            failingKey = null;
        }

        void failWritesTo(String key) {
            failureMessage = "write to " + key + " failed";
            failingKey = key;
        }

        void recover() {
            failureMessage = null;
            failingKey = null;
        }

        private void maybeFail(String key) {
            if (failureMessage != null && (failingKey == null || failingKey.equals(key))) {
                throw new IllegalStateException(failureMessage);
            }
        }

        @Override
        public Optional<Versioned> get(String key) {
            maybeFail(key);
            return delegate.get(key);
        }

        @Override
        public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
            return delegate.scanPrefix(keyPrefix);
        }

        @Override
        public long put(String key, byte[] value) {
            maybeFail(key);
            return delegate.put(key, value);
        }

        @Override
        public long put(String key, byte[] value, long expectedRevision) {
            maybeFail(key);
            return delegate.put(key, value, expectedRevision);
        }

        @Override
        public boolean delete(String key) {
            maybeFail(key);
            return delegate.delete(key);
        }

        @Override
        public boolean delete(String key, long expectedRevision) {
            maybeFail(key);
            return delegate.delete(key, expectedRevision);
        }
    }
}
