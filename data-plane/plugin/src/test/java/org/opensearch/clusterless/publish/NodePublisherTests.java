/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

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
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


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
public class NodePublisherTests extends OpenSearchTestCase {

    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String INDEX_UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private InMemoryKeyValueStore backing;
    private FailableStore store;
    private AtomicLong now;
    private AtomicReference<List<ShardInventory.HeldCopy>> held;
    private AtomicReference<Registration> registration;
    private NodePublisher publisher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
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

    public void testAChangedAttributeRewritesTheRegistrationAndKeepsTheOriginalRegisteredAt() {
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

    public void testARegistrationDeletedUnderneathTheNodeIsWrittenAgain() {
        // The rule the supervisor asked for in DESIGN.md §20.2, and what makes reaping a departed node's
        // documents safe for it to do: a node that cannot notice its own registration vanishing ends up
        // heartbeating with no registration, which is NODE_UNREGISTERED -- placeable for nothing, and until now
        // needing a restart to get out of.
        publisher.publish();
        long registeredAt = publisher.status().registeredAt();
        assertEquals(0, publisher.status().registrationRewrites());

        backing.delete(Keys.nodeRegistration(NODE));
        assertTrue(backing.get(Keys.nodeRegistration(NODE)).isEmpty());

        now.addAndGet(60_000);
        publisher.publish();

        assertTrue("the registration must be back", backing.get(Keys.nodeRegistration(NODE)).isPresent());
        assertEquals(1, publisher.status().registrationRewrites());
        // The document is new; the registration is not. Both of these matter to a supervisor reading it: the
        // ephemeral id is how a restart is distinguished from a rewrite, and registered_at is how long this node
        // claims to have been here.
        assertEquals("registered_at must not move", registeredAt, publisher.status().registeredAt());
        assertEquals(EPHEMERAL, NodeJson.readRegistration(bytesAt(Keys.nodeRegistration(NODE))).ephemeralId());
        assertEquals("z1", NodeJson.readRegistration(bytesAt(Keys.nodeRegistration(NODE))).attributes().get("zone"));
    }

    public void testAQuietPassChecksTheRegistrationAndDoesNotRewriteIt() {
        // The second-pass property, and the one that would make this rule worse than not having it. Rewriting
        // whenever the document is merely *present* would put the registration back on the hot path -- a write
        // per pass, per node, of the document the three-document split exists to keep cold (D3.4) -- and the
        // revision advancing every pass is what the supervisor's liveness check reads as activity.
        publisher.publish();
        long revision = revisionOf(Keys.nodeRegistration(NODE));

        for (int pass = 0; pass < 5; pass++) {
            now.addAndGet(5_000);
            publisher.publish();
        }

        assertEquals("no pass may rewrite a registration that is still there", revision, revisionOf(Keys.nodeRegistration(NODE)));
        assertEquals(0, publisher.status().registrationRewrites());
    }

    public void testAStoreThatCannotBeReadIsNotTakenAsAMissingRegistration() {
        // The destructive misreading, in the same shape as "an absent assignment is not an empty one": a read
        // that failed says nothing about whether the document is there. Reported as a failure, and the next pass
        // decides again -- and the heartbeat still goes out, because a node that stopped heartbeating over this
        // would get its shards rebuilt elsewhere.
        //
        // Two claims, and it is worth knowing which assertion carries which, because they fail differently.
        // "Not mistaken for absence" is the rewrite count, and it holds trivially for an implementation that
        // never looks. "The failure is reported" is lastFailure, and that is the one that fails when the check
        // is taken out -- so this test also pins that the read happens at all.
        publisher.publish();
        long revision = revisionOf(Keys.nodeRegistration(NODE));

        store.failReadsTo(Keys.nodeRegistration(NODE));
        now.addAndGet(5_000);
        publisher.publish();

        assertEquals(revision, revisionOf(Keys.nodeRegistration(NODE)));
        assertEquals("a failed read is not a rewrite", 0, publisher.status().registrationRewrites());
        assertNotNull(publisher.status().lastFailure());
        assertTrue(publisher.status().lastFailure(), publisher.status().lastFailure().contains("registration"));
    }

    public void testARewriteIsReportedOnTheHeartbeatWithWhenItHappened() {
        // Asked for by the supervisor: noticing this is a monitoring job, and its monitoring reads the store rather
        // than a node's HTTP port, because it has to work when the supervisor is wedged and without reaching a node.
        // So the counter has to be in a document, and the heartbeat rather than the registration -- after a reap the
        // registration is absent, which is exactly the window where the count matters and where a reader would get
        // nothing at all.
        publisher.publish();
        assertFalse("nothing has happened, so the field must not be there", heartbeatJson().contains("registration_rewrites"));

        backing.delete(Keys.nodeRegistration(NODE));
        now.addAndGet(60_000);
        long whenRewritten = now.get();
        publisher.publish();

        String heartbeat = heartbeatJson();
        assertTrue(heartbeat, heartbeat.contains("\"registration_rewrites\":{\"count\":1,\"last_at\":" + whenRewritten + "}"));
    }

    public void testTheRewriteTimestampIsWhatMakesTheSignalClearable() {
        // The reason last_at earns a field of its own. The count is monotonic for the life of the process, so a
        // rule of "count above zero is an incident" fires once and never clears -- and an alert that cannot clear
        // gets suppressed. The timestamp has to move with each rewrite for "a rewrite in the last fifteen minutes"
        // to mean anything.
        publisher.publish();

        backing.delete(Keys.nodeRegistration(NODE));
        now.addAndGet(60_000);
        long first = now.get();
        publisher.publish();
        assertTrue(heartbeatJson(), heartbeatJson().contains("\"count\":1,\"last_at\":" + first));

        // Quiet passes must not move it: the field says when a rewrite last happened, not when it was last looked for.
        now.addAndGet(5_000);
        publisher.publish();
        assertTrue(heartbeatJson(), heartbeatJson().contains("\"count\":1,\"last_at\":" + first));

        backing.delete(Keys.nodeRegistration(NODE));
        now.addAndGet(60_000);
        long second = now.get();
        publisher.publish();
        assertTrue(heartbeatJson(), heartbeatJson().contains("\"count\":2,\"last_at\":" + second));
    }

    public void testAnAttributeChangeIsNotCountedAsARewrite() {
        // The semantics the contract has to state: only rewrites caused by absence count. An attribute change
        // rewrites the registration too and is entirely ordinary -- counting both would make the number mean
        // nothing, which is worse than not having it.
        publisher.publish();

        now.addAndGet(60_000);
        registration.set(registrationWith(Map.of("zone", "z1", "rack", "r7")));
        publisher.publish();

        assertEquals(0, publisher.status().registrationRewrites());
        assertFalse(heartbeatJson(), heartbeatJson().contains("registration_rewrites"));
    }

    public void testTheCountIsProcessLifetimeAndStartsAgainOnRestart() {
        // Stated as a test because a monitor differencing samples will see the count go backwards, and the correct
        // reading is "new process" rather than "lost event" -- ephemeral_id in the same document is how it tells.
        // A durable count would be the wrong answer: it would report an incident that a restart has already ended.
        publisher.publish();
        backing.delete(Keys.nodeRegistration(NODE));
        now.addAndGet(60_000);
        publisher.publish();
        assertEquals(1, publisher.status().registrationRewrites());

        NodePublisher restarted = new NodePublisher(
            store,
            NODE,
            "a-new-ephemeral-id",
            registration::get,
            () -> held.get(),
            CapacityProbe.UNKNOWN,
            now::get
        );
        now.addAndGet(5_000);
        restarted.publish();

        assertEquals("a fresh process has rewritten nothing", 0, restarted.status().registrationRewrites());
        assertFalse(heartbeatJson(), heartbeatJson().contains("registration_rewrites"));
    }

    // ---------------------------------------------------------------- the inventory that will not fit

    public void testAnOversizedInventoryWritesNothingAndReportsFailed() {
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

        assertEquals("nothing may be written", revisionBefore, revisionOf(Keys.nodeShards(NODE)));
        assertEquals("failed", outcome());
        assertTrue(appliedDetail().contains("over the contract's budget"));

        // The digest still describes the bytes that are in the store, because that is what the field
        // means. shard_copies reports what the node actually holds -- and the disagreement between the
        // two is precisely the signal that this node's inventory is stale.
        assertEquals(digestBefore, heartbeatDigest());
        assertEquals(20_000, shardCopies());
        assertEquals(2, storedCopyCount());
    }

    public void testAFailedInventoryDoesNotStopTheNodeHeartbeating() {
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

    public void testAnInventoryThatCannotBeComputedLeavesTheLastOneStanding() {
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
        assertEquals("the heartbeat is written anyway", now.get(), heartbeatTimestamp());
    }

    // ---------------------------------------------------------------- an unreachable store

    public void testAStoreOutageIsSurvivedAndTheNextPassRecovers() {
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

    public void testAnInventoryWriteThatFailedIsRetriedRatherThanRememberedAsWritten() {
        // The subtle half of the previous test. If the publisher recorded the inventory as written before
        // the write landed, it would see the copies as unchanged for ever afterwards and never retry --
        // while the heartbeat went on announcing a digest of bytes that are not in the store.
        publisher.publish();
        store.failWritesTo(Keys.nodeShards(NODE));
        held.set(copies(1));
        now.addAndGet(5000);
        publisher.publish();

        assertEquals("the write must not have landed", 0, storedCopyCount());

        store.recover();
        now.addAndGet(5000);
        publisher.publish();

        assertEquals(1, storedCopyCount());
        assertEquals(NodeDocuments.digest(bytesAt(Keys.nodeShards(NODE))), heartbeatDigest());
    }

    public void testTheHeartbeatNeverAnnouncesADigestOfBytesThatAreNotInTheStore() {
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

            assertEquals("after " + copies + " copies", NodeDocuments.digest(bytesAt(Keys.nodeShards(NODE))), heartbeatDigest());
        }
    }

    // ---------------------------------------------------------------- what the applier reports

    public void testAnAppliedOutcomeIsEchoedOnTheNextHeartbeat() {
        publisher.publish();
        assertEquals(Heartbeat.NO_ASSIGNMENT, appliedVersion());

        publisher.reportApplied(new Heartbeat.Applied(42, AppliedOutcome.APPLYING, null));
        now.addAndGet(5000);
        publisher.publish();

        assertEquals(42, appliedVersion());
        assertEquals("applying", outcome());
    }

    public void testAnInventoryFailureOutranksTheApplierButKeepsItsVersion() {
        publisher.reportApplied(new Heartbeat.Applied(42, AppliedOutcome.APPLIED, null));
        held.set(copies(20_000));
        publisher.publish();

        assertEquals("failed", outcome());
        assertEquals("the ack still says which assignment was applied", 42, appliedVersion());
    }

    // ---------------------------------------------------------------- diagnostics

    public void testTheStatusSeparatesWhenTheInventoryChangedFromWhenItWasWritten() {
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

    /** The heartbeat as the supervisor reads it, which is the only place a store-reading monitor can look. */
    private String heartbeatJson() {
        return new String(bytesAt(Keys.nodeHeartbeat(NODE)), StandardCharsets.UTF_8);
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
        private String failingReadKey;

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

        /**
         * Fails reads of one key and nothing else.
         *
         * <p>Narrow on purpose: {@link #failEverything} would also fail the heartbeat, and the heartbeat's
         * failure overwrites the reported problem — so a test about how a failed *read* is reported would end up
         * asserting on the heartbeat instead.
         */
        void failReadsTo(String key) {
            failingReadKey = key;
        }

        void recover() {
            failureMessage = null;
            failingKey = null;
            failingReadKey = null;
        }

        private void maybeFail(String key) {
            if (failureMessage != null && (failingKey == null || failingKey.equals(key))) {
                throw new IllegalStateException(failureMessage);
            }
        }

        @Override
        public Optional<Versioned> get(String key) {
            if (failingReadKey != null && failingReadKey.equals(key)) {
                throw new IllegalStateException("read of " + key + " failed");
            }
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
