package org.opensearch.supervisor.nodecontract;

import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What every implementation of {@code NODE_CONTRACT.md}'s publishing half must do, as a list of named
 * checks rather than as a test class.
 *
 * <p><b>Why a list and not a JUnit base class.</b> The two implementations do not share a test framework and
 * cannot. {@code fake-node} is an ordinary JVM project on JUnit 5; the data plane is an OpenSearch plugin,
 * whose build runs JUnit 4 through {@code randomizedtesting} and whose {@code testingConventions} precommit
 * expects a unit test to extend {@code LuceneTestCase}. An abstract class carrying {@code @Test} annotations
 * therefore cannot be inherited by both — and the failure mode is the bad one: annotations the other side's
 * runner does not recognise are not an error, they are zero tests discovered and a green build.
 *
 * <p>So the checks are data. Each side runs them with its own runner — {@link #all()} for a framework that
 * can report per-check, {@link #runAll} for one that would rather have a single test — and a check added
 * here runs on both sides without either being edited. That is the property D9.1 is actually asking for;
 * inheritance was only ever a way to get it.
 *
 * <p><b>Every check reads the store with the supervisor's own reader</b>, deliberately. A suite that let each
 * implementation check its own JSON would pass happily on two mutually unreadable documents, which is
 * precisely the failure it exists to prevent. What is tested is not "can this node round-trip its own JSON"
 * but "can the supervisor read what this node wrote".
 *
 * <p>The assertions come from JUnit 5's {@code Assertions}, used purely as a library: it throws
 * {@code opentest4j}'s {@code AssertionFailedError}, which every runner in use reports as a failure. Nothing
 * here needs a JUnit 5 engine.
 */
public final class NodeContractChecks {

    private NodeContractChecks() {}

    /** How to build the node under test around a store the harness owns. */
    public interface NodeFactory {
        ContractNode create(RecordingStore store);
    }

    /** One named obligation from the contract. */
    public record Check(String name, Consumer<Harness> body) {}

    /**
     * A node, the store it writes to, and the reading helpers every check needs.
     *
     * <p>Fresh per check. Several checks assert that something was <em>not</em> rewritten, which only means
     * anything if no earlier check has already written it.
     */
    public static final class Harness {

        private final InMemoryKeyValueStore backing;
        private final RecordingStore store;
        private final ContractNode node;

        Harness(NodeFactory factory) {
            this.backing = new InMemoryKeyValueStore();
            this.store = new RecordingStore(backing);
            this.node = factory.create(store);
        }

        public ContractNode node() {
            return node;
        }

        public RecordingStore store() {
            return store;
        }

        public byte[] valueAt(String key) {
            return backing.get(key).orElseThrow(() -> new AssertionError("nothing was written to [" + key + "]")).value();
        }

        public long revisionAt(String key) {
            return backing.get(key).orElseThrow(() -> new AssertionError("nothing was written to [" + key + "]")).revision();
        }

        public Registration registration() {
            return NodeJson.readRegistration(valueAt(Keys.nodeRegistration(node.nodeId())));
        }

        public Heartbeat heartbeat() {
            return NodeJson.readHeartbeat(valueAt(Keys.nodeHeartbeat(node.nodeId())));
        }

        public ShardInventory inventory() {
            return NodeJson.readInventory(valueAt(Keys.nodeShards(node.nodeId())));
        }

        public InMemoryKeyValueStore backing() {
            return backing;
        }
    }

    /** A fresh node and store for one check to run against. */
    public static Harness harness(NodeFactory factory) {
        return new Harness(factory);
    }

    /** The copies a check asks a node to hold. The contract document's own example values. */
    public static List<ContractCopy> exampleCopies() {
        return ContractFixtures.twoShardsOfOneIndex();
    }

    /**
     * Runs every check against a freshly built node, and fails with all of them rather than the first.
     *
     * <p>For a runner that cannot easily present a list of checks as separate tests. Reporting every failure
     * matters more than it usually would: these checks overlap, so one broken field typically breaks
     * several, and the set that failed says which field far faster than the first one alone.
     */
    public static void runAll(NodeFactory factory) {
        List<String> failures = new ArrayList<>();
        for (Check check : all()) {
            try {
                check.body().accept(new Harness(factory));
            } catch (Throwable failure) {
                failures.add(check.name() + ": " + failure);
            }
        }
        if (failures.isEmpty() == false) {
            throw new AssertionError(
                failures.size() + " of " + all().size() + " node contract checks failed:\n  " + String.join("\n  ", failures)
            );
        }
    }

    /** Every obligation, in the order the contract states them. */
    public static List<Check> all() {
        List<Check> checks = new ArrayList<>();

        // ------------------------------------------------------------ registration

        checks.add(new Check("startingPublishesARegistrationTheSupervisorCanRead", harness -> {
            harness.node().start();

            Registration written = harness.registration();
            assertEquals(harness.node().nodeId(), written.nodeId());
            assertEquals(harness.node().ephemeralId(), written.ephemeralId());
            // The supervisor derives this node's staleness threshold from the interval the node declares, so
            // a registration that does not carry one is a node it cannot decide is alive.
            assertTrue(written.heartbeatIntervalMillis() > 0, "a node must declare the interval it heartbeats at");
            assertNotNull(written.name());
            assertNotNull(written.address());
        }));

        checks.add(new Check("theRegistrationIsNotRewrittenWhenNothingAboutTheNodeChanged", harness -> {
            // "Written once when the node starts, and rewritten only if something in it changes." Rewriting
            // it every pass would be harmless for one node and 200 unnecessary writes per interval for a
            // fleet -- and it would make a genuine change invisible in the revision history.
            harness.node().start();
            harness.node().pass();
            long revision = harness.revisionAt(Keys.nodeRegistration(harness.node().nodeId()));

            harness.node().advanceClock(Duration.ofSeconds(30));
            harness.node().pass();
            harness.node().pass();

            assertEquals(revision, harness.revisionAt(Keys.nodeRegistration(harness.node().nodeId())));
        }));

        // ------------------------------------------------------------ the heartbeat / inventory handshake

        checks.add(new Check("anEmptyInventoryIsStillPublished", harness -> {
            // "I have nothing" and "I have not yet said" are different facts, and only one of them is safe
            // to place shards against. A node with no copies publishes an empty document rather than
            // leaving the key absent, so the supervisor has one code path instead of two.
            harness.node().start();
            harness.node().pass();

            assertEquals(0, harness.inventory().copyCount());
            assertEquals(harness.node().nodeId(), harness.inventory().nodeId());
            assertEquals(harness.node().ephemeralId(), harness.inventory().ephemeralId());
        }));

        checks.add(new Check("theHeartbeatsDigestIsTheDigestOfTheStoredInventoryBytes", harness -> {
            // The handshake the whole three-document split rests on: the supervisor re-reads an inventory
            // only when this digest differs from the one it computed over the bytes it last read. Note that
            // the digest is over stored bytes rather than a canonical form, which is what lets two
            // implementations with different serialisers agree.
            harness.node().start();
            harness.node().holdServing(exampleCopies());

            assertEquals(
                NodeJson.digest(harness.valueAt(Keys.nodeShards(harness.node().nodeId()))),
                harness.heartbeat().inventoryDigest().orElseThrow()
            );
        }));

        checks.add(new Check("shardCopiesOnTheHeartbeatMatchesTheInventory", harness -> {
            // The count is what lets an inconsistency be spotted without re-reading the inventory. It only
            // works if the two are computed from the same observation.
            harness.node().start();
            harness.node().holdServing(exampleCopies());

            assertEquals(harness.inventory().copyCount(), harness.heartbeat().shardCopies());
            assertEquals(exampleCopies().size(), harness.heartbeat().shardCopies());
        }));

        checks.add(new Check("theInventoryIsWrittenBeforeTheHeartbeatThatAnnouncesIt", harness -> {
            // Stated as an ordering requirement in the contract. In this order a mismatch always means "the
            // heartbeat is behind the inventory", which is safe for the supervisor to act on; the reverse
            // would have it chase an inventory that does not exist yet.
            harness.node().start();
            harness.store().forgetWrites();
            harness.node().holdServing(exampleCopies());

            List<String> writes = harness.store().writes();
            int lastInventory = writes.lastIndexOf(Keys.nodeShards(harness.node().nodeId()));
            int lastHeartbeat = writes.lastIndexOf(Keys.nodeHeartbeat(harness.node().nodeId()));
            assertTrue(lastInventory >= 0, "the inventory should have been written: " + writes);
            assertTrue(lastHeartbeat >= 0, "the heartbeat should have been written: " + writes);
            assertTrue(lastInventory < lastHeartbeat, "the inventory must be written before the heartbeat: " + writes);
        }));

        checks.add(new Check("anUnchangedInventoryIsNotRewritten", harness -> {
            // The reason the digest exists, and the mistake found by implementing this once already: a node
            // that stamps generated_at on every recomputation produces new bytes and a new digest every
            // interval, so the supervisor re-reads the document that scales with shard count on every pass
            // -- and the three-document split has bought nothing.
            harness.node().start();
            harness.node().holdServing(exampleCopies());
            long revision = harness.revisionAt(Keys.nodeShards(harness.node().nodeId()));
            long generatedAt = harness.inventory().generatedAt();

            harness.node().advanceClock(Duration.ofSeconds(30));
            harness.node().pass();
            harness.node().pass();
            harness.node().pass();

            assertEquals(
                revision,
                harness.revisionAt(Keys.nodeShards(harness.node().nodeId())),
                "the inventory must not be rewritten"
            );
            assertEquals(
                generatedAt,
                harness.inventory().generatedAt(),
                "generated_at means when the inventory last changed, not when it was last computed"
            );
        }));

        checks.add(new Check("theHeartbeatIsRewrittenEveryPassEvenWhenNothingChanged", harness -> {
            // Liveness is measured from this key's revision advancing rather than from the node's own clock,
            // so a heartbeat suppressed as unchanged would make a healthy node look dead. The timestamp is
            // what guarantees the bytes differ, which is why the contract makes it required rather than
            // optional.
            harness.node().start();
            harness.node().pass();
            long first = harness.revisionAt(Keys.nodeHeartbeat(harness.node().nodeId()));

            harness.node().advanceClock(Duration.ofSeconds(5));
            harness.node().pass();

            assertNotEquals(first, harness.revisionAt(Keys.nodeHeartbeat(harness.node().nodeId())));
        }));

        checks.add(new Check("aChangedInventoryIsRewrittenAndTheDigestFollowsIt", harness -> {
            harness.node().start();
            harness.node().holdServing(exampleCopies());
            String firstDigest = harness.heartbeat().inventoryDigest().orElseThrow();

            harness.node().advanceClock(Duration.ofSeconds(5));
            harness.node().holdServing(List.of(exampleCopies().get(0)));

            assertNotEquals(firstDigest, harness.heartbeat().inventoryDigest().orElseThrow());
            assertEquals(
                NodeJson.digest(harness.valueAt(Keys.nodeShards(harness.node().nodeId()))),
                harness.heartbeat().inventoryDigest().orElseThrow()
            );
            assertEquals(1, harness.heartbeat().shardCopies());
        }));

        // ------------------------------------------------------------ what is in the inventory

        checks.add(new Check("theInventoryReportsEachCopyUnderItsTenantAndIndexUuid", harness -> {
            // Keyed by uuid, with the name alongside for humans. A reconciler keyed on the name will
            // eventually confuse an index with its deleted predecessor, because names are reused.
            harness.node().start();
            List<ContractCopy> copies = exampleCopies();
            harness.node().holdServing(copies);

            ShardInventory inventory = harness.inventory();
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
        }));

        checks.add(new Check("everyDocumentCarriesTheSameEphemeralId", harness -> {
            // The supervisor prefers the inventory over the heartbeat when the two disagree about this,
            // because the pair can straddle a restart. That rule only works if a single process is
            // consistent.
            harness.node().start();
            harness.node().holdServing(exampleCopies());

            assertEquals(harness.node().ephemeralId(), harness.registration().ephemeralId());
            assertEquals(harness.node().ephemeralId(), harness.inventory().ephemeralId());
            assertEquals(harness.node().ephemeralId(), harness.heartbeat().ephemeralId());
        }));

        // ------------------------------------------------------------ what a node may write

        checks.add(new Check("aNodeWritesOnlyItsOwnThreeKeys", harness -> {
            // One key segment decides the writer, and the whole partition model rests on it: a node that
            // wrote its own assignment, or anything under a tenant prefix, would make a single-writer
            // document have two. Note that this is checkable by reading a key, which is why the layout is
            // arranged this way.
            harness.node().start();
            harness.node().holdServing(exampleCopies());
            harness.node().advanceClock(Duration.ofSeconds(5));
            harness.node().pass();

            NodeId nodeId = harness.node().nodeId();
            Set<String> permitted = Set.of(Keys.nodeRegistration(nodeId), Keys.nodeHeartbeat(nodeId), Keys.nodeShards(nodeId));
            for (String written : harness.store().writes()) {
                assertTrue(permitted.contains(written), "a node must not write [" + written + "]");
            }
            assertFalse(
                harness.store().writes().contains(Keys.nodeAssignment(nodeId)),
                "a node must never write its own assignment"
            );
        }));

        checks.add(new Check("nothingOutsideTheNodePrefixIsTouched", harness -> {
            harness.node().start();
            harness.node().holdServing(exampleCopies());

            NodeId other = NodeId.of("some-other-node");
            assertTrue(harness.backing().get(Keys.nodeHeartbeat(other)).isEmpty());
            assertTrue(harness.backing().scanPrefix("tenant/").isEmpty(), "a node writes nothing under a tenant prefix");
            assertTrue(harness.backing().scanPrefix("supervisor/").isEmpty(), "a node writes nothing the supervisor owns");
        }));

        return List.copyOf(checks);
    }
}
