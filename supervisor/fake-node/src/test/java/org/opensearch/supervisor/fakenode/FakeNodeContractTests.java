package org.opensearch.supervisor.fakenode;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.nodecontract.ContractCopy;
import org.opensearch.supervisor.nodecontract.ContractNode;
import org.opensearch.supervisor.nodecontract.NodeContractTests;
import org.opensearch.supervisor.nodecontract.RecordingStore;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * {@code fake-node} against the shared contract suite.
 *
 * <p>It should pass trivially — the suite was extracted from tests it already passed — and that is the
 * point of running it here. This is the reference reader of {@code NODE_CONTRACT.md}, so a suite it fails
 * is a suite that has misread the document, and finding that out here is much cheaper than finding it out
 * in the data plane repository where the natural conclusion would be that the plugin is wrong.
 *
 * <p>{@code FakeNodeTests} keeps the assertions that are about this implementation rather than about the
 * contract: converging on an assignment, promotion, an absent assignment, an unreadable one.
 */
class FakeNodeContractTests extends NodeContractTests {

    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");

    @Override
    protected ContractNode newNode(RecordingStore store) {
        return new FakeNodeUnderTest(store);
    }

    /**
     * The adapter.
     *
     * <p>The interesting method is {@link #holdServing}, and what it has to do is the reason the suite
     * asks for "hold these copies" rather than "here is your inventory": this node has no way to hold a
     * copy it was not assigned, so the only honest way to put it in a state is to write it an assignment
     * and let it converge. A real node's adapter does something completely different and both are correct,
     * which is exactly why the suite does not specify how.
     */
    private static final class FakeNodeUnderTest implements ContractNode {

        private final KeyValueStore supervisorWrites;
        private final MutableClock clock = new MutableClock();
        private final FakeNode node;
        private long assignmentVersion;

        FakeNodeUnderTest(RecordingStore store) {
            // The node writes through the recording store, so the suite sees exactly what it wrote. The
            // assignments this adapter has to plant to put the node into a state go through the unrecorded
            // one: they are the harness playing supervisor, and attributing them to the node would fail
            // the assertion that a node writes only its own three keys.
            this.supervisorWrites = store.asSupervisor();
            this.node = new FakeNode(store, template(), clock, "OYidGJ1ITdC2mMrZEW0BNQ");
        }

        private static Registration template() {
            return new Registration(
                NODE,
                "placeholder",
                "data-3",
                PoolId.of("shared-a"),
                EnumSet.of(NodeRole.DATA),
                "10.0.3.14",
                9300,
                "3.7.0",
                Map.of("zone", "z1"),
                5000,
                0L
            );
        }

        @Override
        public NodeId nodeId() {
            return node.nodeId();
        }

        @Override
        public String ephemeralId() {
            return node.ephemeralId();
        }

        @Override
        public void start() {
            node.start();
        }

        @Override
        public void pass() {
            node.tick();
        }

        @Override
        public void advanceClock(Duration by) {
            clock.advance(by);
        }

        @Override
        public void holdServing(List<ContractCopy> copies) {
            supervisorWrites.put(Keys.nodeAssignment(NODE), NodeJson.write(assignmentFor(copies)));
            // Two passes: a new copy appears as INITIALIZING and becomes serving on the following tick.
            // Ticking until the outcome is APPLIED would be more general and would also hide a node that
            // never converges, so the number is fixed and this implementation's own tests assert why.
            node.tick();
            node.tick();
        }

        private Assignment assignmentFor(List<ContractCopy> copies) {
            Map<TenantId, Map<String, IndexBuilder>> byTenant = new TreeMap<>();
            for (ContractCopy copy : copies) {
                byTenant.computeIfAbsent(copy.tenant(), t -> new TreeMap<>())
                    .computeIfAbsent(copy.indexUuid(), u -> new IndexBuilder(copy.indexName(), copy.configRevision())).shards
                        .put(copy.shard(), copy.role());
            }

            Map<TenantId, Assignment.TenantAssignment> tenants = new LinkedHashMap<>();
            byTenant.forEach((tenant, indices) -> {
                Map<String, Assignment.IndexAssignment> built = new LinkedHashMap<>();
                indices.forEach(
                    (uuid, builder) -> built.put(uuid, new Assignment.IndexAssignment(builder.name, builder.configRevision, builder.shards))
                );
                tenants.put(tenant, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), built));
            });

            return new Assignment(++assignmentVersion, PoolId.of("shared-a"), 7, NODE, clock.millis(), tenants);
        }

        private static final class IndexBuilder {
            private final String name;
            private final long configRevision;
            private final Map<Integer, ShardRole> shards = new TreeMap<>();

            IndexBuilder(String name, long configRevision) {
                this.name = name;
                this.configRevision = configRevision;
            }
        }
    }

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
}
