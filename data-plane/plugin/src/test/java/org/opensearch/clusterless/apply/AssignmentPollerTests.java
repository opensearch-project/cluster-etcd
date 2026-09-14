/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterState;
import org.opensearch.clusterless.contract.Keys;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.NodeRole;
import org.opensearch.clusterless.contract.PoolId;
import org.opensearch.clusterless.contract.Registration;
import org.opensearch.clusterless.publish.CapacityProbe;
import org.opensearch.clusterless.publish.NodePublisher;
import org.opensearch.clusterless.publish.ShardInventorySource;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The reading pass, and the one misreading in it that would empty a node.
 *
 * <p>Most of these are about what happens when the assignment is <em>not</em> a well-formed instruction. That is
 * where the asymmetry lives: reading a present assignment slightly wrong costs a shard, and reading an absent one
 * as "hold nothing" costs every shard the node has.
 */
public class AssignmentPollerTests extends OpenSearchTestCase {

    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");
    private static final String EPHEMERAL = "OYidGJ1ITdC2mMrZEW0BNQ";
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private InMemoryKeyValueStore store;
    private NodePublisher publisher;
    private AtomicLong now;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        store = new InMemoryKeyValueStore();
        now = new AtomicLong(1788969977000L);
        publisher = new NodePublisher(
            store,
            NODE,
            EPHEMERAL,
            () -> new Registration(
                NODE,
                EPHEMERAL,
                "data-3",
                PoolId.of("shared-a"),
                EnumSet.of(NodeRole.DATA),
                "10.0.3.14",
                9300,
                "3.9.0",
                Map.of(),
                5000,
                0L
            ),
            ShardInventorySource.EMPTY,
            CapacityProbe.UNKNOWN,
            now::get
        );
    }

    private AssignmentPoller poller(boolean apply) {
        return new AssignmentPoller(
            store,
            NODE,
            new DesiredStateResolver((tenant, name) -> Optional.of(TestFixtures.indexConfig(UUID))),
            () -> ClusterState.EMPTY_STATE,
            desired -> new DataNodeState(TestFixtures.localNode(NODE.value()), desired, MappingSource.WRAPPED, LocalShardStore.EMPTY),
            publisher,
            apply
        );
    }

    private void writeAssignment(String json) {
        store.put(Keys.nodeAssignment(NODE), json.getBytes(StandardCharsets.UTF_8));
    }

    private static String assignmentJson(long version, String shards) {
        return """
            {"version": %d, "tenants": {"acme": {"role": "data", "indices": {
              "%s": {"name": "my-index", "config_revision": 3041, "shards": %s}}}}}""".formatted(version, UUID, shards);
    }

    // ---------------------------------------------------------------- absent is not empty

    public void testAnAbsentAssignmentChangesNothingAtAll() {
        // The destructive misreading. An empty assignment means "drop every shard you hold", and a supervisor
        // that has not written one yet, a pool between owners and a lost key all look exactly like this.
        publisher.publish();
        AssignmentPoller poller = poller(false);

        poller.poll();

        assertEquals(0, poller.status().assignmentsRead());
        assertEquals(0, poller.status().assignmentVersion());
        assertEquals(0, poller.status().copies());
        assertNull(poller.status().lastFailure());
        // The applied ack is untouched: the contract says a node with no assignment keeps reporting the last
        // version it applied.
        assertEquals("applied", publisher.status().appliedOutcome().wireName());
        assertEquals(0, publisher.status().appliedAssignmentVersion());
    }

    public void testAnAssignmentThatDisappearsDoesNotRetractWhatWasRead() {
        writeAssignment(assignmentJson(7, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(false);
        poller.poll();
        assertEquals(1, poller.status().copies());

        store.delete(Keys.nodeAssignment(NODE));
        poller.poll();

        assertEquals("the last resolved state stands", 1, poller.status().copies());
        assertEquals(7, poller.status().assignmentVersion());
    }

    // ---------------------------------------------------------------- unreadable is not empty either

    public void testAnUnreadableAssignmentIsReportedAndChangesNothing() {
        writeAssignment(assignmentJson(7, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(false);
        poller.poll();

        writeAssignment("{ not json");
        poller.poll();

        assertEquals("the last resolved state stands", 1, poller.status().copies());
        assertTrue(poller.status().lastFailure(), poller.status().lastFailure().contains("could not parse"));

        publisher.publish();
        assertEquals("failed", publisher.status().appliedOutcome().wireName());
        assertEquals("the version acked is the last one understood", 7, publisher.status().appliedAssignmentVersion());
    }

    // ---------------------------------------------------------------- reading a real one

    public void testAnAssignmentIsResolvedAndAcknowledged() {
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\", \"1\": \"SEARCH_REPLICA\"}"));
        AssignmentPoller poller = poller(false);

        poller.poll();

        AssignmentPoller.PollStatus status = poller.status();
        assertEquals(1, status.assignmentsRead());
        assertEquals(42, status.assignmentVersion());
        assertEquals(1, status.indices());
        assertEquals(2, status.copies());
        assertTrue(status.deferred().toString(), status.deferred().isEmpty());

        publisher.publish();
        assertEquals(42, publisher.status().appliedAssignmentVersion());
    }

    public void testReadOnlyModeSaysFailedRatherThanPretendingToHaveApplied() {
        // The alternatives are both worse. "applied" is a lie the supervisor acts on -- it would stop expecting
        // anything to change. A permanent "applying" implies progress that is not coming.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(false);

        poller.poll();
        publisher.publish();

        assertFalse(poller.status().applyEnabled());
        assertEquals("failed", publisher.status().appliedOutcome().wireName());
        assertEquals(42, publisher.status().appliedAssignmentVersion());
        assertTrue(publisher.status().lastFailure() == null);
    }

    public void testDeferredCopiesAreVisibleInTheStatus() {
        // "The supervisor asked for a shard and this node did not build it" is otherwise invisible from both
        // ends: the store shows an unassigned shard and the node shows nothing.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\", \"1\": \"REPLICA\"}"));
        AssignmentPoller poller = poller(false);

        poller.poll();

        assertEquals(1, poller.status().copies());
        assertEquals(1, poller.status().deferred().size());
        assertTrue(poller.status().deferred().get(0), poller.status().deferred().get(0).contains("peer resolution"));
    }

    public void testAPresentAssignmentThatNamesNothingResolvesToNothing() {
        // The one document that means "drop everything". It has to be distinguishable from an absent key, and
        // this is the pair of tests that proves it is.
        writeAssignment("{\"version\": 9, \"tenants\": {}}");
        AssignmentPoller poller = poller(false);

        poller.poll();

        assertEquals(1, poller.status().assignmentsRead());
        assertEquals(9, poller.status().assignmentVersion());
        assertEquals(0, poller.status().copies());
    }

    // ---------------------------------------------------------------- the pass never throws

    public void testAStoreThatCannotBeReadDoesNotKillThePass() {
        // A scheduled task that throws stops being rescheduled on some executors, and a node that quietly
        // stopped reading its assignment would look healthy while ignoring every instruction it was given.
        AssignmentPoller poller = new AssignmentPoller(
            new TestFixtures.FailingStore("the store is unreachable"),
            NODE,
            new DesiredStateResolver((tenant, name) -> Optional.empty()),
            () -> ClusterState.EMPTY_STATE,
            desired -> new DataNodeState(TestFixtures.localNode(NODE.value()), desired, MappingSource.WRAPPED, LocalShardStore.EMPTY),
            publisher,
            false
        );

        poller.poll();

        assertTrue(poller.status().lastFailure(), poller.status().lastFailure().contains("unreachable"));
        assertEquals(0, poller.status().assignmentsRead());
    }

    public void testTheIntendedStateIsFabricatedFromWhatWasRead() {
        // The milestone's deliverable: what would be applied, derived from the assignment and the index
        // documents, without touching the node. The rendering itself is ClusterStateReportTests' business.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(false);

        poller.poll();

        assertEquals(1, poller.status().copies());
        assertEquals(List.of(), poller.status().deferred());
    }
}
