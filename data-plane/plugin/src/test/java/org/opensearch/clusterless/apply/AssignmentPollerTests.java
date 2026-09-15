/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterName;
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

    private static final ClusterName CLUSTER = new ClusterName("clusterless");
    private static final NodeId NODE = NodeId.of("pv1sDMfZRxSTMiW5VrbN9w");
    private static final String EPHEMERAL = "OYidGJ1ITdC2mMrZEW0BNQ";
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private InMemoryKeyValueStore store;
    private NodePublisher publisher;
    private AtomicLong now;
    private RecordingApplier recordingApplier;

    /**
     * The server's half of applying, reduced to what the poller can observe.
     *
     * <p>It really runs the update function against the state it holds, because the poller's acknowledgement is
     * computed from the live state rather than from the fact that an update was submitted — so an applier that
     * only counted calls would let a wrong ack pass. {@link #finishRecovery} then does what the server does once
     * a shard has recovered, which is the transition the ack is really about.
     */
    private static final class RecordingApplier implements ClusterStateApplier {

        private ClusterState state = ClusterState.EMPTY_STATE;
        private int applications;
        private Exception failWith;

        @Override
        public void apply(String source, java.util.function.Function<ClusterState, ClusterState> update, Outcome onOutcome) {
            applications++;
            if (failWith != null) {
                onOutcome.applied(source, failWith);
                return;
            }
            state = update.apply(state);
            onOutcome.applied(source, null);
        }

        void finishRecovery() {
            org.opensearch.cluster.routing.RoutingTable.Builder routing = org.opensearch.cluster.routing.RoutingTable.builder();
            for (org.opensearch.cluster.metadata.IndexMetadata index : state.metadata()) {
                org.opensearch.cluster.routing.IndexRoutingTable.Builder indexRouting =
                    org.opensearch.cluster.routing.IndexRoutingTable.builder(index.getIndex());
                for (org.opensearch.cluster.routing.IndexShardRoutingTable shard : state.routingTable().index(index.getIndex())) {
                    for (org.opensearch.cluster.routing.ShardRouting copy : shard) {
                        indexRouting.addShard(copy.initializing() ? copy.moveToStarted() : copy);
                    }
                }
                routing.add(indexRouting);
            }
            state = ClusterState.builder(state).routingTable(routing.build()).build();
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        store = new InMemoryKeyValueStore();
        now = new AtomicLong(1788969977000L);
        recordingApplier = new RecordingApplier();
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

    /** The index document both the resolver and the reaper read. Removed to simulate a delete. */
    private final java.util.concurrent.atomic.AtomicBoolean indexExists = new java.util.concurrent.atomic.AtomicBoolean(true);

    private final IndexConfigSource indexConfigs = (tenant, name) -> indexExists.get()
        ? Optional.of(TestFixtures.indexConfig(UUID))
        : Optional.empty();

    private AssignmentPoller poller(boolean apply) {
        return new AssignmentPoller(
            store,
            NODE,
            new DesiredStateResolver(indexConfigs),
            () -> recordingApplier.state,
            (desired, stopRetaining, recoverySources, coordinated) -> new DataNodeState(
                CLUSTER,
                TestFixtures.localNode(NODE.value()),
                desired,
                MappingSource.WRAPPED,
                LocalShardStore.EMPTY,
                stopRetaining
            ),
            publisher,
            recordingApplier,
            new RetainedIndexReaper(indexConfigs, 12),
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
        // Two primaries rather than a primary and a search replica: this poller's node has no remote store
        // configured, so a search replica would be declined for want of somewhere to read its segments, and this
        // test is about reading and acking an assignment rather than about that rule.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\", \"1\": \"PRIMARY\"}"));
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

    // ---------------------------------------------------------------- letting go of a deleted index

    public void testADeletedIndexIsReleasedOnlyAfterItStaysGone() {
        // Deleting the index document is not enough on its own: an index that has not been observed and one that
        // has been deleted look identical, and only one of those readings is recoverable. So the node keeps the
        // metadata -- and therefore the files -- until it has watched the document stay absent.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(true);
        poller.poll();
        recordingApplier.finishRecovery();
        assertEquals(1, recordingApplier.state.metadata().indices().size());

        // The control plane deletes the index, so the supervisor stops assigning the copy.
        indexExists.set(false);
        store.put(Keys.nodeAssignment(NODE), "{\"version\": 43, \"tenants\": {}}".getBytes(StandardCharsets.UTF_8));

        poller.poll();
        assertEquals("the metadata must survive the first look", 1, recordingApplier.state.metadata().indices().size());
        for (int pass = 0; pass < 10; pass++) {
            poller.poll();
        }
        assertEquals("and every look until the threshold", 1, recordingApplier.state.metadata().indices().size());

        poller.poll();
        assertTrue(
            "at which point the metadata goes, which is what lets the server delete the data",
            recordingApplier.state.metadata().indices().isEmpty()
        );
    }

    public void testAnIndexThatIsStillInTheStoreIsKeptForEver() {
        // The other reading of the same signal: the copy moved elsewhere, or the index was closed. The index
        // document is still there, so the data stays -- for as many passes as it takes.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(true);
        poller.poll();
        recordingApplier.finishRecovery();

        store.put(Keys.nodeAssignment(NODE), "{\"version\": 43, \"tenants\": {}}".getBytes(StandardCharsets.UTF_8));
        for (int pass = 0; pass < 30; pass++) {
            poller.poll();
        }

        assertEquals(1, recordingApplier.state.metadata().indices().size());
    }

    public void testAnIndexThatComesBackForgetsItsStrikes() {
        // A copy that comes and goes must not accumulate strikes across the gaps, or a long-lived node would
        // eventually delete an index that had merely been absent a few times.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(true);
        poller.poll();
        recordingApplier.finishRecovery();

        for (int cycle = 0; cycle < 5; cycle++) {
            indexExists.set(false);
            store.put(Keys.nodeAssignment(NODE), "{\"version\": 43, \"tenants\": {}}".getBytes(StandardCharsets.UTF_8));
            poller.poll();
            poller.poll();

            indexExists.set(true);
            writeAssignment(assignmentJson(44 + cycle, "{\"0\": \"PRIMARY\"}"));
            poller.poll();
            recordingApplier.finishRecovery();
        }

        assertEquals("ten absent passes in total, and it is still here", 1, recordingApplier.state.metadata().indices().size());
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
            (desired, stopRetaining, recoverySources, coordinated) -> new DataNodeState(
                CLUSTER,
                TestFixtures.localNode(NODE.value()),
                desired,
                MappingSource.WRAPPED,
                LocalShardStore.EMPTY,
                stopRetaining
            ),
            publisher,
            recordingApplier,
            new RetainedIndexReaper(indexConfigs, 12),
            false
        );

        poller.poll();

        assertTrue(poller.status().lastFailure(), poller.status().lastFailure().contains("unreachable"));
        assertEquals(0, poller.status().assignmentsRead());
    }

    // ---------------------------------------------------------------- applying

    public void testApplyingHandsTheFabricatedStateToTheServer() {
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(true);

        poller.poll();

        assertEquals(1, recordingApplier.applications);
        assertEquals(1, poller.status().statesApplied());
        assertEquals(1, recordingApplier.state.metadata().indices().size());
        assertEquals(1, poller.status().copies());
    }

    public void testAppliedIsNotClaimedUntilTheCopyIsActuallyServing() {
        // The easiest lie available: the state went in without error, so it is tempting to say "applied". Every
        // new copy is INITIALIZING at that moment, and whether it starts depends on a recovery that happens
        // afterwards and may not succeed.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(true);

        poller.poll();
        publisher.publish();
        assertEquals("applying", publisher.status().appliedOutcome().wireName());

        recordingApplier.finishRecovery();
        poller.poll();
        publisher.publish();

        assertEquals("applied", publisher.status().appliedOutcome().wireName());
        assertEquals(42, publisher.status().appliedAssignmentVersion());
    }

    public void testTheAcknowledgementSaysWhatItIsWaitingFor() {
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\", \"1\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(true);

        poller.poll();

        // Read from the live state each pass rather than remembered, so this is derived rather than tracked.
        Convergence convergence = Convergence.assess(
            new DesiredStateResolver((tenant, name) -> Optional.of(TestFixtures.indexConfig(UUID))).resolve(
                org.opensearch.clusterless.contract.Assignment.read(
                    assignmentJson(42, "{\"0\": \"PRIMARY\", \"1\": \"PRIMARY\"}").getBytes(StandardCharsets.UTF_8)
                )
            ),
            recordingApplier.state,
            NODE.value()
        );
        assertEquals(2, convergence.desired());
        assertEquals(0, convergence.serving());
        assertTrue(convergence.detail(), convergence.detail().contains("0 of 2 copies serving"));
        assertTrue(convergence.detail(), convergence.detail().contains("INITIALIZING"));
    }

    public void testAnApplyFailureIsReportedRatherThanRetriedSilently() {
        // A state the server refuses is not something the next pass fixes by itself, so it has to be visible.
        // The pass still runs -- level-triggered means the next one tries again from what it reads.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        recordingApplier.failWith = new IllegalStateException("the applier said no");
        AssignmentPoller poller = poller(true);

        poller.poll();
        publisher.publish();

        assertEquals("failed", publisher.status().appliedOutcome().wireName());
        assertTrue(poller.status().lastApplyFailure(), poller.status().lastApplyFailure().contains("the applier said no"));
        assertEquals(0, poller.status().statesApplied());
    }

    public void testARecoveredApplyStopsBeingReportedAsFailed() {
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        recordingApplier.failWith = new IllegalStateException("transient");
        AssignmentPoller poller = poller(true);
        poller.poll();

        recordingApplier.failWith = null;
        poller.poll();
        recordingApplier.finishRecovery();
        poller.poll();
        publisher.publish();

        assertNull(poller.status().lastApplyFailure());
        assertEquals("applied", publisher.status().appliedOutcome().wireName());
    }

    public void testACopyThatWasNotBuiltKeepsTheAckHonestEvenWhenTheRestConverges() {
        // Deferred copies are not something more time fixes, so the supervisor should see that part as stuck
        // rather than reading "applying" for ever and waiting.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\", \"1\": \"REPLICA\"}"));
        AssignmentPoller poller = poller(true);

        poller.poll();
        recordingApplier.finishRecovery();
        poller.poll();
        publisher.publish();

        assertEquals("failed", publisher.status().appliedOutcome().wireName());
        assertEquals(1, poller.status().copies());
        assertEquals(1, poller.status().deferred().size());
    }

    public void testApplyingWithoutAnApplierIsRefusedAtConstruction() {
        // A configuration mistake that would otherwise surface as a NullPointerException on the first pass, from
        // a scheduled task, with nothing pointing at the setting that caused it.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new AssignmentPoller(
                store,
                NODE,
                new DesiredStateResolver((tenant, name) -> Optional.empty()),
                () -> ClusterState.EMPTY_STATE,
                (desired, stopRetaining, recoverySources, coordinated) -> new DataNodeState(
                    CLUSTER,
                    TestFixtures.localNode(NODE.value()),
                    desired,
                    MappingSource.WRAPPED,
                    LocalShardStore.EMPTY,
                    stopRetaining
                ),
                publisher,
                null,
                new RetainedIndexReaper(indexConfigs, 12),
                true
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("no applier"));
    }

    public void testAnUnchangedAssignmentIsAppliedOnceAndNotAgain() {
        // The bug this replaced was found by running a node: the fabricator bumps the version by construction,
        // so applying unconditionally published a fresh cluster state every poll -- version 1917 and climbing --
        // and ran every applier and listener in the server each time. Level-triggered convergence needs a fixed
        // point, and this is the assertion that there is one.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(true);

        poller.poll();
        assertEquals(1, recordingApplier.applications);
        long versionAfterFirst = recordingApplier.state.version();

        for (int pass = 0; pass < 10; pass++) {
            poller.poll();
        }

        assertEquals("the same desired state must be applied once", 1, recordingApplier.applications);
        assertEquals("and the cluster state version must stop climbing", versionAfterFirst, recordingApplier.state.version());
    }

    public void testAShardFinishingRecoveryDoesNotProvokeAnotherApplication() {
        // The state the server hands back after recovery differs from the one that was applied, and the
        // fabricator carries that difference forward rather than undoing it -- so this must not look like a
        // change that needs applying, or the node would republish once per recovery for ever.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(true);
        poller.poll();
        recordingApplier.finishRecovery();

        poller.poll();
        poller.poll();

        assertEquals(1, recordingApplier.applications);
    }

    public void testARealChangeIsStillApplied() {
        // The other half: the fixed point must not be a dead end.
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(true);
        poller.poll();
        poller.poll();
        assertEquals(1, recordingApplier.applications);

        writeAssignment(assignmentJson(43, "{\"0\": \"PRIMARY\", \"1\": \"PRIMARY\"}"));
        poller.poll();

        assertEquals(2, recordingApplier.applications);
        assertEquals(2, poller.status().copies());
    }

    public void testReadOnlyModeAppliesNothingAtAll() {
        writeAssignment(assignmentJson(42, "{\"0\": \"PRIMARY\"}"));
        AssignmentPoller poller = poller(false);

        poller.poll();

        assertEquals("nothing may reach the applier in read-only mode", 0, recordingApplier.applications);
        assertEquals(0, poller.status().statesApplied());
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
