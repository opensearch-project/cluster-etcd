package org.opensearch.supervisor.model.diff;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.Replication;

import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The differ, exercised with hand-built observations and no store at all.
 *
 * <p>This is the payoff for keeping the core pure, and the reason {@code DESIGN.md} §1 insists on it: the
 * riskiest code in the service is testable exhaustively in a sandbox with no sockets, no containers and no
 * Gradle. A test here is two literals and an assertion.
 */
class DifferTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final NodeId NODE_A = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId NODE_B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");

    // ---------------------------------------------------------------- builders

    private static IndexConfig index(int shards, int replicas) {
        return new IndexConfig("my-index", UUID, true, shards, replicas, Replication.SEGMENT, false, org.opensearch.supervisor.model.settings.AllocationEnable.UNKNOWN, 3041);
    }

    private static Registration registration(NodeId node) {
        return new Registration(
            node, "eph", node.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of(), 5000, 1L
        );
    }

    private static Observation.NodeObservation node(NodeId id, Liveness liveness, ShardInventory inventory) {
        return new Observation.NodeObservation(id, registration(id), null, inventory, liveness, POOL, null, 0);
    }

    private static ShardInventory holding(NodeId node, Map<Integer, ShardInventory.ShardCopy> shards) {
        return new ShardInventory(node, "eph", 1L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                UUID, new ShardInventory.IndexShards("my-index", 3041, shards)
            ))
        ));
    }

    private static Observation observe(IndexConfig index, Map<NodeId, Observation.NodeObservation> nodes) {
        return observe(1, index, nodes);
    }

    private static Observation observe(long pass, IndexConfig index, Map<NodeId, Observation.NodeObservation> nodes) {
        Map<TenantId, Observation.DesiredState> tenants = new LinkedHashMap<>();
        tenants.put(ACME, new Observation.DesiredState(index == null ? Map.of() : Map.of(UUID, index)));
        return new Observation(pass, Instant.EPOCH, POOL, tenants, nodes, List.of());
    }

    private static ShardInventory.ShardCopy copy(ShardRole role, ShardState state) {
        return ShardInventory.ShardCopy.of(role, state);
    }

    // ---------------------------------------------------------------- the empty diff

    @Test
    void aFullyPlacedIndexProducesNothing() {
        // R2.2 in spirit: the interesting property of a reconciler is that it says nothing when there is
        // nothing to say. A differ that always finds something is a differ nobody will read.
        Diff diff = Differ.diff(observe(index(1, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))),
            NODE_B, node(NODE_B, Liveness.LIVE, holding(NODE_B, Map.of(0, copy(ShardRole.REPLICA, ShardState.STARTED))))
        )));

        assertTrue(diff.isEmpty(), () -> "expected nothing, got " + diff.findings());
    }

    @Test
    void aClosedIndexWantsNothingAndIsNotAFinding() {
        // A closed index still has settings and mappings and still exists; it simply should not be serving,
        // and OpenSearch reports it green with no shards.
        IndexConfig closed = new IndexConfig("my-index", UUID, false, 3, 1, Replication.SEGMENT, false, org.opensearch.supervisor.model.settings.AllocationEnable.UNKNOWN, 3041);

        assertTrue(Differ.diff(observe(closed, Map.of())).isEmpty());
        assertEquals(0, closed.desiredCopies());
    }

    // ---------------------------------------------------------------- missing copies

    @Test
    void anIndexWithNoCopiesAnywhereIsACriticalUnassignedPrimary() {
        Diff diff = Differ.diff(observe(index(1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, null)
        )));

        List<Finding> primaries = diff.of(FindingKind.UNASSIGNED_PRIMARY);
        assertEquals(1, primaries.size());
        assertEquals(Severity.CRITICAL, primaries.get(0).severity());
        assertEquals(Subject.ofShard(ACME, UUID, 0), primaries.get(0).subject());
    }

    @Test
    void everyShardOfAMultiShardIndexIsItsOwnFinding() {
        // Per shard, not per index. An index of three shards missing all three primaries is three things
        // to fix, and collapsing them would hide two of them behind the first.
        Diff diff = Differ.diff(observe(index(3, 0), Map.of()));

        assertEquals(3, diff.of(FindingKind.UNASSIGNED_PRIMARY).size());
        assertEquals(
            List.of(0, 1, 2),
            diff.of(FindingKind.UNASSIGNED_PRIMARY).stream().map(finding -> finding.subject().shard()).sorted().toList()
        );
    }

    @Test
    void aMissingReplicaIsAWarningNotACritical() {
        Diff diff = Differ.diff(observe(index(1, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))))
        )));

        assertTrue(diff.of(FindingKind.UNASSIGNED_PRIMARY).isEmpty());
        assertEquals(1, diff.of(FindingKind.UNASSIGNED_REPLICA).size());
        assertEquals(Severity.WARNING, diff.of(FindingKind.UNASSIGNED_REPLICA).get(0).severity());
    }

    @Test
    void anInitializingPrimaryIsPresentAndSoIsNotUnassigned() {
        // The case the role/state split exists for. A primary that is recovering is not missing -- placing
        // a second one would be actively wrong -- but it is also not serving.
        Diff diff = Differ.diff(observe(index(1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.INITIALIZING))))
        )));

        assertTrue(diff.of(FindingKind.UNASSIGNED_PRIMARY).isEmpty(), "an initializing primary is still a primary");
    }

    // ---------------------------------------------------------------- surplus and orphans

    @Test
    void anExtraReplicaIsASurplusCopy() {
        Diff diff = Differ.diff(observe(index(1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))),
            NODE_B, node(NODE_B, Liveness.LIVE, holding(NODE_B, Map.of(0, copy(ShardRole.REPLICA, ShardState.STARTED))))
        )));

        assertEquals(1, diff.of(FindingKind.SURPLUS_COPY).size());
    }

    @Test
    void aCopyOfAnIndexNobodyWantsIsAnOrphanRatherThanSomethingToDelete() {
        // The finding that costs data if it is acted on too early. A deleted index and an index document
        // that has not landed yet are indistinguishable from here, so this reports and never decides.
        Diff diff = Differ.diff(observe(null, Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))))
        )));

        List<Finding> orphans = diff.of(FindingKind.ORPHANED_COPY);
        assertEquals(1, orphans.size());
        assertEquals(NODE_A, orphans.get(0).subject().node());
        assertTrue(orphans.get(0).because().contains("has not landed yet"),
            "the reason must name the ambiguity, or someone will act on it");
    }

    // ---------------------------------------------------------------- liveness

    @Test
    void aStaleNodesCopiesStillCount() {
        // The gap between stale and lost is the tolerance for a GC pause. Treating a paused node's shards
        // as gone is how a reconciler turns a hiccup into a cluster-wide rebuild.
        Diff diff = Differ.diff(observe(index(1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.STALE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))))
        )));

        assertEquals(1, diff.of(FindingKind.NODE_STALE).size());
        assertTrue(diff.of(FindingKind.UNASSIGNED_PRIMARY).isEmpty(), "a stale node's copies must still count");
    }

    @Test
    void aLostNodesCopiesStopCounting() {
        Diff diff = Differ.diff(observe(index(1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.LOST, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))))
        )));

        assertEquals(1, diff.of(FindingKind.NODE_LOST).size());
        assertEquals(1, diff.of(FindingKind.UNASSIGNED_PRIMARY).size());
    }

    @Test
    void aHeartbeatWithoutRegistrationIsReportedButNotPlacedOn() {
        Observation.NodeObservation unregistered = new Observation.NodeObservation(
            NODE_A, null,
            new org.opensearch.supervisor.model.node.Heartbeat(
                NODE_A, "eph", 1L, null, null, 0,
                org.opensearch.supervisor.model.node.Heartbeat.Capacity.UNKNOWN
            ),
            null, Liveness.LIVE, POOL, null, 0
        );
        Observation observation = observe(index(1, 0), Map.of(NODE_A, unregistered));

        assertEquals(1, Differ.diff(observation).of(FindingKind.NODE_UNREGISTERED).size());
        assertTrue(observation.placeableNodes().isEmpty());
        assertTrue(Differ.diff(observation).of(FindingKind.UNASSIGNED_PRIMARY).get(0).because()
            .contains("no node in this pool is currently placeable"));
    }

    // ---------------------------------------------------------------- docrep ordering

    @Test
    void replicasWithNoServingPrimaryUnderDocrepAreAPlanningMistake() {
        IndexConfig docrep = new IndexConfig("my-index", UUID, true, 1, 1, Replication.DOCUMENT, false, org.opensearch.supervisor.model.settings.AllocationEnable.UNKNOWN, 3041);
        Diff diff = Differ.diff(observe(docrep, Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.INITIALIZING)))),
            NODE_B, node(NODE_B, Liveness.LIVE, holding(NODE_B, Map.of(0, copy(ShardRole.REPLICA, ShardState.INITIALIZING))))
        )));

        assertEquals(1, diff.of(FindingKind.PRIMARY_BEFORE_REPLICA).size());
    }

    @Test
    void segmentReplicationHasNoSuchOrdering() {
        // A segrep replica syncs from the remote store, so it does not wait on a primary and there is
        // nothing wrong with it recovering first.
        Diff diff = Differ.diff(observe(index(1, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.INITIALIZING)))),
            NODE_B, node(NODE_B, Liveness.LIVE, holding(NODE_B, Map.of(0, copy(ShardRole.REPLICA, ShardState.INITIALIZING))))
        )));

        assertTrue(diff.of(FindingKind.PRIMARY_BEFORE_REPLICA).isEmpty());
    }

    // ---------------------------------------------------------------- forward compatibility

    @Test
    void anUnrecognisedStateIsReportedAndCountedAsPresent() {
        Diff diff = Differ.diff(observe(index(1, 0), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.UNKNOWN, ShardState.UNKNOWN))))
        )));

        assertEquals(1, diff.of(FindingKind.UNRECOGNISED_STATE).size());
        // Present, so not an orphan; but not a primary either, so the primary is still missing. Both are
        // the conservative reading and both are deliberate.
        assertEquals(1, diff.of(FindingKind.UNASSIGNED_PRIMARY).size());
    }

    @Test
    void anUnreadableDocumentIsAFindingRatherThanAFailedPass() {
        Observation observation = new Observation(
            1, Instant.EPOCH, POOL, Map.of(), Map.of(), List.of("nodes/shards/broken")
        );

        assertEquals(1, Differ.diff(observation).of(FindingKind.UNREADABLE_DOCUMENT).size());
    }

    // ---------------------------------------------------------------- ordering and reasons

    @Test
    void everyFindingCarriesAReason() {
        Diff diff = Differ.diff(observe(index(2, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LOST, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))))
        )));

        assertFalse(diff.isEmpty());
        diff.findings().forEach(finding -> assertFalse(
            finding.because().isBlank(), finding.kind() + " on " + finding.subject() + " has no reason"
        ));
    }

    @Test
    void findingsAreOrderedSoTruncationDropsTheLeastInteresting() {
        Diff diff = Differ.diff(observe(index(1, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LOST, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))))
        )));

        List<Severity> severities = diff.findings().stream().map(Finding::severity).toList();
        List<Severity> sorted = severities.stream()
            .sorted((a, b) -> Integer.compare(b.ordinal(), a.ordinal()))
            .toList();
        assertEquals(sorted, severities, "most severe must come first");
    }

    @Test
    void countsByKindMatchTheFindings() {
        Diff diff = Differ.diff(observe(index(3, 0), Map.of()));

        assertEquals(3, diff.countsByKind().get(FindingKind.UNASSIGNED_PRIMARY));
        assertEquals(diff.findings().size(), diff.countsByKind().values().stream().mapToInt(Integer::intValue).sum());
    }
}
