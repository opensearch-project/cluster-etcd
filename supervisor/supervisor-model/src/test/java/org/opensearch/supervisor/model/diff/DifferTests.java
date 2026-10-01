package org.opensearch.supervisor.model.diff;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.status.Replication;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    /** A node declaring which remote repository it writes segments to. */
    private static Observation.NodeObservation nodeInRepository(NodeId id, String repository) {
        Registration registration = new Registration(
            id, "eph", id.value(), POOL, java.util.EnumSet.of(NodeRole.DATA), "10.0.0.1", 9300, "3.7.0",
            Map.of(org.opensearch.supervisor.model.allocate.RemoteStoreDecider.SEGMENT_REPOSITORY_ATTRIBUTE,
                repository),
            5000, 1L
        );
        return new Observation.NodeObservation(id, registration, null, null, Liveness.LIVE, POOL, null, 0);
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
        // SEARCH_REPLICA, because index(...) builds a segment-replication index and that is the role its
        // replicas take. The fixture said REPLICA when the count was checked in total and the role was ignored;
        // now that copies are counted per role, a REPLICA here is a surplus and a search replica is missing.
        Diff diff = Differ.diff(observe(index(1, 1), Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))),
            NODE_B, node(NODE_B, Liveness.LIVE,
                holding(NODE_B, Map.of(0, copy(ShardRole.SEARCH_REPLICA, ShardState.STARTED))))
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

        // A segment-replication index's missing replica is a missing *search* replica, which is its own kind so
        // that the planner knows which role to place and an operator can read the two counts apart.
        assertTrue(diff.of(FindingKind.UNASSIGNED_PRIMARY).isEmpty());
        assertEquals(1, diff.of(FindingKind.UNASSIGNED_SEARCH_REPLICA).size());
        assertEquals(Severity.WARNING, diff.of(FindingKind.UNASSIGNED_SEARCH_REPLICA).get(0).severity());
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

    @Test
    void anAssignmentEntryForAnIndexNobodyWantsIsAnOrphanEvenWhereTheNodeHoldsNothing() {
        // The half that looks like tidiness and is not. A node defers an entry naming an index it cannot
        // resolve, and reports a deferred assignment as failed -- so the entry alone stops this node ever
        // reporting an assignment as applied, which is what a consumer-pointer reset is completed on.
        Assignment assignment = new Assignment(1, POOL, 1, NODE_A, 1L, Map.of(
            ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                UUID, new Assignment.IndexAssignment("my-index", 3041, Map.of(0, ShardRole.PRIMARY))
            ))
        ));
        Observation.NodeObservation node = new Observation.NodeObservation(
            NODE_A, registration(NODE_A), null, null, Liveness.LIVE, POOL, assignment, 0
        );

        Diff diff = Differ.diff(observe(null, Map.of(NODE_A, node)));

        List<Finding> orphans = diff.of(FindingKind.ORPHANED_COPY);
        assertEquals(1, orphans.size());
        assertEquals(0, orphans.get(0).subject().shard());
        assertTrue(orphans.get(0).because().contains("is assigned"),
            "the reason must say which of the two records this came from: " + orphans.get(0).because());
    }

    @Test
    void aCopyBothHeldAndAssignedIsOneOrphanNotTwo() {
        // Two records of one copy. Two findings would age independently, count twice in the diff, and produce
        // two removals for one shard.
        Assignment assignment = new Assignment(1, POOL, 1, NODE_A, 1L, Map.of(
            ACME, new Assignment.TenantAssignment(Set.of(NodeRole.DATA), Map.of(
                UUID, new Assignment.IndexAssignment("my-index", 3041, Map.of(0, ShardRole.PRIMARY))
            ))
        ));
        Observation.NodeObservation node = new Observation.NodeObservation(
            NODE_A, registration(NODE_A), null,
            holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))),
            Liveness.LIVE, POOL, assignment, 0
        );

        Diff diff = Differ.diff(observe(null, Map.of(NODE_A, node)));

        List<Finding> orphans = diff.of(FindingKind.ORPHANED_COPY);
        assertEquals(1, orphans.size());
        assertTrue(orphans.get(0).because().contains("node holds"),
            "held is the stronger statement and must win: " + orphans.get(0).because());
    }

    // ---------------------------------------------------------------- a node putting its registration back

    private static Observation.NodeObservation nodeWithRewrites(int count, long lastAt) {
        Heartbeat heartbeat = new Heartbeat(NODE_A, "eph", 1L, Heartbeat.Applied.NONE, null, 0,
            new Heartbeat.Capacity(1000, 500, 10, 10, 0.1),
            count == 0 ? null : new Heartbeat.RegistrationRewrites(count, lastAt));
        return new Observation.NodeObservation(
            NODE_A, registration(NODE_A), heartbeat, null, Liveness.LIVE, POOL, null, 0
        );
    }

    /** An observation whose {@code observedAt} is a known distance after the epoch, so a window is testable. */
    private static Observation observeAt(long observedAtMillis, Observation.NodeObservation node) {
        Map<TenantId, Observation.DesiredState> tenants = new LinkedHashMap<>();
        tenants.put(ACME, new Observation.DesiredState(Map.of()));
        return new Observation(1, Instant.ofEpochMilli(observedAtMillis), POOL, tenants,
            Map.of(NODE_A, node), List.of());
    }

    @Test
    void aNodeThatRecentlyPutItsRegistrationBackIsReported() {
        // The supervisor reporting on itself. A node rewrites its registration when it finds the document
        // missing -- which is what makes reaping safe -- so a rewrite is the node saying this service deleted
        // something that was alive, and the rule that makes reaping safe is what would otherwise hide it.
        long now = 1_000_000;
        Diff diff = Differ.diff(observeAt(now, nodeWithRewrites(2, now - 60_000)));

        List<Finding> found = diff.of(FindingKind.NODE_REGISTRATION_REWRITTEN);
        assertEquals(1, found.size());
        assertEquals(NODE_A, found.get(0).subject().node());
        assertTrue(found.get(0).because().contains("reaping a node that was alive"),
            "the reason has to name the likely culprit: " + found.get(0).because());
    }

    @Test
    void anOldRewriteIsHistoryRatherThanAFinding() {
        // Keyed on the age of the last rewrite, never on the count. The count never returns to zero within a
        // process's lifetime, so a finding on "has ever happened" would stand for ever -- and a finding that
        // cannot clear is one an operator learns to scroll past.
        long now = 10_000_000;
        Diff diff = Differ.diff(observeAt(now, nodeWithRewrites(2, now - Duration.ofHours(3).toMillis())));

        assertEquals(List.of(), diff.of(FindingKind.NODE_REGISTRATION_REWRITTEN));
    }

    @Test
    void aNodeThatHasNeverRewrittenItsRegistrationSaysNothing() {
        // Absent rather than zero, which is the contract's convention and the reason the field is nullable.
        Diff diff = Differ.diff(observeAt(1_000_000, nodeWithRewrites(0, 0)));

        assertEquals(List.of(), diff.of(FindingKind.NODE_REGISTRATION_REWRITTEN));
    }

    @Test
    void aRewriteReportedFromAClockAheadOfOursStillCounts() {
        // A node whose clock runs fast reports a rewrite in the supervisor's future. Filtering that out would
        // drop the finding for exactly the deployments most likely to have clock skew.
        long now = 1_000_000;
        Diff diff = Differ.diff(observeAt(now, nodeWithRewrites(1, now + 30_000)));

        assertEquals(1, diff.of(FindingKind.NODE_REGISTRATION_REWRITTEN).size());
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
            NODE_B, node(NODE_B, Liveness.LIVE,
                holding(NODE_B, Map.of(0, copy(ShardRole.SEARCH_REPLICA, ShardState.INITIALIZING))))
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

    // ---------------------------------------------------------- search replicas (FINDINGS_FROM_THE_DATA_PLANE 1)

    @Test
    void aRequestedSearchReplicaIsWantedRatherThanIgnored() {
        // The defect: index.number_of_search_replicas was read by nothing, so a tenant who set it got no copy, no
        // complaint, and a status saying everything asked for had been delivered.
        IndexConfig withSearchReplica = new IndexConfig("my-index", UUID, true, 1, 0, 1, Replication.SEGMENT,
            false, org.opensearch.supervisor.model.settings.AllocationEnable.UNKNOWN, 3041, Map.of());

        Diff diff = Differ.diff(observe(withSearchReplica, Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))))
        )));

        assertEquals(1, diff.of(FindingKind.UNASSIGNED_SEARCH_REPLICA).size(), diff.findings().toString());
        assertTrue(diff.of(FindingKind.UNASSIGNED_SEARCH_REPLICA).get(0).because().contains("0 of 1 search replicas"),
            diff.of(FindingKind.UNASSIGNED_SEARCH_REPLICA).get(0).because());
        assertTrue(diff.of(FindingKind.UNASSIGNED_REPLICA).isEmpty(), "no ordinary replica was asked for");
    }

    @Test
    void theTwoCountsAddUpOnASegmentReplicationIndex() {
        // Both settings ask for search replicas here, which is the mapping IndexConfig.desiredCopiesOf documents
        // and which is a contract with the data plane rather than a detail of this service.
        IndexConfig both = new IndexConfig("my-index", UUID, true, 1, 1, 1, Replication.SEGMENT, false,
            org.opensearch.supervisor.model.settings.AllocationEnable.UNKNOWN, 3041, Map.of());

        assertEquals(3, both.copiesPerShard());
        assertEquals(2, both.desiredCopiesOf(ShardRole.SEARCH_REPLICA));
        assertEquals(0, both.desiredCopiesOf(ShardRole.REPLICA));

        Diff oneShort = Differ.diff(observe(both, Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))),
            NODE_B, node(NODE_B, Liveness.LIVE,
                holding(NODE_B, Map.of(0, copy(ShardRole.SEARCH_REPLICA, ShardState.STARTED))))
        )));

        assertEquals(1, oneShort.of(FindingKind.UNASSIGNED_SEARCH_REPLICA).size(), oneShort.findings().toString());
        assertTrue(oneShort.of(FindingKind.SURPLUS_COPY).isEmpty());
    }

    @Test
    void copiesAreCountedPerRoleRatherThanInTotal() {
        // Two counts that sum correctly can still both be wrong. A document-replication index wanting one replica
        // is not satisfied by a search replica, however many copies there are.
        IndexConfig docrep = new IndexConfig("my-index", UUID, true, 1, 1, 0, Replication.DOCUMENT, false,
            org.opensearch.supervisor.model.settings.AllocationEnable.UNKNOWN, 3041, Map.of());

        Diff diff = Differ.diff(observe(docrep, Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED)))),
            NODE_B, node(NODE_B, Liveness.LIVE,
                holding(NODE_B, Map.of(0, copy(ShardRole.SEARCH_REPLICA, ShardState.STARTED))))
        )));

        assertEquals(1, diff.of(FindingKind.UNASSIGNED_REPLICA).size(), diff.findings().toString());
        assertEquals(1, diff.of(FindingKind.SURPLUS_COPY).size(), "and the search replica is one too many");
    }

    @Test
    void searchReplicasOnADocumentReplicationIndexAreReportedRatherThanPlanned() {
        // The general defect behind the specific one: a request this supervisor declines must leave a reason
        // behind. Not counted as unassigned, because unassigned means "intended and not yet placed" and this can
        // never be placed -- reporting it that way would leave the index yellow for ever with nothing to do.
        IndexConfig impossible = new IndexConfig("my-index", UUID, true, 1, 0, 2, Replication.DOCUMENT, false,
            org.opensearch.supervisor.model.settings.AllocationEnable.UNKNOWN, 3041, Map.of());

        assertEquals(0, impossible.desiredCopiesOf(ShardRole.SEARCH_REPLICA));
        assertEquals(1, impossible.copiesPerShard(), "the primary alone");

        Diff diff = Differ.diff(observe(impossible, Map.of(
            NODE_A, node(NODE_A, Liveness.LIVE, holding(NODE_A, Map.of(0, copy(ShardRole.PRIMARY, ShardState.STARTED))))
        )));

        assertEquals(1, diff.of(FindingKind.DESIRED_COPY_IGNORED).size(), diff.findings().toString());
        assertTrue(diff.of(FindingKind.DESIRED_COPY_IGNORED).get(0).because().contains("document replication"),
            diff.of(FindingKind.DESIRED_COPY_IGNORED).get(0).because());
        assertTrue(diff.of(FindingKind.UNASSIGNED_SEARCH_REPLICA).isEmpty(),
            "not pending: nothing could ever place it");
    }

    @Test
    void theSettingIsParsedTheWayOpenSearchSpellsIt() {
        IndexConfig parsed = IndexConfig.parse(IndexConfig.toDocument("my-index", UUID, true, Map.of(
            IndexConfig.SETTING_SHARDS, "1",
            IndexConfig.SETTING_REPLICAS, "0",
            IndexConfig.SETTING_SEARCH_REPLICAS, "1",
            IndexConfig.SETTING_REPLICATION, "SEGMENT"
        )), 4);

        assertEquals(1, parsed.numberOfSearchReplicas());
        assertEquals(2, parsed.copiesPerShard(), "a primary and the search replica that was asked for");
    }

    // ------------------------------------------- pool repositories (FINDINGS_FROM_THE_DATA_PLANE 7)

    @Test
    void nodesWritingToDifferentRepositoriesAreACriticalFinding() {
        // The only finding here whose failure mode is a lie rather than an absence. Copies of one shard share a
        // remote path, so a search replica on the odd node out reads an empty store and reports itself healthy.
        // No node can detect it -- each sees only its own attributes -- so nothing else will report it.
        Diff diff = Differ.diff(observe(index(1, 1), Map.of(
            NODE_A, nodeInRepository(NODE_A, "segments-east"),
            NODE_B, nodeInRepository(NODE_B, "segments-west")
        )));

        assertEquals(1, diff.of(FindingKind.POOL_REPOSITORY_MISMATCH).size(), diff.findings().toString());
        Finding finding = diff.of(FindingKind.POOL_REPOSITORY_MISMATCH).get(0);
        assertEquals(Severity.CRITICAL, finding.severity());
        assertTrue(finding.because().contains("segments-east"), finding.because());
        assertTrue(finding.because().contains("segments-west"), finding.because());
        assertTrue(finding.because().contains("reports itself healthy"), finding.because());
    }

    @Test
    void agreeingOnOneRepositoryIsNotAFinding() {
        Diff diff = Differ.diff(observe(index(1, 1), Map.of(
            NODE_A, nodeInRepository(NODE_A, "segments"),
            NODE_B, nodeInRepository(NODE_B, "segments")
        )));

        assertTrue(diff.of(FindingKind.POOL_REPOSITORY_MISMATCH).isEmpty(), diff.findings().toString());
    }

    @Test
    void aNodeDeclaringNoRepositoryIsNotADisagreement() {
        // Absence is RemoteStoreDecider's finding, with its own reason. A coordinator has no repository, and a
        // data node without one gets refused a search-only copy rather than being called inconsistent.
        Diff diff = Differ.diff(observe(index(1, 1), Map.of(
            NODE_A, nodeInRepository(NODE_A, "segments"),
            NODE_B, node(NODE_B, Liveness.LIVE, null)
        )));

        assertTrue(diff.of(FindingKind.POOL_REPOSITORY_MISMATCH).isEmpty(), diff.findings().toString());
    }

    @Test
    void theMismatchNamesOneExampleNodePerRepositoryRatherThanAllOfThem() {
        // Bounded, for the same reason the allocator groups refusals: this is projected into the store and a large
        // pool must not produce a large document.
        Map<NodeId, Observation.NodeObservation> nodes = new LinkedHashMap<>();
        nodes.put(NODE_A, nodeInRepository(NODE_A, "segments-east"));
        nodes.put(NODE_B, nodeInRepository(NODE_B, "segments-west"));
        NodeId nodeC = NodeId.of("nodeCCCCCCCCCCCCCCCCCC");
        nodes.put(nodeC, nodeInRepository(nodeC, "segments-west"));

        Finding finding = Differ.diff(observe(index(1, 2), nodes))
            .of(FindingKind.POOL_REPOSITORY_MISMATCH).get(0);

        assertTrue(finding.because().contains("2 different remote repositories"), finding.because());
        assertTrue(finding.because().contains("2 nodes, e.g."), finding.because());
        assertTrue(finding.because().contains("1 node, e.g."), finding.because());
    }
}
