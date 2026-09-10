package org.opensearch.supervisor.model.plan;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Differ;
import org.opensearch.supervisor.model.diff.FindingAges;
import org.opensearch.supervisor.model.diff.FindingKind;
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

import java.time.Instant;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The planner, with no store anywhere near it.
 *
 * <p>This is the first code in the service capable of changing a cluster, so it is also the code most worth
 * being able to test by writing down a situation and reading off the answer.
 */
class PlannerTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final TenantId GLOBEX = TenantId.of("globex");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";
    private static final NodeId EMPTY_NODE = NodeId.of("nodeEmptyAAAAAAAAAAAA");
    private static final NodeId FULL_NODE = NodeId.of("nodeFullBBBBBBBBBBBBB");
    private static final Planner.Policy POLICY = Planner.Policy.defaults();

    private static Registration registration(NodeId node) {
        return new Registration(
            node, "eph", node.value(), POOL, EnumSet.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of(), 5000, 1L
        );
    }

    private static Heartbeat heartbeat(NodeId node, long freeBytes) {
        return new Heartbeat(node, "eph", 1L, Heartbeat.Applied.NONE, null, 0,
            new Heartbeat.Capacity(1_000_000_000L, freeBytes, 10, 10, 0.1));
    }

    private static Observation.NodeObservation node(NodeId id, long freeBytes) {
        return new Observation.NodeObservation(
            id, registration(id), heartbeat(id, freeBytes), null, Liveness.LIVE, POOL, null, 0
        );
    }

    private static Observation.NodeObservation nodeHolding(NodeId id, ShardRole role, ShardState state) {
        ShardInventory inventory = new ShardInventory(id, "eph", 1L, Map.of(
            ACME, new ShardInventory.TenantShards(Map.of(
                UUID, new ShardInventory.IndexShards("my-index", 3041, Map.of(
                    0, ShardInventory.ShardCopy.of(role, state)
                ))
            ))
        ));
        return new Observation.NodeObservation(
            id, registration(id), heartbeat(id, 500), inventory, Liveness.LIVE, POOL, null, 0
        );
    }

    private static Observation.NodeObservation nodeAssigned(NodeId id) {
        Assignment assignment = new Assignment(1, POOL, 1, id, 1L, Map.of(
            ACME, new Assignment.TenantAssignment(NodeRole.DATA, Map.of(
                UUID, new Assignment.IndexAssignment("my-index", 3041, Map.of(0, ShardRole.PRIMARY))
            ))
        ));
        return new Observation.NodeObservation(
            id, registration(id), heartbeat(id, 500), null, Liveness.LIVE, POOL, assignment, 7
        );
    }

    private static IndexConfig index(String uuid, String name, int shards) {
        return new IndexConfig(name, uuid, true, shards, 0, Replication.SEGMENT, false, 3041);
    }

    private static Observation observe(
        long pass,
        Map<TenantId, Map<String, IndexConfig>> indices,
        Map<NodeId, Observation.NodeObservation> nodes
    ) {
        Map<TenantId, Observation.DesiredState> tenants = new LinkedHashMap<>();
        indices.forEach((tenant, byUuid) -> tenants.put(tenant, new Observation.DesiredState(byUuid)));
        return new Observation(pass, Instant.EPOCH, POOL, tenants, nodes, List.of());
    }

    /** Runs the differ so the findings carry real ages, then plans. */
    private static Plan planAfter(int passes, Observation observation) {
        FindingAges ages = new FindingAges();
        Diff diff = null;
        for (int pass = 1; pass <= passes; pass++) {
            Observation aged = new Observation(
                pass, observation.observedAt(), observation.pool(),
                observation.tenants(), observation.nodes(), observation.unreadable()
            );
            diff = Differ.diff(aged, ages);
        }
        return Planner.plan(observation, diff, 7, POLICY);
    }

    // ---------------------------------------------------------------- the narrow case

    @Test
    void placesAPrimaryForAShardWithNoCopiesAnywhere() {
        Plan plan = planAfter(3, observe(3,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 1))),
            Map.of(EMPTY_NODE, node(EMPTY_NODE, 900))
        ));

        assertEquals(1, plan.instructions().size());
        Instruction.PlaceCopy place = (Instruction.PlaceCopy) plan.instructions().get(0);
        assertEquals(EMPTY_NODE, place.node());
        assertEquals(ShardRole.PRIMARY, place.role());
        assertEquals(0, place.shard());
        assertEquals(3041, place.configRevision(), "the node must be told which config to build against");
        assertFalse(place.because().isBlank());
    }

    @Test
    void waitsForAFindingToHoldBeforeActingOnIt() {
        // Not patience for its own sake: one pass of delay is what stops the supervisor reacting to a torn
        // read, and a couple of seconds is cheap next to a wrong placement.
        Observation observation = observe(1,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 1))),
            Map.of(EMPTY_NODE, node(EMPTY_NODE, 900))
        );

        assertTrue(planAfter(1, observation).isEmpty(), "a finding one pass old is not yet actionable");
        assertFalse(planAfter((int) POLICY.minimumPasses(), observation).isEmpty());
    }

    @Test
    void doesNothingWhenTheShardIsAlreadyHeld() {
        Plan plan = planAfter(5, observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 1))),
            Map.of(EMPTY_NODE, nodeHolding(EMPTY_NODE, ShardRole.PRIMARY, ShardState.STARTED))
        ));

        assertTrue(plan.isEmpty());
    }

    @Test
    void doesNothingWhileANodeIsStillInitializingTheShard() {
        Plan plan = planAfter(5, observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 1))),
            Map.of(EMPTY_NODE, nodeHolding(EMPTY_NODE, ShardRole.PRIMARY, ShardState.INITIALIZING))
        ));

        assertTrue(plan.isEmpty(), "an initializing primary is already there");
    }

    @Test
    void doesNotPlaceAShardThatIsAlreadyAssignedButNotYetReported() {
        // The check that makes re-placement impossible without remembering anything. A node given an
        // assignment reports nothing for a pass or two, so a planner looking only at inventories would place
        // the same shard again -- on a different node each time, producing one surplus copy per pass.
        Plan plan = planAfter(5, observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 1))),
            Map.of(
                EMPTY_NODE, nodeAssigned(EMPTY_NODE),
                FULL_NODE, node(FULL_NODE, 900)
            )
        ));

        assertTrue(plan.isEmpty(), "already assigned, so nothing to do");
    }

    @Test
    void doesNothingForAClosedIndex() {
        IndexConfig closed = new IndexConfig("my-index", UUID, false, 1, 0, Replication.SEGMENT, false, 3041);
        Plan plan = planAfter(5, observe(5,
            Map.of(ACME, Map.of(UUID, closed)),
            Map.of(EMPTY_NODE, node(EMPTY_NODE, 900))
        ));

        assertTrue(plan.isEmpty());
    }

    @Test
    void doesNothingWhenNoNodeIsPlaceable() {
        // Producing instructions that cannot be carried out would turn a visible problem into a busy loop.
        // The diff already says what is wrong and why.
        Observation.NodeObservation lost = new Observation.NodeObservation(
            EMPTY_NODE, registration(EMPTY_NODE), heartbeat(EMPTY_NODE, 900), null, Liveness.LOST, POOL, null, 0
        );
        Plan plan = planAfter(5, observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 1))),
            Map.of(EMPTY_NODE, lost)
        ));

        assertTrue(plan.isEmpty());
    }

    @Test
    void onlyEverAddsAndNeverRemoves() {
        // The property that makes the first acting milestone safe: the worst a wrong decision can do is
        // create a surplus copy, which is a self-clearing finding rather than lost data.
        Plan plan = planAfter(5, observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 3))),
            Map.of(EMPTY_NODE, node(EMPTY_NODE, 900))
        ));

        assertFalse(plan.isEmpty());
        plan.instructions().forEach(instruction ->
            assertTrue(instruction instanceof Instruction.PlaceCopy, instruction.getClass().getName()));
    }

    // ---------------------------------------------------------------- node selection

    @Test
    void prefersTheNodeWithTheMostFreeDisk() {
        Plan plan = planAfter(5, observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 1))),
            Map.of(
                FULL_NODE, node(FULL_NODE, 10),
                EMPTY_NODE, node(EMPTY_NODE, 900_000_000)
            )
        ));

        assertEquals(EMPTY_NODE, ((Instruction.PlaceCopy) plan.instructions().get(0)).node());
    }

    @Test
    void spreadsSeveralShardsRatherThanStackingThemAll() {
        // Counting what has already been planned this pass is what stops every shard of an index landing on
        // whichever node happened to be emptiest when the pass started.
        Plan plan = planAfter(5, observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 4))),
            Map.of(
                EMPTY_NODE, node(EMPTY_NODE, 1000),
                FULL_NODE, node(FULL_NODE, 1000)
            )
        ));

        assertEquals(4, plan.instructions().size());
        assertEquals(2, plan.nodesTouched().size(), "both nodes should be used: " + plan.byNode());
    }

    @Test
    void selectionIsDeterministic() {
        // Two nodes with identical free space must produce the same answer every time, or two supervisors
        // computing the same plan could each pick a different node and create a duplicate.
        Observation observation = observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 1))),
            Map.of(EMPTY_NODE, node(EMPTY_NODE, 500), FULL_NODE, node(FULL_NODE, 500))
        );

        NodeId first = ((Instruction.PlaceCopy) planAfter(5, observation).instructions().get(0)).node();
        NodeId again = ((Instruction.PlaceCopy) planAfter(5, observation).instructions().get(0)).node();
        assertEquals(first, again);
    }

    // ---------------------------------------------------------------- bounded work

    @Test
    void stopsAtTheWorkCeiling() {
        // R2.6: a pass over ten thousand indices must not be one giant write, and a large backlog should make
        // progress rather than time out for ever.
        Planner.Policy small = new Planner.Policy(10, 5, 1);
        Observation observation = observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 100))),
            Map.of(EMPTY_NODE, node(EMPTY_NODE, 1_000_000))
        );

        FindingAges ages = new FindingAges();
        Diff diff = Differ.diff(observation, ages);
        diff = Differ.diff(observation, ages);
        Plan plan = Planner.plan(observation, diff, 7, small);

        assertEquals(10, plan.instructions().size());
        assertTrue(plan.ceilingHit(), "hitting the ceiling must be visible, not silent");
    }

    @Test
    void oneLargeTenantCannotConsumeTheWholePass() {
        // The per-tenant floor. Without it a ten-thousand-index tenant in a shared pool starves the small
        // tenant next to it for ever, and the small tenant's operator sees an unexplained standstill.
        String globexUuid = "globexIndexUuidAAAAAA";
        Planner.Policy policy = new Planner.Policy(10, 2, 1);
        Observation observation = observe(5,
            Map.of(
                ACME, Map.of(UUID, index(UUID, "huge", 50)),
                GLOBEX, Map.of(globexUuid, index(globexUuid, "small", 1))
            ),
            Map.of(EMPTY_NODE, node(EMPTY_NODE, 1_000_000))
        );

        FindingAges ages = new FindingAges();
        Diff diff = Differ.diff(observation, ages);
        diff = Differ.diff(observation, ages);
        Plan plan = Planner.plan(observation, diff, 7, policy);

        boolean globexServed = plan.instructions()
            .stream()
            .anyMatch(instruction -> ((Instruction.PlaceCopy) instruction).tenant().equals(GLOBEX));
        assertTrue(globexServed, "the small tenant got nothing: " + plan.instructions());
    }

    @Test
    void aPlanCarriesTheEpochItWasComputedUnder() {
        Plan plan = planAfter(5, observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 1))),
            Map.of(EMPTY_NODE, node(EMPTY_NODE, 900))
        ));

        assertEquals(7, plan.epoch());
        assertEquals(POOL, plan.pool());
    }

    @Test
    void oneShardIsNeverPlannedTwiceInOnePass() {
        Plan plan = planAfter(5, observe(5,
            Map.of(ACME, Map.of(UUID, index(UUID, "my-index", 3))),
            Map.of(EMPTY_NODE, node(EMPTY_NODE, 1000), FULL_NODE, node(FULL_NODE, 1000))
        ));

        long distinct = plan.instructions()
            .stream()
            .map(instruction -> ((Instruction.PlaceCopy) instruction).shard())
            .distinct()
            .count();
        assertEquals(plan.instructions().size(), distinct);
    }

    @Test
    void replicasAreNotPlacedYet() {
        // Milestone 5 acts on one finding. A missing replica stays a finding, which is the honest state of
        // the world rather than a half-built allocator quietly doing something unreviewed.
        IndexConfig withReplica = new IndexConfig("my-index", UUID, true, 1, 1, Replication.SEGMENT, false, 3041);
        Observation observation = observe(5,
            Map.of(ACME, Map.of(UUID, withReplica)),
            Map.of(EMPTY_NODE, nodeHolding(EMPTY_NODE, ShardRole.PRIMARY, ShardState.STARTED))
        );

        FindingAges ages = new FindingAges();
        Diff diff = Differ.diff(observation, ages);
        diff = Differ.diff(observation, ages);

        assertEquals(1, diff.of(FindingKind.UNASSIGNED_REPLICA).size());
        assertTrue(Planner.plan(observation, diff, 7, POLICY).isEmpty());
    }
}
