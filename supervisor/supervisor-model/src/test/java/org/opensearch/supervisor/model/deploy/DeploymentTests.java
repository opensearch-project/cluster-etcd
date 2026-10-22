package org.opensearch.supervisor.model.deploy;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.observe.Observation;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The deployment document and which nodes it covers ({@code DESIGN.md} §29.2). */
class DeploymentTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final NodeId Z1_A = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId Z1_B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    private static final NodeId Z2_C = NodeId.of("nodeCCCCCCCCCCCCCCCCCC");
    private static final long NOW = 1_790_000_000_000L;

    private static Deployment parse(String json) {
        return Deployment.from(json.getBytes(StandardCharsets.UTF_8));
    }

    private static Deployment drain(String zone, Long until) {
        return new Deployment(DeploymentState.DRAIN, Map.of("zone", zone), DrainIntent.RETURN, until, null, null);
    }

    private static Observation.NodeObservation node(NodeId id, String zone) {
        Registration registration = new Registration(id, "eph", id.value(), POOL, Set.of(NodeRole.DATA),
            "10.0.0.1", 9300, "3.7.0", Map.of("zone", zone), 5000, 1L);
        return new Observation.NodeObservation(id, registration, null, null, null, POOL, null, 0);
    }

    private static Observation observe(Map<String, Deployment> deployments, List<String> unreadable) {
        Map<NodeId, Observation.NodeObservation> nodes = new LinkedHashMap<>();
        nodes.put(Z1_A, node(Z1_A, "z1"));
        nodes.put(Z1_B, node(Z1_B, "z1"));
        nodes.put(Z2_C, node(Z2_C, "z2"));
        return new Observation(3, Instant.ofEpochMilli(NOW), POOL, Map.of(), nodes, unreadable, Map.of(), deployments);
    }

    // ---------------------------------------------------------------- the document

    @Test
    void aDrainReadsAsUpstreamWritesIt() {
        Deployment read = parse("{\"state\":\"DRAIN\",\"attributes\":{\"zone\":\"z1\"},\"intent\":\"return\","
            + "\"until\":1790900000000}");

        assertEquals(DeploymentState.DRAIN, read.state());
        assertEquals(Map.of("zone", "z1"), read.attributes());
        assertEquals(DrainIntent.RETURN, read.effectiveIntent());
        assertTrue(read.problems().isEmpty(), read.problems().toString());
        assertEquals(read, Deployment.from(read.toBytes()), "and round-trips");
    }

    @Test
    void statesAndIntentsAreReadWhateverTheirCase() {
        assertEquals(DeploymentState.DIAL_UP, parse("{\"state\":\"dial_up\",\"attributes\":{\"zone\":\"z1\"},\"dial\":0.5}").state());
        assertEquals(DrainIntent.REMOVE, parse("{\"state\":\"DRAIN\",\"attributes\":{\"zone\":\"z1\"},\"intent\":\"Remove\"}").intent());
    }

    @Test
    void anUnrecognisedStateOrIntentIsReadNotRefused() {
        // A newer deployment system may name a phase this build does not know. It must parse, so that the nodes it
        // selects can be frozen; refusing to parse would freeze the whole pool.
        Deployment future = parse("{\"state\":\"CANARY\",\"attributes\":{\"zone\":\"z1\"}}");

        assertEquals(DeploymentState.UNKNOWN, future.state());
        assertFalse(future.problems().isEmpty());
    }

    @Test
    void aDocumentThatCannotBeActedOnSaysWhy() {
        assertTrue(drain("z1", null).problems().get(0).contains("needs an until"), "a returning drain must expire");
        assertTrue(parse("{\"state\":\"DRAIN\",\"attributes\":{}}").problems().get(0).contains("selects no nodes"));
        assertTrue(parse("{\"state\":\"DIAL_UP\",\"attributes\":{\"zone\":\"z1\"},\"dial\":1.5}").problems().get(0)
            .contains("between 0 and 1"));
        assertTrue(parse("{\"state\":\"FINISH\",\"attributes\":{\"zone\":\"z1\"}}").problems().get(0)
            .contains("delete the key"));
        assertTrue(parse("{\"state\":\"DRAIN\",\"attributes\":{\"zone\":\"z1\"},\"intent\":\"remove\"}").problems()
            .isEmpty(), "a removal does not expire, so it needs no until");
    }

    @Test
    void aNodeIsSelectedOnlyIfEveryAttributeMatches() {
        Deployment both = new Deployment(DeploymentState.DRAIN, Map.of("zone", "z1", "rack", "r7"), DrainIntent.REMOVE,
            null, null, null);

        assertTrue(both.selects(Map.of("zone", "z1", "rack", "r7", "version", "3.10")));
        assertFalse(both.selects(Map.of("zone", "z1")));
        assertFalse(both.selects(Map.of("zone", "z1", "rack", "r8")));
    }

    // ---------------------------------------------------------------- coverage

    @Test
    void aValidDrainCoversTheNodesItSelectsAndNoOthers() {
        DeploymentCoverage coverage = DeploymentCoverage.of(observe(Map.of("zone-1", drain("z1", NOW + 60_000)), List.of()));

        assertEquals(Set.of(Z1_A, Z1_B), coverage.byNode().keySet());
        assertTrue(coverage.of(Z1_A).orElseThrow().active());
        assertEquals("zone-1", coverage.of(Z1_A).orElseThrow().id());
        assertTrue(coverage.of(Z2_C).isEmpty());
        assertFalse(coverage.frozen(Z1_A));
    }

    @Test
    void anInvalidDeploymentFreezesTheNodesItSelectsRatherThanReleasingThem() {
        // A drain without its until is still a drain somebody meant; reading it as absent would release its nodes.
        DeploymentCoverage coverage = DeploymentCoverage.of(observe(Map.of("zone-1", drain("z1", null)), List.of()));

        assertTrue(coverage.frozen(Z1_A));
        assertFalse(coverage.of(Z1_A).orElseThrow().active());
        assertFalse(coverage.frozen(Z2_C), "a node it does not select is unaffected");
        assertTrue(coverage.invalid().containsKey("zone-1"));
    }

    @Test
    void aDeploymentThatCannotBeParsedFreezesThePool() {
        DeploymentCoverage coverage = DeploymentCoverage.of(observe(Map.of(), List.of("deployments/zone-1")));

        assertTrue(coverage.poolFrozen());
        assertTrue(coverage.frozen(Z2_C), "which nodes it selects is unknown, so all of them");
        assertEquals(Set.of("zone-1"), coverage.unreadable());
    }

    @Test
    void aNodeSelectedByTwoDeploymentsIsFrozenAndGivenToNeither() {
        Deployment byRack = new Deployment(DeploymentState.DRAIN, Map.of("zone", "z1"), DrainIntent.REMOVE, null, null, null);
        DeploymentCoverage coverage = DeploymentCoverage.of(observe(
            Map.of("zone-1", drain("z1", NOW + 60_000), "decommission", byRack), List.of()));

        assertEquals(Set.of("decommission", "zone-1"), coverage.conflicts().get(Z1_A));
        assertTrue(coverage.frozen(Z1_A));
        assertTrue(coverage.frozen(Z1_B));
    }

    @Test
    void aReturningDrainPastItsUntilReadsAsAbsentAndIsReported() {
        DeploymentCoverage coverage = DeploymentCoverage.of(observe(Map.of("zone-1", drain("z1", NOW - 1)), List.of()));

        assertTrue(coverage.expired().contains("zone-1"));
        assertTrue(coverage.of(Z1_A).orElseThrow().expired());
        assertFalse(coverage.of(Z1_A).orElseThrow().active());
        assertFalse(coverage.frozen(Z1_A), "expired releases, which is the point of a deadline");
    }

    @Test
    void noDeploymentsCoverNothing() {
        assertEquals(DeploymentCoverage.NONE, DeploymentCoverage.of(observe(Map.of(), List.of())));
    }

    // ---------------------------------------------------------------- findings

    private static java.util.List<org.opensearch.supervisor.model.diff.Finding> findings(
        Observation observation, org.opensearch.supervisor.model.diff.FindingKind kind) {
        return org.opensearch.supervisor.model.diff.Differ.diff(observation).of(kind);
    }

    @Test
    void eachThingThatCannotBeActedOnIsReportedAndSaysWhatItFreezes() {
        var invalid = findings(observe(Map.of("zone-1", drain("z1", null)), List.of()),
            org.opensearch.supervisor.model.diff.FindingKind.DEPLOYMENT_INVALID);
        assertEquals(1, invalid.size(), invalid.toString());
        assertTrue(invalid.get(0).because().contains("needs an until"), invalid.get(0).because());
        assertTrue(invalid.get(0).because().contains("frozen"), invalid.get(0).because());

        var unreadable = findings(observe(Map.of(), List.of("deployments/zone-1")),
            org.opensearch.supervisor.model.diff.FindingKind.DEPLOYMENT_INVALID);
        assertEquals(1, unreadable.size(), unreadable.toString());
        assertTrue(unreadable.get(0).because().contains("every node of this pool is frozen"), unreadable.get(0).because());
        assertTrue(findings(observe(Map.of(), List.of("deployments/zone-1")),
            org.opensearch.supervisor.model.diff.FindingKind.UNREADABLE_DOCUMENT).isEmpty(),
            "reported once, by the kind that says what it freezes");
    }

    @Test
    void twoInvalidDeploymentsInOnePoolAreTwoFindings() {
        // A deployment has its own subject, so one finding's age is not the other's.
        var invalid = findings(observe(Map.of("zone-1", drain("z1", null), "zone-2", drain("z2", null)), List.of()),
            org.opensearch.supervisor.model.diff.FindingKind.DEPLOYMENT_INVALID);

        assertEquals(2, invalid.size(), invalid.toString());
        assertFalse(invalid.get(0).subject().key().equals(invalid.get(1).subject().key()));
    }

    @Test
    void aConflictIsReportedPerNodeAndAnExpiryPerDeployment() {
        Deployment byZone = new Deployment(DeploymentState.DRAIN, Map.of("zone", "z1"), DrainIntent.REMOVE, null, null, null);
        var conflicts = findings(observe(Map.of("zone-1", drain("z1", NOW + 60_000), "decommission", byZone), List.of()),
            org.opensearch.supervisor.model.diff.FindingKind.DEPLOYMENT_CONFLICT);
        assertEquals(2, conflicts.size(), "one per node both select: " + conflicts);

        var expired = findings(observe(Map.of("zone-1", drain("z1", NOW - 1)), List.of()),
            org.opensearch.supervisor.model.diff.FindingKind.DEPLOYMENT_EXPIRED);
        assertEquals(1, expired.size(), expired.toString());
    }
}
