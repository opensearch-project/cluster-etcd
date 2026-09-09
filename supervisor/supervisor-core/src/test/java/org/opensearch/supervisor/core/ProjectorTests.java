package org.opensearch.supervisor.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Finding;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.diff.Subject;
import org.opensearch.supervisor.model.observe.Observation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The observability surface, which R7 makes a requirement rather than a nicety: a reconciler that is
 * quietly failing looks exactly like one with nothing to do, and both produce no output.
 */
class ProjectorTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private InMemoryKeyValueStore store;
    private Projector projector;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        projector = new Projector(store, "supervisor-a1b2");
    }

    private static Observation observation(long pass) {
        return new Observation(
            pass, Instant.ofEpochMilli(1_000_000), POOL,
            Map.of(ACME, new Observation.DesiredState(Map.of())), Map.of(), List.of()
        );
    }

    private static Diff diff(long pass, int count) {
        List<Finding> findings = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            findings.add(new Finding(
                FindingKind.UNASSIGNED_PRIMARY, Subject.ofShard(ACME, UUID, i),
                "shard " + i + " has no primary", pass, 1
            ));
        }
        return new Diff(pass, POOL, findings);
    }

    private com.fasterxml.jackson.databind.JsonNode read(String key) throws Exception {
        return Json.mapper().readTree(store.get(key).orElseThrow().value());
    }

    @Test
    void projectsTheDiffWhereAnOperatorCanReadIt() throws Exception {
        projector.project(diff(1, 3), observation(1), 240, 0);

        var document = read(Keys.tenantDiff(ACME));
        assertEquals("acme", document.get("tenant").asText());
        assertEquals(3, document.get("total").asInt());
        assertEquals(3, document.get("counts").get("UNASSIGNED_PRIMARY").asInt());
        assertFalse(document.get("truncated").asBoolean());
        assertEquals("shard 0 has no primary", document.get("findings").get(0).get("because").asText());
    }

    @Test
    void aTruncatedDiffKeepsItsCountsExact() throws Exception {
        // The cost of projecting rather than serving: the document has to be bounded. A truncated list with
        // exact counts beside it is never wrong, only incomplete.
        projector.project(diff(1, 500), observation(1), 240, 0);

        var document = read(Keys.tenantDiff(ACME));
        assertTrue(document.get("truncated").asBoolean());
        assertEquals(Projector.MAX_FINDINGS_SHOWN, document.get("shown").asInt());
        assertEquals(500, document.get("total").asInt());
        assertEquals(500, document.get("counts").get("UNASSIGNED_PRIMARY").asInt());
        assertEquals(Projector.MAX_FINDINGS_SHOWN, document.get("findings").size());
    }

    @Test
    void aTenantWithNothingWrongGetsAnEmptyDocumentRatherThanNoDocument() throws Exception {
        // "Nothing wrong" and "never looked" are different facts and an absent key cannot tell them apart.
        projector.project(new Diff(1, POOL, List.of()), observation(1), 240, 0);

        var document = read(Keys.tenantDiff(ACME));
        assertEquals(0, document.get("total").asInt());
        assertEquals(0, document.get("findings").size());
    }

    @Test
    void convergenceCarriesTheTrendRatherThanJustACount() throws Exception {
        // Twelve findings shrinking is a recovery; twelve findings flat is the alarm. One number cannot
        // tell those apart, which is the whole reason this document exists.
        projector.project(diff(1, 10), observation(1), 100, 0);
        projector.project(diff(2, 7), observation(2), 100, 0);
        projector.project(diff(3, 4), observation(3), 100, 0);

        var document = read(Keys.poolConvergence(POOL));
        assertEquals(4, document.get("findings_total").asInt());
        assertEquals(-6, document.get("trend").get("delta").asInt(), "shrinking, so negative");
        assertEquals("supervisor-a1b2", document.get("holder").asText());
        assertEquals(3, document.get("pass").asInt());
    }

    @Test
    void aStableNonEmptyDiffShowsNoImprovement() {
        for (long pass = 1; pass <= 5; pass++) {
            projector.project(diff(pass, 8), observation(pass), 100, 0);
        }

        // delta of zero with a non-empty diff is exactly the condition REQUIREMENTS.md calls the alarm.
        var convergence = store.get(Keys.poolConvergence(POOL)).orElseThrow();
        String json = new String(convergence.value(), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(json.contains("\"delta\":0"), json);
        assertTrue(json.contains("\"findings_total\":8"), json);
    }

    @Test
    void passesSinceEmptyCountsUpFromTheLastCleanPass() throws Exception {
        projector.project(new Diff(1, POOL, List.of()), observation(1), 100, 0);
        projector.project(diff(2, 1), observation(2), 100, 0);
        projector.project(diff(3, 1), observation(3), 100, 0);

        assertEquals(2, read(Keys.poolConvergence(POOL)).get("passes_since_empty").asInt());
    }

    @Test
    void anUnchangedDocumentIsNotRewritten() {
        // Rewriting per-tenant diffs every pass for a quiet deployment would make the supervisor the
        // store's dominant writer, ahead of the nodes it is watching.
        projector.project(diff(1, 2), observation(1), 100, 0);
        long revision = store.get(Keys.tenantDiff(ACME)).orElseThrow().revision();

        // Same findings, same pass content -- the convergence document changes because the pass number
        // does, but the tenant diff does not.
        projector.project(diff(1, 2), observation(1), 100, 0);

        assertEquals(revision, store.get(Keys.tenantDiff(ACME)).orElseThrow().revision());
    }

    @Test
    void aTenantWithAPersistentFindingIsRewrittenSoItsAgeStaysCurrent() {
        // The trade-off in the change check, made explicit. A finding's age advances every pass, so a
        // broken tenant's document is rewritten every pass -- which is right, because "how long has this
        // been wrong" is the most useful field on it and it is only hot while something is wrong. A healthy
        // tenant, which is the common case, is silent.
        projector.project(new Diff(1, POOL, List.of(
            new Finding(FindingKind.UNASSIGNED_PRIMARY, Subject.ofShard(ACME, UUID, 0), "no primary", 1, 1)
        )), observation(1), 100, 0);
        long broken = store.get(Keys.tenantDiff(ACME)).orElseThrow().revision();

        projector.project(new Diff(2, POOL, List.of(
            new Finding(FindingKind.UNASSIGNED_PRIMARY, Subject.ofShard(ACME, UUID, 0), "no primary", 1, 2)
        )), observation(2), 100, 0);

        assertTrue(store.get(Keys.tenantDiff(ACME)).orElseThrow().revision() != broken,
            "an ageing finding must keep its document current");

        projector.project(new Diff(3, POOL, List.of()), observation(3), 100, 0);
        long healthy = store.get(Keys.tenantDiff(ACME)).orElseThrow().revision();
        projector.project(new Diff(4, POOL, List.of()), observation(4), 100, 0);

        assertEquals(healthy, store.get(Keys.tenantDiff(ACME)).orElseThrow().revision(),
            "a healthy tenant should cost no writes at all");
    }

    @Test
    void theConvergenceDocumentNamesTheWorstTenant() throws Exception {
        projector.project(diff(1, 3), observation(1), 100, 0);

        var tenants = read(Keys.poolConvergence(POOL)).get("tenants");
        assertEquals(3, tenants.get("acme").get("findings").asInt());
    }

    @Test
    void aChangedDiffIsRewritten() {
        projector.project(diff(1, 2), observation(1), 100, 0);
        long revision = store.get(Keys.tenantDiff(ACME)).orElseThrow().revision();

        projector.project(diff(2, 3), observation(2), 100, 0);

        assertTrue(store.get(Keys.tenantDiff(ACME)).orElseThrow().revision() != revision);
    }

    @Test
    void theWatchFailureCounterIsPublished() throws Exception {
        // A silently dead watch looks exactly like a quiet keyspace, which is this mechanism's most likely
        // failure. WORKING_NOTES.md and REQUIREMENTS.md §8 both insist it be visible.
        projector.project(diff(1, 0), observation(1), 100, 17);

        assertEquals(17, read(Keys.poolConvergence(POOL)).get("watch_poll_failures").asInt());
    }

    @Test
    void everyProjectedFindingKeepsItsReason() throws Exception {
        projector.project(diff(1, 5), observation(1), 100, 0);

        var findings = read(Keys.tenantDiff(ACME)).get("findings");
        for (var finding : findings) {
            assertFalse(finding.get("because").asText().isBlank());
            assertTrue(finding.has("first_observed_pass"));
            assertTrue(finding.has("passes"));
        }
    }

    @Test
    void projectedDocumentsStayUnderTheObservabilityBudget() {
        // SUPERVISOR_KEYS.md §5: 64 KiB for supervisor/status/*, and these are the only documents allowed
        // to truncate at all.
        projector.project(diff(1, 5000), observation(1), 100, 0);

        assertTrue(store.get(Keys.tenantDiff(ACME)).orElseThrow().value().length < 64 * 1024);
        assertTrue(store.get(Keys.poolConvergence(POOL)).orElseThrow().value().length < 64 * 1024);
    }

    @Test
    void findingsOnNodesRatherThanTenantsDoNotVanish() {
        // A node-scoped finding has no tenant, so a naive per-tenant projection would drop it entirely --
        // and NODE_LOST is one of the most important things this service can say.
        Diff nodeDiff = new Diff(1, POOL, List.of(
            Finding.of(FindingKind.NODE_LOST, Subject.ofNode(NodeId.of("n1")), 1, "silent past the threshold")
        ));
        projector.project(nodeDiff, observation(1), 100, 0);

        String convergence = new String(
            store.get(Keys.poolConvergence(POOL)).orElseThrow().value(), java.nio.charset.StandardCharsets.UTF_8
        );
        assertTrue(convergence.contains("NODE_LOST"), "a node finding must survive into the convergence counts");
        assertEquals(1, projectorFindingsTotal(convergence));
    }

    private static int projectorFindingsTotal(String json) {
        int at = json.indexOf("\"findings_total\":");
        int end = json.indexOf(',', at);
        return Integer.parseInt(json.substring(at + "\"findings_total\":".length(), end));
    }
}
