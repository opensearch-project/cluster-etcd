package org.opensearch.supervisor.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.supervisor.fakenode.FakeNode;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.allocate.RemoteStoreDecider;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.plan.Planner;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.status.Replication;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks {@code SUPERVISOR_KEYS.md} against what a real pass actually writes.
 *
 * <p><b>Why this is worth a test rather than a periodic read-through.</b> These documents are the interface to
 * two other repositories, and the document is the only specification of them — so drift is not a documentation
 * problem, it is an interface problem that nothing else in either build can see. It had drifted in both
 * directions at once: fields written and never documented, fields documented and never written, one field
 * renamed, and two whole documents described in detail that no code writes at all.
 *
 * <p>The undocumented direction turned out to be the dangerous one. {@code serving} on a shard placement
 * existed only because {@code isServing()} is named the way Jackson detects, and the data plane had already
 * started reading it — so "tidying up an accidental getter", which is a change this repository has correctly
 * made twice for the same reason, would have silently broken coordinator routing in another repository with no
 * test failing here.
 *
 * <p>Compares <b>field paths, not values</b>. The examples in the document are illustrative and their numbers
 * are made up; their field names are the contract. Objects whose keys are data rather than schema — a map of
 * shard number to copies, of colour to count, of tenant to summary — are collapsed to {@code *}, which is the
 * one piece of knowledge this test needs that it cannot read off either side.
 *
 * <p>Scope is §1 and §4, the documents this service projects. §2's task records and §3's pool and lease
 * documents are either the control plane's or covered by the tests that write them.
 */
class SupervisorKeysDocumentTests {

    private static final PoolId POOL = PoolId.of("shared-a");
    private static final TenantId ACME = TenantId.of("acme");
    private static final NodeId A = NodeId.of("nodeAAAAAAAAAAAAAAAAAA");
    private static final NodeId B = NodeId.of("nodeBBBBBBBBBBBBBBBBBB");
    private static final String PLACED = "uzKxLWZu4nPSrTknEcDpoA";
    private static final String STRAINED = "vBxKLWZu4nPSrTknEcDpoA";

    /**
     * Objects whose keys are data, not schema.
     *
     * <p>Everything else is compared key by key. This list is deliberately short and explicit: a test that
     * guessed which objects were maps would quietly stop checking whichever document it guessed wrong about.
     */
    private static final Set<String> DATA_KEYED = Set.of(
        "shards", "indices_by_status", "tenants", "attributes", "counts", "findings_by_kind",
        "liveness.effective_millis"
    );

    private static final ObjectMapper JSON = new ObjectMapper();

    private InMemoryKeyValueStore store;
    private MutableClock clock;

    private static final class MutableClock extends Clock {
        private Instant now = Instant.ofEpochMilli(1_000_000);

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

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        clock = new MutableClock();
        store.put(Keys.poolDefinition(POOL), new PoolDefinition(
            POOL, PoolKind.SHARED, false, null, null, List.of("zone"), false).toBytes());
        store.put(Keys.poolBinding(ACME), PoolBinding.to(POOL).toBytes());
        store.put(Keys.tenantRegistration(ACME),
            "{\"tenant\":\"acme\"}".getBytes(StandardCharsets.UTF_8));

        FakeNode a = node(A, "z1");
        FakeNode b = node(B, "z2");
        a.start();
        b.start();
        a.tick();
        b.tick();

        // One index that converges, so the placement fields are populated and the health is green...
        store.put(Keys.indicesPrefix(ACME) + "placed", IndexConfig.toDocument(
            "placed", PLACED, true, IndexConfig.settingsOf(2, 0, 1, Replication.SEGMENT)));
        // ...and one asking for more copies than a two-node pool can hold, so the diff has a finding in it with
        // an allocator refusal attached. An empty `findings` array would check nothing about a finding's shape,
        // which is the field list this document exists for.
        store.put(Keys.indicesPrefix(ACME) + "strained", IndexConfig.toDocument(
            "strained", STRAINED, true, IndexConfig.settingsOf(1, 0, 3, Replication.SEGMENT)));

        Supervisor supervisor = new Supervisor(store, clock, "s1", Set.of(), 1.0, Duration.ofSeconds(15),
            Balancer.Config.defaults(), new Planner.Policy(200, 5, 2), false);

        for (int pass = 0; pass < 14; pass++) {
            supervisor.tick();
            a.tick();
            b.tick();
            clock.advance(Duration.ofSeconds(2));
        }
    }

    private FakeNode node(NodeId id, String zone) {
        return new FakeNode(store, new Registration(
            id, "placeholder", id.value(), POOL, EnumSet.of(NodeRole.DATA, NodeRole.COORDINATOR),
            "10.0.0." + id.value().charAt(4), 9300, "3.7.0",
            Map.of("zone", zone, "rack", "r7", RemoteStoreDecider.SEGMENT_REPOSITORY_ATTRIBUTE, "segments"),
            5000, 0L
        ), clock, "eph-" + id.value());
    }

    // ---------------------------------------------------------------- the checks

    @Test
    void theIndexStatusDocumentMatchesItsDescription() {
        assertDocumented("tenant/<t>/status/indices/<index-uuid>", Keys.indexStatus(ACME, PLACED));
    }

    @Test
    void theClusterStatusDocumentMatchesItsDescription() {
        assertDocumented("tenant/<t>/status/cluster", Keys.clusterStatus(ACME));
    }

    @Test
    void theNodeStatusDocumentMatchesItsDescription() {
        assertDocumented("tenant/<t>/status/nodes/<node-id>", Keys.nodeStatus(ACME, A));
    }

    @Test
    void theAwarenessDocumentMatchesItsDescription() {
        assertDocumented("tenant/<t>/status/awareness", Keys.awarenessStatus(ACME));
    }

    @Test
    void theConvergenceDocumentMatchesItsDescription() {
        assertDocumented("supervisor/status/pools/<pool-id>/convergence", Keys.poolConvergence(POOL));
    }

    @Test
    void theTenantDiffDocumentMatchesItsDescription() {
        assertDocumented("supervisor/status/diff/<tenant-id>", Keys.tenantDiff(ACME));
    }

    @Test
    void theHousekeepingDocumentMatchesItsDescription() {
        assertDocumented("supervisor/status/housekeeping", Keys.HOUSEKEEPING_STATUS);
    }

    @Test
    void theMemberStatusDocumentMatchesItsDescription() {
        assertDocumented("supervisor/status/members/<supervisor-id>", Keys.memberStatus("s1"));
    }

    @Test
    void theFixtureProducesADiffWithAFindingInIt() {
        // Guards the guard. Every check above is only as good as the document it read, and a finding-shaped
        // check against an empty findings array passes without comparing anything.
        JsonNode diff = actual(Keys.tenantDiff(ACME));
        assertTrue(diff.get("findings").size() > 0,
            "the strained index has to produce a finding, or the diff check is vacuous: " + diff);
        assertNotNull(diff.get("findings").get(0).get("unplaceable_because"),
            "and a refusal, or the field the deciders exist to produce goes unchecked");
    }

    @Test
    void everyDocumentDescribedInTheFileIsEitherCheckedHereOrMarkedUnimplemented() {
        // The rule that stops this test rotting: a new section describing a document must either be covered
        // above or say plainly that nothing writes it. Two sections had drifted into describing, in detail and
        // in the present tense, documents that do not exist.
        Set<String> checked = Set.of(
            "tenant/<t>/status/indices/<index-uuid>",
            "tenant/<t>/status/cluster",
            "tenant/<t>/status/nodes/<node-id>",
            "tenant/<t>/status/awareness",
            "supervisor/status/pools/<pool-id>/convergence",
            "supervisor/status/diff/<tenant-id>",
            "supervisor/status/housekeeping",
            "supervisor/status/members/<supervisor-id>",
            // Checked by MonitoringTests rather than here: it is absent unless a reap happened, so producing one
            // means running a pool down to a lost node, which is a different fixture from this one.
            "supervisor/status/pools/<pool-id>/reaping"
        );
        List<String> unaccounted = new ArrayList<>();
        for (Section section : sectionsOf(projectedSectionsOf(document()))) {
            if (checked.contains(section.key()) || section.unimplemented() || section.shapeDeferred()) {
                continue;
            }
            unaccounted.add(section.key());
        }
        assertEquals(List.of(), unaccounted,
            "each of these describes a document that is neither checked by this test nor marked as not"
                + " implemented; add a check, or say in the document that nothing writes it");
    }

    // ---------------------------------------------------------------- the machinery

    private void assertDocumented(String key, String actualKey) {
        Section section = sectionsOf(projectedSectionsOf(document())).stream()
            .filter(candidate -> candidate.key().equals(key))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no section in SUPERVISOR_KEYS.md for " + key));
        assertFalse(section.unimplemented(), key + " is marked as not implemented but something wrote it");

        Set<String> documented = fieldsOf(parse(section.json(), key));
        Set<String> written = fieldsOf(actual(actualKey));

        Set<String> missing = new TreeSet<>(documented);
        missing.removeAll(written);
        Set<String> extra = new TreeSet<>(written);
        extra.removeAll(documented);

        assertEquals(Set.of(), missing, () -> key + ": documented and not written — " + missing
            + "\n  a reader following the document would find these absent."
            + "\n  written: " + new TreeSet<>(written));
        assertEquals(Set.of(), extra, () -> key + ": written and not documented — " + extra
            + "\n  another repository may already be reading one of these. Check before deleting it."
            + "\n  documented: " + new TreeSet<>(documented));
    }

    private JsonNode actual(String key) {
        byte[] value = store.get(key)
            .orElseThrow(() -> new AssertionError("nothing was written to " + key))
            .value();
        return parse(new String(value, StandardCharsets.UTF_8), key);
    }

    private static JsonNode parse(String json, String what) {
        try {
            return JSON.readTree(json);
        } catch (IOException e) {
            throw new AssertionError("could not parse the JSON for " + what + ": " + json, e);
        }
    }

    /** Every field path in a document, with data-keyed maps collapsed to {@code *}. */
    private static Set<String> fieldsOf(JsonNode node) {
        Set<String> paths = new LinkedHashSet<>();
        flatten(node, "", paths);
        return paths;
    }

    private static void flatten(JsonNode node, String path, Set<String> paths) {
        if (node.isObject()) {
            if (DATA_KEYED.contains(path)) {
                // The keys are data, so only the shape of the values is schema.
                node.forEach(value -> flatten(value, path + ".*", paths));
                return;
            }
            node.fieldNames().forEachRemaining(name -> {
                String child = path.isEmpty() ? name : path + "." + name;
                paths.add(child);
                flatten(node.get(name), child, paths);
            });
        } else if (node.isArray() && node.isEmpty() == false) {
            // One element is enough: a heterogeneous array would not be a schema either side could describe.
            flatten(node.get(0), path + "[]", paths);
        }
    }

    // ---------------------------------------------------------------- reading the document

    /** One `### `key`` heading, its prose, and the first JSON block under it. */
    private record Section(String key, String json, boolean unimplemented, boolean shapeDeferred) {}

    private static final Pattern HEADING = Pattern.compile("^### `([^`]+)`", Pattern.MULTILINE);

    /**
     * Only §1 and §4, the documents this service projects.
     *
     * <p>§2's task records are the control plane's shape and §3's pool, binding and lease documents are written by
     * paths with their own tests. Scoping by heading rather than by an exclusion list so that a new section added
     * to §1 or §4 is covered automatically — which is the direction drift actually came from.
     */
    private static String projectedSectionsOf(String document) {
        return slice(document, "## 1. Projections", "## 2. ") + slice(document, "## 4. Observability", "## 5. ");
    }

    private static String slice(String document, String from, String to) {
        int start = document.indexOf(from);
        int end = document.indexOf(to, start < 0 ? 0 : start);
        if (start < 0 || end < 0) {
            throw new AssertionError("SUPERVISOR_KEYS.md no longer has a section starting [" + from + "]");
        }
        return document.substring(start, end);
    }

    private static List<Section> sectionsOf(String document) {
        List<Section> sections = new ArrayList<>();
        Matcher headings = HEADING.matcher(document);
        List<int[]> bounds = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        while (headings.find()) {
            keys.add(headings.group(1));
            bounds.add(new int[] { headings.end(), document.length() });
        }
        for (int i = 0; i < bounds.size(); i++) {
            int end = i + 1 < bounds.size() ? bounds.get(i + 1)[0] : document.length();
            String body = document.substring(bounds.get(i)[0], end);
            int open = body.indexOf("```json");
            String json = null;
            if (open >= 0) {
                int close = body.indexOf("```", open + 7);
                json = body.substring(open + 7, close < 0 ? body.length() : close);
            }
            sections.add(new Section(
                keys.get(i), json,
                body.contains("**Nothing writes this yet.**"),
                json == null || body.contains("Shape deferred")
            ));
        }
        return sections;
    }

    private static String document() {
        Path root = Paths.get("").toAbsolutePath();
        while (root != null && Files.exists(root.resolve("SUPERVISOR_KEYS.md")) == false) {
            root = root.getParent();
        }
        if (root == null) {
            throw new AssertionError("could not find SUPERVISOR_KEYS.md above " + Paths.get("").toAbsolutePath());
        }
        try {
            return Files.readString(root.resolve("SUPERVISOR_KEYS.md"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
