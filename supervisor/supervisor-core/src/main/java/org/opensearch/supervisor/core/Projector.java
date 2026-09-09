package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Finding;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.diff.Severity;
import org.opensearch.supervisor.model.observe.Observation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes the diff and the convergence figures into the store, which is how an operator asks "what does this
 * think is wrong right now?".
 *
 * <p>Projected rather than served. An HTTP endpoint would make this a second API service, which
 * {@code REQUIREMENTS.md} §9 rules out, and would be unreachable in exactly the situation where it is
 * wanted — a wedged process. The cost is that every document here must be <b>bounded</b>, so the diff is
 * truncated and carries exact counts beside the truncated list. The summary is never wrong even when the
 * detail is incomplete.
 *
 * <p>Documents are written only when their content changes. Rewriting per-tenant diffs every pass for a
 * quiet deployment would make the supervisor the store's dominant writer, ahead of the nodes.
 */
public final class Projector {

    /** Findings kept in a projected diff. The rest are counted and dropped. */
    public static final int MAX_FINDINGS_SHOWN = 50;

    /** Per-tenant entries in the convergence document, worst first. Must not grow with tenant count. */
    public static final int MAX_TENANTS_SHOWN = 20;

    /** Passes of history kept for the trend. */
    public static final int TREND_WINDOW = 20;

    private final KeyValueStore store;
    private final String supervisorId;
    private final Map<String, String> lastWritten = new LinkedHashMap<>();
    private final Deque<Integer> recentTotals = new ArrayDeque<>();

    private long lastEmptyDiffAt;
    private long lastEmptyDiffPass;

    public Projector(KeyValueStore store, String supervisorId) {
        this.store = Objects.requireNonNull(store, "store");
        this.supervisorId = Objects.requireNonNull(supervisorId, "supervisorId");
    }

    /** What a projected diff looks like on the wire. */
    public record TenantDiff(
        TenantId tenant,
        PoolId pool,
        long pass,
        long observedAt,
        Map<String, Integer> counts,
        boolean truncated,
        int shown,
        int total,
        List<ProjectedFinding> findings
    ) {}

    public record ProjectedFinding(
        String kind,
        String severity,
        String subject,
        String because,
        long firstObservedPass,
        long passes
    ) {}

    /** Per-pool convergence: the answer to "is this getting better?", which one number cannot give. */
    public record Convergence(
        PoolId pool,
        String holder,
        long pass,
        long passStartedAt,
        long passDurationMillis,
        int nodes,
        int placeableNodes,
        int indices,
        int findingsTotal,
        Map<String, Integer> findingsByKind,
        long lastEmptyDiffAt,
        long passesSinceEmpty,
        Trend trend,
        Map<String, TenantSummary> tenants,
        int unreadableDocuments,
        long watchPollFailures
    ) {}

    /** Per tenant, capped at the worst {@link #MAX_TENANTS_SHOWN} so this cannot grow with tenant count. */
    public record TenantSummary(int findings, long oldestPasses) {}

    /**
     * @param delta change in finding count across the window. Negative is recovery; zero with a non-empty
     *     diff is the alarm {@code REQUIREMENTS.md} names — a reconciler that is quietly failing looks
     *     exactly like one with nothing to do.
     */
    public record Trend(int window, int delta) {}

    public void project(Diff diff, Observation observation, long passDurationMillis, long watchPollFailures) {
        projectTenantDiffs(diff, observation);
        projectConvergence(diff, observation, passDurationMillis, watchPollFailures);
    }

    private void projectTenantDiffs(Diff diff, Observation observation) {
        // A tenant with findings gets a document; a tenant whose findings have all cleared gets an empty
        // one rather than a deleted key, so that "nothing wrong" is distinguishable from "never looked".
        Map<TenantId, List<Finding>> byTenant = new LinkedHashMap<>();
        observation.tenants().keySet().forEach(tenant -> byTenant.put(tenant, new ArrayList<>()));
        for (Finding finding : diff.findings()) {
            TenantId tenant = finding.subject().tenant();
            if (tenant != null) {
                byTenant.computeIfAbsent(tenant, ignored -> new ArrayList<>()).add(finding);
            }
        }

        byTenant.forEach((tenant, findings) -> {
            Map<String, Integer> counts = new LinkedHashMap<>();
            findings.forEach(finding -> counts.merge(finding.kind().name(), 1, Integer::sum));

            List<ProjectedFinding> shown = findings.stream()
                .limit(MAX_FINDINGS_SHOWN)
                .map(Projector::project)
                .toList();

            // The change check deliberately ignores the pass number and the timestamp. Including them
            // would make every pass a write for every tenant -- exactly the write amplification this
            // method exists to avoid, and worst for the documents that scale with tenant count.
            //
            // So a stored `pass` is "the pass at which these findings last changed", not "the last pass
            // that looked". That is the more useful of the two readings anyway, and an operator gets
            // current-ness by comparing it against the convergence document, which is one key per pool and
            // is rewritten every pass.
            write(Keys.tenantDiff(tenant), new TenantDiff(
                tenant, diff.pool(), diff.pass(), observation.observedAt().toEpochMilli(),
                counts, findings.size() > MAX_FINDINGS_SHOWN, shown.size(), findings.size(), shown
            ), "tenant diff", Json.digest(Json.write(List.of(counts, shown, findings.size()), "diff content")));
        });
    }

    private static ProjectedFinding project(Finding finding) {
        return new ProjectedFinding(
            finding.kind().name(),
            finding.severity().name().toLowerCase(java.util.Locale.ROOT),
            finding.subject().key(),
            finding.because(),
            finding.firstObservedPass(),
            finding.passes()
        );
    }

    private void projectConvergence(
        Diff diff,
        Observation observation,
        long passDurationMillis,
        long watchPollFailures
    ) {
        int total = diff.findings().size();
        recentTotals.addLast(total);
        while (recentTotals.size() > TREND_WINDOW) {
            recentTotals.removeFirst();
        }
        int delta = total - recentTotals.peekFirst();

        if (total == 0) {
            lastEmptyDiffAt = observation.observedAt().toEpochMilli();
            lastEmptyDiffPass = diff.pass();
        }

        Map<String, Integer> byKind = new LinkedHashMap<>();
        diff.countsByKind().forEach((kind, count) -> byKind.put(kind.name(), count));

        int indices = observation.tenants().values().stream()
            .mapToInt(desired -> desired.indicesByUuid().size())
            .sum();

        write(Keys.poolConvergence(diff.pool()), new Convergence(
            diff.pool(),
            supervisorId,
            diff.pass(),
            observation.observedAt().toEpochMilli(),
            passDurationMillis,
            observation.nodes().size(),
            observation.placeableNodes().size(),
            indices,
            total,
            byKind,
            lastEmptyDiffAt,
            lastEmptyDiffPass == 0 ? diff.pass() : diff.pass() - lastEmptyDiffPass,
            new Trend(recentTotals.size(), delta),
            worstTenants(diff),
            observation.unreadable().size(),
            watchPollFailures
        ), "convergence", null);
    }

    /**
     * The worst tenants by finding count, so an operator can tell which one to look at without scanning
     * every per-tenant document. Capped, because a per-tenant map in a per-pool document is exactly the
     * kind of field that quietly grows until it exceeds the value size limit.
     */
    private static Map<String, TenantSummary> worstTenants(Diff diff) {
        Map<String, int[]> counts = new LinkedHashMap<>();
        for (Finding finding : diff.findings()) {
            if (finding.subject().tenant() == null) {
                continue;
            }
            int[] entry = counts.computeIfAbsent(finding.subject().tenant().value(), ignored -> new int[2]);
            entry[0]++;
            entry[1] = Math.max(entry[1], (int) finding.passes());
        }
        return counts.entrySet()
            .stream()
            .sorted((a, b) -> Integer.compare(b.getValue()[0], a.getValue()[0]))
            .limit(MAX_TENANTS_SHOWN)
            .collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey,
                entry -> new TenantSummary(entry.getValue()[0], entry.getValue()[1]),
                (a, b) -> a,
                LinkedHashMap::new
            ));
    }

    /**
     * Writes only if the content differs from what this process last wrote to that key.
     *
     * <p>Comparing against what we wrote rather than re-reading is deliberate: it costs no reads, and being
     * wrong about it is harmless — a supervisor that has just taken over the pool writes once more than it
     * needed to, which is the correct behaviour anyway since the previous holder's document is stale.
     *
     * @param contentDigest what to compare on, when that is narrower than the whole document. Passing
     *     {@code null} compares the serialised bytes, which is right for a document whose every field is
     *     meaningful and wrong for one carrying a pass number that changes on its own.
     */
    private void write(String key, Object document, String what, String contentDigest) {
        byte[] bytes = Json.write(document, what);
        String digest = contentDigest == null ? Json.digest(bytes) : contentDigest;
        if (digest.equals(lastWritten.get(key))) {
            return;
        }
        store.put(key, bytes);
        lastWritten.put(key, digest);
    }

    /** How many keys this projector has written, for tests and for the housekeeping document. */
    public int projectedKeys() {
        return lastWritten.size();
    }

    /** Highest severity anywhere in the diff, which is what an alerting rule wants to key on. */
    public static Severity worst(Diff diff) {
        return diff.findings().stream()
            .map(Finding::severity)
            .max(java.util.Comparator.comparingInt(Enum::ordinal))
            .orElse(Severity.INFO);
    }

    /** Findings of a kind that have held long enough to be worth acting on, for the planner later. */
    public static List<Finding> aged(Diff diff, FindingKind kind, long minimumPasses) {
        return diff.of(kind).stream().filter(finding -> finding.passes() >= minimumPasses).toList();
    }
}
