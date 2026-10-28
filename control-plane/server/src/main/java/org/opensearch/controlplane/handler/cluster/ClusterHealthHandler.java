/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.handler.cluster.StatusProjections.Copy;
import org.opensearch.controlplane.handler.cluster.StatusProjections.IndexHealth;
import org.opensearch.controlplane.handler.cluster.StatusProjections.Status;
import org.opensearch.controlplane.handler.cluster.StatusProjections.Totals;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.resolver.IndexNameExpressionResolver;
import org.opensearch.controlplane.resolver.IndexResolution;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * {@code GET /_cluster/health} and {@code GET /_cluster/health/{index}}, answered from the supervisor's
 * projections.
 *
 * <p>Handed here by the data plane, which refuses both for a tenant (its {@code FINDINGS_FROM_THE_DATA_PLANE.md},
 * item 1, in this component). A read of documents the supervisor writes, so it needs no node. See
 * {@link StatusProjections} for which document supplies what, and why the figures are the supervisor's rather
 * than recomputed.
 *
 * <h2>Waiting</h2>
 *
 * <p>The {@code wait_for_*} parameters and an index name each add a condition, as upstream counts them in
 * {@code TransportClusterHealthAction}; the request re-reads the projections until every condition holds or
 * {@code timeout} (30 s by default) passes, then answers {@code 408} with the full body and
 * {@code timed_out: true}. With no condition it answers at once. Blocking is affordable because dispatch runs on
 * a virtual thread, off the event loop. {@code wait_for_events} is accepted and validated, and is satisfied at
 * once: it waits for upstream's cluster-state task queue to drain, and there is no queue here.
 *
 * <h2>No supervisor</h2>
 *
 * <p>When {@code status/cluster} is absent or older than its heartbeat plus a failover, nothing is managing the
 * tenant, which is upstream's "no cluster manager discovered". Answered as upstream answers that: the request
 * waits {@code cluster_manager_timeout} (by default {@code timeout}) for one to appear, then fails with
 * {@code 503 cluster_manager_not_discovered_exception} and a null reason. With {@code local=true} it answers the
 * last figures instead, red, with {@code discovered_cluster_manager: false}.
 *
 * <h2>Considered divergences</h2>
 *
 * <ul>
 *   <li>{@code relocating_shards} and {@code delayed_unassigned_shards} are always 0, because no copy here
 *       relocates and nothing delays reallocation; {@code number_of_in_flight_fetch} is 0 for the same reason.</li>
 *   <li>A closed index is green with no shards, as the supervisor reports it. Upstream gives an index closed
 *       with {@code index.verified_before_close} a routing table and counts its shards.</li>
 *   <li>An index whose primaries are still initializing is red, the supervisor's rule; upstream reports a
 *       newly created one yellow.</li>
 *   <li>Indices under {@code indices} are sorted by name, where upstream emits hash order.</li>
 *   <li>{@code level=awareness_attributes} is refused, not answered: its per-value counts and weights are not
 *       projected.</li>
 *   <li>A timeout, {@code -1} included, is capped at {@link #MAX_WAIT}.</li>
 * </ul>
 */
public final class ClusterHealthHandler implements OperationHandler {

    /** Upstream's default for {@code timeout}, and so for {@code cluster_manager_timeout}. */
    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    static final Duration MAX_WAIT = ClusterRequests.MAX_WAIT;

    private static final List<String> PRIORITIES = List.of("IMMEDIATE", "URGENT", "HIGH", "NORMAL", "LOW", "LANGUID");

    private final ClusterIdentity identity;
    private final Duration poll;

    public ClusterHealthHandler(ClusterIdentity identity) {
        this(identity, ClusterRequests.POLL);
    }

    ClusterHealthHandler(ClusterIdentity identity, Duration poll) {
        this.identity = identity;
        this.poll = poll;
    }

    @Override
    public String operationGroup() {
        return "cluster.health";
    }

    /** The parsed request: what to select, how to render, and the conditions to wait for. */
    private record Request(
        List<String> expressions,
        IndicesOptions options,
        boolean local,
        Duration timeout,
        Duration clusterManagerTimeout,
        String level,
        Status waitForStatus,
        boolean waitForNoRelocating,
        boolean waitForNoInitializing,
        String waitForActiveShards,
        String waitForNodes
    ) {
        /** Upstream's waitCount: one per condition, and one for naming indices at all. */
        int conditions() {
            int count = 0;
            if (waitForStatus != null) count++;
            if (waitForNoRelocating) count++;
            if (waitForNoInitializing) count++;
            if (waitForActiveShards != null) count++;
            if (waitForNodes.isEmpty() == false) count++;
            if (expressions.isEmpty() == false) count++;
            return count;
        }
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        Request parsed;
        try {
            Optional<RestResponse> rejected = validate(request);
            if (rejected.isPresent()) {
                return rejected.get();
            }
            parsed = parse(request);
        } catch (IllegalArgumentException e) {
            return ErrorResponses.illegalArgument(request, e.getMessage());
        }
        if ("awareness_attributes".equals(parsed.level())) {
            return ErrorResponses.illegalArgument(request,
                "level=awareness_attributes is not supported: per-value awareness counts are not projected");
        }

        long start = System.nanoTime();
        long deadline = start + Math.min(parsed.timeout().toNanos(), MAX_WAIT.toNanos());
        long clusterManagerDeadline = start + Math.min(parsed.clusterManagerTimeout().toNanos(), MAX_WAIT.toNanos());
        while (true) {
            StatusProjections projections = StatusProjections.read(context.store());
            boolean fresh = projections.fresh(context.nowEpochMillis());
            if (fresh == false && parsed.local() == false) {
                if (System.nanoTime() - clusterManagerDeadline >= 0) {
                    return ErrorResponses.clusterManagerNotDiscovered(request);
                }
                ClusterRequests.sleepUntil(clusterManagerDeadline, poll);
                continue;
            }

            Selection selection = select(parsed, projections, context);
            Totals totals = Totals.over(selection.indices());
            Status status = fresh ? totals.status() : Status.RED;
            if (selection.missing()) {
                status = Status.RED;
            }
            boolean satisfied = satisfied(parsed, projections, totals, status, selection.missing());
            if (satisfied || System.nanoTime() - deadline >= 0) {
                return render(request, parsed, context, projections, selection, totals, status, fresh, satisfied == false);
            }
            ClusterRequests.sleepUntil(deadline, poll);
        }
    }

    // ------------------------------------------------------------------ parameters

    /** The validation {@code ClusterHealthRequest.validate} runs, reported as it reports it. */
    private static Optional<RestResponse> validate(RestRequest request) {
        Optional<RestResponse> both = ClusterRequests.rejectBothManagerTimeouts(request);
        if (both.isPresent()) {
            return both;
        }
        if (request.param("wait_for_relocating_shards") != null) {
            return Optional.of(ErrorResponses.illegalArgument(request,
                "wait_for_relocating_shards has been removed, use wait_for_no_relocating_shards [true/false] instead"));
        }
        boolean awarenessLevel = "awareness_attributes".equals(request.param("level"));
        if (request.param("awareness_attribute") != null) {
            if (request.pathParam("index") != null) {
                return Optional.of(ErrorResponses.validationFailed(request,
                    "awareness_attribute is not a supported parameter with index health"));
            }
            if (awarenessLevel == false) {
                return Optional.of(ErrorResponses.validationFailed(request,
                    "level=awareness_attributes is required with awareness_attribute parameter"));
            }
        }
        if (request.paramAsBoolean("ensure_node_weighed_in", false) && request.paramAsBoolean("local", false) == false) {
            return Optional.of(ErrorResponses.validationFailed(request,
                "not a local request to ensure local node commissioned or weighed in"));
        }
        return Optional.empty();
    }

    private static Request parse(RestRequest request) {
        String index = request.pathParam("index");
        List<String> expressions = index == null || index.isEmpty()
            ? List.of()
            : Arrays.stream(index.split(",")).filter(name -> name.isEmpty() == false).toList();

        Duration timeout = ClusterRequests.timeValue(request, "timeout", DEFAULT_TIMEOUT);
        Duration clusterManagerTimeout =
            ClusterRequests.timeValue(request, ClusterRequests.clusterManagerTimeoutParam(request), timeout);

        // Upstream recognises three levels and silently reads anything else as cluster.
        String level = request.param("level", "cluster");
        if (List.of("indices", "shards", "awareness_attributes").contains(level) == false) {
            level = "cluster";
        }

        Status waitForStatus = null;
        String status = request.param("wait_for_status");
        if (status != null) {
            String upper = status.toUpperCase(Locale.ROOT);
            try {
                waitForStatus = Status.valueOf(upper);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("No enum constant org.opensearch.cluster.health.ClusterHealthStatus." + upper);
            }
        }

        String waitForNodes = request.param("wait_for_nodes", "");
        if (waitForNodes.isEmpty() == false) {
            nodesCondition(waitForNodes);
        }

        String events = request.param("wait_for_events");
        if (events != null && PRIORITIES.contains(events.toUpperCase(Locale.ROOT)) == false) {
            throw new IllegalArgumentException("No enum constant org.opensearch.common.Priority." + events.toUpperCase(Locale.ROOT));
        }

        return new Request(
            expressions,
            // LENIENT_EXPAND_OPEN_CLOSED_HIDDEN, upstream's default for this request.
            new IndicesOptions(true, true, true, true, true).withOverridesFrom(request),
            request.paramAsBoolean("local", false),
            timeout,
            clusterManagerTimeout,
            level,
            waitForStatus,
            request.paramAsBoolean("wait_for_no_relocating_shards", false),
            request.paramAsBoolean("wait_for_no_initializing_shards", false),
            activeShardsCondition(request.param("wait_for_active_shards")),
            waitForNodes
        );
    }

    /** {@code ActiveShardCount.parseString}: {@code all}, a count, or {@code default}; null when it waits for nothing. */
    private static String activeShardsCondition(String value) {
        if (value == null || value.equals("default")) {
            return null;
        }
        if (value.equals("all")) {
            return value;
        }
        int count;
        try {
            count = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("cannot parse ActiveShardCount[" + value + "]");
        }
        if (count < 0) {
            throw new IllegalArgumentException("shard count cannot be a negative value");
        }
        return count == 0 ? null : value;
    }

    /** A {@code wait_for_nodes} expression as an operator and a count, in upstream's order of prefixes. */
    private record NodesCondition(String operator, int count) {
        boolean holds(int nodes) {
            return switch (operator) {
                case ">=" -> nodes >= count;
                case "<=" -> nodes <= count;
                case ">" -> nodes > count;
                case "<" -> nodes < count;
                default -> nodes == count;
            };
        }
    }

    private static NodesCondition nodesCondition(String expression) {
        String[][] forms = {
            { ">=", ">=", "" }, { "ge(", ">=", ")" }, { "<=", "<=", "" }, { "le(", "<=", ")" },
            { ">", ">", "" }, { "gt(", ">", ")" }, { "<", "<", "" }, { "lt(", "<", ")" }
        };
        for (String[] form : forms) {
            if (expression.startsWith(form[0])) {
                String number = expression.substring(form[0].length(), expression.length() - form[2].length());
                return new NodesCondition(form[1], parseNodes(number));
            }
        }
        return new NodesCondition("==", parseNodes(expression));
    }

    private static int parseNodes(String number) {
        try {
            return Integer.parseInt(number);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("For input string: \"" + number + "\"");
        }
    }

    // ------------------------------------------------------------------ selection and conditions

    /** The indices a request reports on, and whether it named one that does not exist. */
    private record Selection(List<IndexHealth> indices, boolean missing) {}

    private static Selection select(Request parsed, StatusProjections projections, OperationContext context) {
        if (parsed.expressions().isEmpty()) {
            return new Selection(List.copyOf(projections.indices().values()), false);
        }
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(IndexAbstractions.from(context.store()));
        List<IndexHealth> selected = new ArrayList<>();
        if (resolver.resolve(parsed.expressions(), parsed.options()) instanceof IndexResolution.Resolved resolved) {
            resolved.indices().stream().sorted().distinct()
                .forEach(name -> projections.index(name).ifPresent(selected::add));
        }
        // Upstream re-resolves strictly to decide whether the named indices exist, and a missing one makes the
        // request red and its condition unmet, so it waits for the index to appear.
        boolean missing = (resolver.resolve(parsed.expressions(), new IndicesOptions(true, true, false, false, true))
            instanceof IndexResolution.Resolved) == false;
        return new Selection(selected, missing);
    }

    private static boolean satisfied(Request parsed, StatusProjections projections, Totals totals, Status status, boolean missing) {
        int met = 0;
        if (parsed.waitForStatus() != null && status.compareTo(parsed.waitForStatus()) <= 0) met++;
        if (parsed.waitForNoRelocating()) met++;
        if (parsed.waitForNoInitializing() && totals.initializingShards() == 0) met++;
        if (parsed.waitForActiveShards() != null) {
            boolean holds = parsed.waitForActiveShards().equals("all")
                ? totals.unassignedShards() == 0 && totals.initializingShards() == 0
                : Integer.parseInt(parsed.waitForActiveShards()) <= totals.activeShards();
            if (holds) met++;
        }
        if (parsed.waitForNodes().isEmpty() == false
            && nodesCondition(parsed.waitForNodes()).holds(projections.numberOfNodes())) met++;
        if (parsed.expressions().isEmpty() == false && missing == false) met++;
        return met == parsed.conditions();
    }

    // ------------------------------------------------------------------ rendering

    /** {@code ClusterHealthResponse.toXContent}'s fields, in its order. */
    private RestResponse render(
        RestRequest request,
        Request parsed,
        OperationContext context,
        StatusProjections projections,
        Selection selection,
        Totals totals,
        Status status,
        boolean fresh,
        boolean timedOut
    ) {
        HealthFigures.PendingTasks pending = HealthFigures.pendingTasks(context.store(), context.nowEpochMillis());

        ObjectNode body = Json.object();
        body.put("cluster_name", identity.clusterName());
        body.put("status", status.wire());
        body.put("timed_out", timedOut);
        body.put("number_of_nodes", projections.numberOfNodes());
        body.put("number_of_data_nodes", projections.numberOfDataNodes());
        body.put("discovered_master", fresh);
        body.put("discovered_cluster_manager", fresh);
        body.put("active_primary_shards", totals.activePrimaryShards());
        body.put("active_shards", totals.activeShards());
        body.put("relocating_shards", 0);
        body.put("initializing_shards", totals.initializingShards());
        body.put("unassigned_shards", totals.unassignedShards());
        body.put("delayed_unassigned_shards", 0);
        body.put("number_of_pending_tasks", pending.count());
        body.put("number_of_in_flight_fetch", 0);
        body.put("task_max_waiting_in_queue_millis", pending.longestWaitMillis());
        body.put("active_shards_percent_as_number", HealthFigures.activeShardsPercent(status, projections));

        if (parsed.level().equals("indices") || parsed.level().equals("shards")) {
            ObjectNode indices = body.putObject("indices");
            for (IndexHealth index : selection.indices()) {
                ObjectNode entry = indices.putObject(index.name());
                entry.put("status", index.status().wire());
                entry.put("number_of_shards", index.numberOfShards());
                entry.put("number_of_replicas", index.numberOfReplicas());
                entry.put("active_primary_shards", index.activePrimaryShards());
                entry.put("active_shards", index.activeShards());
                entry.put("relocating_shards", 0);
                entry.put("initializing_shards", index.initializingShards());
                entry.put("unassigned_shards", index.unassignedShards());
                if (parsed.level().equals("shards")) {
                    ObjectNode shards = entry.putObject("shards");
                    index.shards().forEach((id, copies) -> renderShard(shards.putObject(Integer.toString(id)), copies, index));
                }
            }
        }
        return Json.response(timedOut ? RestStatus.REQUEST_TIMEOUT : RestStatus.OK, body, request);
    }

    /**
     * One shard, judged by the supervisor's own per-shard rule ({@code StatusComputation.indexStatus}) over the
     * same placements, so that a shard and its index never disagree: red without a started primary, yellow with
     * fewer started copies than the index asks for, green otherwise.
     */
    private static void renderShard(ObjectNode shard, List<Copy> copies, IndexHealth index) {
        int started = (int) copies.stream().filter(Copy::started).count();
        boolean primaryActive = copies.stream().anyMatch(copy -> copy.started() && "PRIMARY".equals(copy.role()));
        int initializing = (int) copies.stream().filter(copy -> "INITIALIZING".equals(copy.state())).count();
        Status status = primaryActive == false ? Status.RED : started < index.copiesPerShard() ? Status.YELLOW : Status.GREEN;
        shard.put("status", status.wire());
        shard.put("primary_active", primaryActive);
        shard.put("active_shards", started);
        shard.put("relocating_shards", 0);
        shard.put("initializing_shards", initializing);
        shard.put("unassigned_shards", Math.max(0, index.copiesPerShard() - copies.size()));
    }
}
