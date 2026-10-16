/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.handler.cluster.HealthProjections.IndexHealth;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
import org.opensearch.controlplane.handler.core.ClusterUuid;
import org.opensearch.controlplane.handler.template.Templates;
import org.opensearch.controlplane.resolver.IndexAbstractions;
import org.opensearch.controlplane.resolver.IndexNameExpressionResolver;
import org.opensearch.controlplane.resolver.IndexResolution;
import org.opensearch.controlplane.resolver.IndicesOptions;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * {@code GET /_cluster/state[/{metric}[/{index}]]}, composed from the documents this service and the
 * supervisor keep for the tenant, with no cluster state behind it.
 *
 * <p>Handed here by the data plane, which refuses it for a tenant (its {@code FINDINGS_FROM_THE_DATA_PLANE.md},
 * item 1, in this component). Where each section comes from:
 *
 * <ul>
 *   <li><b>{@code metadata}</b> — this service's own index documents, aliases and legacy templates. Needs no
 *       supervisor, so {@code GET /_cluster/state/metadata/my-index} answers whether or not one is running.</li>
 *   <li><b>{@code nodes}, {@code routing_table}, {@code routing_nodes}</b> — the supervisor's
 *       {@code status/nodes} and {@code status/indices} projections. The nodes are the tenant's own, by the
 *       supervisor's design: a tenant in a shared pool does not see its neighbours' machines.</li>
 *   <li><b>{@code blocks}</b> — derived from index state and {@code index.blocks.*} settings.</li>
 *   <li><b>{@code version}</b> — see below.</li>
 * </ul>
 *
 * <h2>{@code version} and {@code state_uuid}</h2>
 *
 * <p>There is no cluster state to number, so both are derived from the store revisions of the documents the
 * response is composed from. {@code state_uuid} is a digest of every one of those (key, revision) pairs, so it
 * changes whenever any is written <em>or removed</em>. {@code version} is the highest revision among them: it
 * rises with writes, but a removal of the newest document can lower it, and across backends it is a count
 * (etcd, ZooKeeper) or a clock reading (per-key revisions), so it is a change signal and not a clock. Neither
 * moves when only {@code status/cluster} does, which the supervisor rewrites every heartbeat.
 * {@code wait_for_metadata_version} waits for the highest revision among the <em>metadata</em> documents to
 * reach the number given, with the same caveat.
 *
 * <h2>Without a supervisor</h2>
 *
 * <p>As for {@code cluster.health}: a request for {@code nodes}, {@code routing_table} or {@code routing_nodes}
 * when {@code status/cluster} is absent or stale waits {@code cluster_manager_timeout} (30 s) and then fails
 * with {@code 503 cluster_manager_not_discovered_exception}; with {@code local=true} it answers the last
 * documents. Whenever it is answered and {@code blocks} is included, the no-cluster-manager block (id 2) is
 * there.
 *
 * <h2>Considered divergences</h2>
 *
 * <ul>
 *   <li>{@code master_node} and {@code cluster_manager_node} are null: there is no elected cluster manager.</li>
 *   <li>{@code cluster_coordination} is empty and at term 0, for the same reason.</li>
 *   <li>Metadata customs are not rendered: {@code index-graveyard}, composable and component templates, data
 *       streams, stored scripts, pipelines and repositories. Each has its own read operation.</li>
 *   <li>Per index, the version counters, {@code routing_num_shards}, {@code primary_terms} and
 *       {@code split_shards_metadata} are absent, as are {@code unassigned_info} and {@code recovery_source}
 *       on a shard. See {@link ClusterStateSections}.</li>
 *   <li>A wait for a metadata version that times out answers {@code 200} with
 *       {@code wait_for_timed_out: true} and the cluster name and uuid only. Upstream builds that response
 *       with no state and, as far as the source shows, fails serialising it.</li>
 *   <li>Keyed collections come out sorted, where upstream emits hash order.</li>
 *   <li>Only the metrics the spec lists: {@code customs} is not one, and an unrecognised metric is ignored, as
 *       upstream ignores it.</li>
 * </ul>
 */
public final class ClusterStateHandler implements OperationHandler {

    private static final Set<String> METRICS = Set.of(
        "version", "master_node", "cluster_manager_node", "blocks", "nodes", "metadata", "routing_table", "routing_nodes");

    private static final Duration DEFAULT_CLUSTER_MANAGER_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_WAIT_FOR_TIMEOUT = Duration.ofMinutes(1);

    /** {@code ClusterStateRequest}'s index options: open indices, and a missing name is simply skipped. */
    private static final IndicesOptions LENIENT_EXPAND_OPEN = new IndicesOptions(true, false, false, true, true);

    private final ClusterIdentity identity;
    private final Duration poll;

    public ClusterStateHandler(ClusterIdentity identity) {
        this(identity, ClusterRequests.POLL);
    }

    ClusterStateHandler(ClusterIdentity identity, Duration poll) {
        this.identity = identity;
        this.poll = poll;
    }

    @Override
    public String operationGroup() {
        return "cluster.state";
    }

    @Override
    public RestResponse handle(RestRequest request, OperationContext context) {
        Optional<RestResponse> rejected = ClusterRequests.rejectBothManagerTimeouts(request);
        if (rejected.isPresent()) {
            return rejected.get();
        }
        Duration clusterManagerTimeout;
        Duration waitForTimeout;
        Long waitForMetadataVersion;
        try {
            clusterManagerTimeout = ClusterRequests.timeValue(
                request, ClusterRequests.clusterManagerTimeoutParam(request), DEFAULT_CLUSTER_MANAGER_TIMEOUT);
            waitForTimeout = ClusterRequests.timeValue(request, "wait_for_timeout", DEFAULT_WAIT_FOR_TIMEOUT);
            waitForMetadataVersion = waitForMetadataVersion(request);
        } catch (IllegalArgumentException e) {
            return ErrorResponses.illegalArgument(request, e.getMessage());
        }

        Set<String> metrics = metrics(request.pathParam("metric"));
        boolean needsSupervisor =
            metrics.contains("nodes") || metrics.contains("routing_table") || metrics.contains("routing_nodes");
        boolean local = request.paramAsBoolean("local", false);

        long start = System.nanoTime();
        long clusterManagerDeadline = start + Math.min(clusterManagerTimeout.toNanos(), ClusterRequests.MAX_WAIT.toNanos());
        long waitDeadline = start + Math.min(waitForTimeout.toNanos(), ClusterRequests.MAX_WAIT.toNanos());
        while (true) {
            Snapshot snapshot = Snapshot.read(context.store());
            boolean fresh = snapshot.projections().fresh(context.nowEpochMillis());

            if (needsSupervisor && fresh == false && local == false) {
                if (System.nanoTime() - clusterManagerDeadline >= 0) {
                    return ErrorResponses.clusterManagerNotDiscovered(request);
                }
                ClusterRequests.sleepUntil(clusterManagerDeadline, poll);
                continue;
            }
            if (waitForMetadataVersion != null && snapshot.metadataVersion() < waitForMetadataVersion) {
                if (System.nanoTime() - waitDeadline >= 0) {
                    return timedOut(request, snapshot);
                }
                ClusterRequests.sleepUntil(waitDeadline, poll);
                continue;
            }
            return render(request, context, metrics, snapshot, fresh, waitForMetadataVersion != null);
        }
    }

    // ------------------------------------------------------------------ parameters

    /** {@code CS.parseMetrics}: {@code _all} anywhere means all; names it does not know are ignored. */
    private static Set<String> metrics(String parameter) {
        if (parameter == null || parameter.isEmpty()) {
            return METRICS;
        }
        Set<String> requested = new LinkedHashSet<>();
        for (String name : parameter.split(",")) {
            String metric = name.trim();
            if (metric.equals("_all")) {
                return METRICS;
            }
            if (METRICS.contains(metric)) {
                requested.add(metric);
            }
        }
        return requested;
    }

    private static Long waitForMetadataVersion(RestRequest request) {
        String value = request.param("wait_for_metadata_version");
        if (value == null) {
            return null;
        }
        long version;
        try {
            version = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse long parameter [wait_for_metadata_version] with value [" + value + "]");
        }
        if (version < 1) {
            throw new IllegalArgumentException(
                "provided waitForMetadataVersion should be >= 1, but instead is [" + version + "]");
        }
        return version;
    }

    // ------------------------------------------------------------------ the documents

    /**
     * Everything a response is composed from, read in one pass, with the revisions that give it a version.
     * Not a consistent snapshot of the store: a document written during the read may or may not be in it,
     * which is the same freedom a scan has.
     */
    private record Snapshot(
        HealthProjections projections,
        NavigableMap<String, JsonNode> nodes,
        NavigableMap<String, JsonNode> legacyTemplates,
        Map<String, List<String>> aliasesByIndex,
        String clusterUuid,
        long metadataVersion,
        long version,
        String stateUuid
    ) {

        static Snapshot read(KeyValueStore store) {
            HealthProjections projections = HealthProjections.read(store);
            NavigableMap<String, Long> metadataRevisions = new TreeMap<>();
            NavigableMap<String, Long> routingRevisions = new TreeMap<>();
            projections.revisions().forEach((key, revision) ->
                (key.startsWith(IndexAbstractions.INDEX_KEY_PREFIX) ? metadataRevisions : routingRevisions).put(key, revision));

            NavigableMap<String, JsonNode> nodes = new TreeMap<>();
            store.scanPrefix("status/nodes/").forEach((key, versioned) -> {
                JsonNode node = parse(versioned.value());
                nodes.put(node.path("node_id").asText(key.substring("status/nodes/".length())), node);
                routingRevisions.put(key, versioned.revision());
            });

            NavigableMap<String, JsonNode> templates = new TreeMap<>();
            store.scanPrefix(Templates.LEGACY_KEY_PREFIX).forEach((key, versioned) -> {
                templates.put(key.substring(Templates.LEGACY_KEY_PREFIX.length()), parse(versioned.value()));
                metadataRevisions.put(key, versioned.revision());
            });

            Map<String, List<String>> aliasesByIndex = new TreeMap<>();
            store.scanPrefix(IndexAbstractions.ALIAS_KEY_PREFIX).forEach((key, versioned) -> {
                String alias = key.substring(IndexAbstractions.ALIAS_KEY_PREFIX.length());
                parse(versioned.value()).path("indices").fieldNames()
                    .forEachRemaining(index -> aliasesByIndex.computeIfAbsent(index, name -> new ArrayList<>()).add(alias));
                metadataRevisions.put(key, versioned.revision());
            });
            aliasesByIndex.values().forEach(names -> names.sort(String::compareTo));

            String clusterUuid = ClusterUuid.resolve(store);
            long metadataVersion = metadataRevisions.values().stream().mapToLong(Long::longValue).max().orElse(0);
            long routingVersion = routingRevisions.values().stream().mapToLong(Long::longValue).max().orElse(0);

            NavigableMap<String, Long> all = new TreeMap<>(metadataRevisions);
            all.putAll(routingRevisions);
            return new Snapshot(projections, nodes, templates, aliasesByIndex, clusterUuid, metadataVersion,
                Math.max(metadataVersion, routingVersion), digest(clusterUuid, all));
        }

        /** 128 bits of SHA-256 over the cluster uuid and every (key, revision), in OpenSearch's uuid format. */
        private static String digest(String clusterUuid, NavigableMap<String, Long> revisions) {
            try {
                MessageDigest sha = MessageDigest.getInstance("SHA-256");
                sha.update(clusterUuid.getBytes(StandardCharsets.UTF_8));
                revisions.forEach((key, revision) ->
                    sha.update(("\n" + key + "=" + revision).getBytes(StandardCharsets.UTF_8)));
                return Base64.getUrlEncoder().withoutPadding().encodeToString(Arrays.copyOf(sha.digest(), 16));
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 is required of every Java platform", e);
            }
        }

        private static JsonNode parse(byte[] value) {
            try {
                return Json.parse(value);
            } catch (IOException e) {
                throw new IllegalStateException("corrupt document", e);
            }
        }
    }

    // ------------------------------------------------------------------ rendering

    /** The index expression's matches: lenient, so a missing name is skipped and no match is an empty set. */
    private static Collection<IndexHealth> selectedIndices(RestRequest request, OperationContext context, Snapshot snapshot) {
        String expression = request.pathParam("index");
        if (expression == null || expression.isEmpty() || expression.equals("_all")) {
            return snapshot.projections().indices().values();
        }
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(IndexAbstractions.from(context.store()));
        List<IndexHealth> selected = new ArrayList<>();
        List<String> expressions = Arrays.stream(expression.split(",")).filter(name -> name.isEmpty() == false).toList();
        if (resolver.resolve(expressions, LENIENT_EXPAND_OPEN.withOverridesFrom(request)) instanceof IndexResolution.Resolved resolved) {
            resolved.indices().stream().sorted().distinct()
                .forEach(name -> snapshot.projections().index(name).ifPresent(selected::add));
        }
        return selected;
    }

    private RestResponse render(
        RestRequest request,
        OperationContext context,
        Set<String> metrics,
        Snapshot snapshot,
        boolean fresh,
        boolean waited
    ) {
        String expression = request.pathParam("index");
        boolean filtered = expression != null && expression.isEmpty() == false && expression.equals("_all") == false;
        Collection<IndexHealth> selected = selectedIndices(request, context, snapshot);
        boolean flatSettings = request.paramAsBoolean("flat_settings", false);

        ObjectNode body = Json.object();
        if (waited) {
            body.put("wait_for_timed_out", false);
        }
        body.put("cluster_name", identity.clusterName());
        body.put("cluster_uuid", snapshot.clusterUuid());
        if (metrics.contains("version")) {
            body.put("version", snapshot.version());
            body.put("state_uuid", snapshot.stateUuid());
        }
        // No elected cluster manager exists, so the field is present and null, as on a cluster without one.
        if (metrics.contains("master_node")) {
            body.putNull("master_node");
        }
        if (metrics.contains("cluster_manager_node")) {
            body.putNull("cluster_manager_node");
        }
        if (metrics.contains("blocks")) {
            // Blocks are not filtered by the index expression: upstream copies them whole.
            body.set("blocks", ClusterStateSections.blocks(snapshot.projections().indices().values(), fresh == false));
        }
        if (metrics.contains("nodes")) {
            body.set("nodes", ClusterStateSections.nodes(snapshot.nodes()));
        }
        if (metrics.contains("metadata")) {
            body.set("metadata", ClusterStateSections.metadata(
                snapshot.clusterUuid(), filtered, snapshot.legacyTemplates(), selected, snapshot.aliasesByIndex(), flatSettings));
        }
        if (metrics.contains("routing_table")) {
            body.set("routing_table", ClusterStateSections.routingTable(selected));
        }
        if (metrics.contains("routing_nodes")) {
            // Every data node is listed, even one holding nothing, when the nodes themselves are in the response.
            List<String> dataNodes = metrics.contains("nodes")
                ? snapshot.nodes().entrySet().stream()
                    .filter(node -> ClusterStateSections.hasRole(node.getValue(), "data"))
                    .map(Map.Entry::getKey)
                    .toList()
                : List.of();
            body.set("routing_nodes", ClusterStateSections.routingNodes(selected, dataNodes));
        }
        return Json.response(RestStatus.OK, body, request);
    }

    /** What a wait that ran out answers: the intended {@code wait_for_timed_out}, with no state to describe. */
    private RestResponse timedOut(RestRequest request, Snapshot snapshot) {
        ObjectNode body = Json.object();
        body.put("wait_for_timed_out", true);
        body.put("cluster_name", identity.clusterName());
        body.put("cluster_uuid", snapshot.clusterUuid());
        return Json.response(RestStatus.OK, body, request);
    }
}
