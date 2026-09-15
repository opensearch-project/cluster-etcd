package org.opensearch.supervisor.model.status;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Computes what the control plane needs from an observation: per-index health and placement, a cluster
 * summary, a tenant-scoped node list, and the awareness values that actually exist.
 *
 * <p>Pure, like the differ, and for the same reason — these documents are a public contract with another
 * service, so the rules that produce them should be checkable by writing down a situation and reading off the
 * answer.
 *
 * <p><b>Everything here is derived from observation, never from intent.</b> That is the invariant peer
 * resolution depends on: an index status names a node as primary only after that node reported itself
 * primary, so the projection lags reality and never leads it. A docrep replica reading it can therefore point
 * at a node that <em>was</em> primary but never at one that has merely been asked to become one.
 */
public final class StatusComputation {

    /** Everything one tenant's projection consists of. */
    public record TenantStatus(
        Map<String, IndexStatus> indices,
        ClusterStatus cluster,
        Map<NodeId, NodeStatus> nodes,
        AwarenessStatus awareness
    ) {}

    private StatusComputation() {}

    public static TenantStatus compute(TenantId tenant, Observation observation) {
        Observation.DesiredState desired = observation.tenants().get(tenant);
        if (desired == null) {
            return new TenantStatus(Map.of(), null, Map.of(), null);
        }

        Map<String, IndexStatus> indices = new LinkedHashMap<>();
        Map<String, Integer> byStatus = new TreeMap<>();
        int activeShards = 0;
        int activePrimaries = 0;
        int unassigned = 0;
        int initializing = 0;
        HealthStatus worst = HealthStatus.GREEN;

        for (IndexConfig index : desired.indicesByUuid().values()) {
            IndexStatus status = indexStatus(index, tenant, observation);
            indices.put(index.uuid(), status);

            byStatus.merge(status.status().name().toLowerCase(java.util.Locale.ROOT), 1, Integer::sum);
            activeShards += status.activeShards();
            activePrimaries += status.activePrimaryShards();
            unassigned += status.unassignedShards();
            initializing += status.initializingShards();
            worst = worse(worst, status.status());
        }

        Map<NodeId, NodeStatus> nodes = nodeStatuses(tenant, observation);
        long dataNodes = nodes.values()
            .stream()
            .filter(node -> node.roles().contains(org.opensearch.supervisor.model.node.NodeRole.DATA))
            .count();

        ClusterStatus cluster = new ClusterStatus(
            worst, nodes.size(), (int) dataNodes,
            activeShards, activePrimaries, unassigned, initializing,
            byStatus, observation.observedAt().toEpochMilli(), observation.pass()
        );

        return new TenantStatus(indices, cluster, nodes, awareness(nodes, observation));
    }

    // ---------------------------------------------------------------- per index

    /**
     * OpenSearch's health definition, and the two places it is easy to get subtly wrong.
     *
     * <p><b>A primary that is initializing is not active.</b> Red means "some shard cannot serve reads or
     * writes", and a recovering primary cannot — so an index whose primaries all exist but are still coming up
     * is red, not yellow. This is exactly the distinction the role/state split was made for, and with the two
     * collapsed into one enum it was not expressible.
     *
     * <p><b>A closed index is green with no shards</b>, matching OpenSearch. A closed index is not unhealthy;
     * it is simply not serving, and reporting it red would make every deliberate close look like an incident.
     */
    private static IndexStatus indexStatus(IndexConfig index, TenantId tenant, Observation observation) {
        if (index.open() == false) {
            return new IndexStatus(
                index.name(), index.uuid(), index.configRevision(), HealthStatus.GREEN, index.replication(),
                index.numberOfShards(), index.numberOfReplicas(), index.numberOfSearchReplicas(), Map.of(),
                observation.observedAt().toEpochMilli(), observation.pass()
            );
        }

        Map<Integer, List<IndexStatus.ShardPlacement>> shards = new TreeMap<>();
        for (int shard = 0; shard < index.numberOfShards(); shard++) {
            shards.put(shard, placementsOf(tenant, index.uuid(), shard, observation));
        }

        int activeShards = 0;
        int activePrimaries = 0;
        int initializing = 0;
        boolean anyPrimaryInactive = false;

        for (List<IndexStatus.ShardPlacement> placements : shards.values()) {
            boolean primaryActive = false;
            for (IndexStatus.ShardPlacement placement : placements) {
                if (placement.state() == ShardState.STARTED) {
                    activeShards++;
                    if (placement.role() == ShardRole.PRIMARY) {
                        primaryActive = true;
                        activePrimaries++;
                    }
                } else if (placement.state() == ShardState.INITIALIZING) {
                    initializing++;
                }
            }
            if (primaryActive == false) {
                anyPrimaryInactive = true;
            }
        }

        int desiredCopies = index.numberOfShards() * index.copiesPerShard();
        int presentCopies = shards.values().stream().mapToInt(List::size).sum();
        int unassigned = Math.max(0, desiredCopies - presentCopies);

        HealthStatus status;
        if (anyPrimaryInactive) {
            status = HealthStatus.RED;
        } else if (activeShards < desiredCopies) {
            status = HealthStatus.YELLOW;
        } else {
            status = HealthStatus.GREEN;
        }

        return new IndexStatus(
            index.name(), index.uuid(), index.configRevision(), status, index.replication(),
            index.numberOfShards(), index.numberOfReplicas(), index.numberOfSearchReplicas(), shards,
            observation.observedAt().toEpochMilli(), observation.pass()
        );
    }

    /**
     * Every observed copy of a shard, <b>including ones that are not yet serving</b>.
     *
     * <p>Required rather than incidental: a docrep replica reads this document to find the primary it must
     * replicate from, and a projection filtered to serving copies would deadlock — the replica could never see
     * the primary it is waiting for. See {@code NODE_CONTRACT.md}.
     */
    private static List<IndexStatus.ShardPlacement> placementsOf(
        TenantId tenant,
        String indexUuid,
        int shard,
        Observation observation
    ) {
        List<IndexStatus.ShardPlacement> placements = new ArrayList<>();
        observation.nodes().forEach((nodeId, node) -> {
            if (node.copiesCount() == false || node.inventory() == null) {
                return;
            }
            ShardInventory.TenantShards tenantShards = node.inventory().tenants().get(tenant);
            if (tenantShards == null) {
                return;
            }
            ShardInventory.IndexShards held = tenantShards.indices().get(indexUuid);
            if (held == null) {
                return;
            }
            ShardInventory.ShardCopy copy = held.shards().get(shard);
            if (copy != null) {
                placements.add(new IndexStatus.ShardPlacement(nodeId, copy.role(), copy.state()));
            }
        });
        placements.sort(java.util.Comparator.comparing(placement -> placement.nodeId().value()));
        return placements;
    }

    private static HealthStatus worse(HealthStatus a, HealthStatus b) {
        return a.ordinal() >= b.ordinal() ? a : b;
    }

    // ---------------------------------------------------------------- per node

    /**
     * The nodes this tenant can see, which is <b>only the nodes holding or assigned something of its own</b>.
     *
     * <p>A decision, and the alternative is worse in both directions. Listing every node in the pool would
     * tell a tenant in a shared pool how many machines its neighbours are using, and would make its
     * {@code number_of_nodes} change when somebody else's data moved. Listing only its own nodes means the
     * figure is smaller than an operator looking at the pool would expect — which is the correct answer to a
     * tenant-scoped question, and is why this document lives under the tenant prefix at all.
     *
     * <p>For a dedicated pool the two definitions coincide, so this only differs where it has to.
     */
    private static Map<NodeId, NodeStatus> nodeStatuses(TenantId tenant, Observation observation) {
        Map<NodeId, NodeStatus> nodes = new LinkedHashMap<>();
        observation.nodes().forEach((nodeId, node) -> {
            if (node.registration() == null || node.copiesCount() == false) {
                return;
            }
            if (servesTenant(node, tenant) == false) {
                return;
            }
            nodes.put(nodeId, new NodeStatus(
                nodeId,
                // From the registration rather than the heartbeat: both carry it, and the registration is the
                // document whose change already triggers this projection being rewritten.
                node.registration().ephemeralId(),
                node.registration().name(),
                node.registration().roles(),
                node.registration().address(),
                node.registration().port(),
                node.registration().version(),
                node.registration().attributes(),
                node.liveness(),
                observation.observedAt().toEpochMilli()
            ));
        });
        return nodes;
    }

    private static boolean servesTenant(Observation.NodeObservation node, TenantId tenant) {
        if (node.assignment() != null && node.assignment().tenants().containsKey(tenant)) {
            return true;
        }
        return node.inventory() != null && node.inventory().tenants().containsKey(tenant);
    }

    // ---------------------------------------------------------------- awareness

    private static AwarenessStatus awareness(Map<NodeId, NodeStatus> nodes, Observation observation) {
        Map<String, Set<String>> values = new TreeMap<>();
        for (NodeStatus node : nodes.values()) {
            if (node.isReachable() == false) {
                continue;
            }
            node.attributes().forEach((key, value) ->
                values.computeIfAbsent(key, ignored -> new TreeSet<>()).add(value));
        }

        Map<String, List<String>> attributes = new LinkedHashMap<>();
        values.forEach((key, set) -> attributes.put(key, List.copyOf(new LinkedHashSet<>(set))));
        return new AwarenessStatus(attributes, observation.observedAt().toEpochMilli());
    }
}
