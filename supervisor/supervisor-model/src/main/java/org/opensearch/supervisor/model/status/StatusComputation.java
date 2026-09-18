package org.opensearch.supervisor.model.status;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;

import java.util.ArrayList;
import java.util.Comparator;
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
        return compute(tenant, observation, List.of());
    }

    /**
     * @param awarenessAttributes the pool's declared awareness attributes, which are the <em>only</em> valid
     *     weighted-routing dimensions and therefore the only ones {@code status/awareness} should offer. An empty
     *     list means the pool declares none, and the honest answer then is an empty document rather than every
     *     attribute a node happens to carry.
     */
    public static TenantStatus compute(TenantId tenant, Observation observation, List<String> awarenessAttributes) {
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

        return new TenantStatus(indices, cluster, nodes, awareness(nodes, observation, awarenessAttributes));
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
                index.numberOfShards(), index.numberOfReplicas(), index.numberOfSearchReplicas(),
                index.ingestionPaused(), Map.of(),
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
            index.numberOfShards(), index.numberOfReplicas(), index.numberOfSearchReplicas(),
            index.ingestionPaused(), shards,
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
        Set<NodeId> replaced = supersededOnTheirEndpoint(observation);
        observation.nodes().forEach((nodeId, node) -> {
            if (node.registration() == null || node.copiesCount() == false) {
                return;
            }
            if (servesTenant(node, tenant) == false) {
                return;
            }
            if (replaced.contains(nodeId)) {
                // A predecessor on an endpoint another node now answers. Publishing its address would hand a
                // reader two nodes claiming one socket, and the reader cannot tell which is real -- see
                // supersededOnTheirEndpoint.
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

    /**
     * Nodes whose {@code address:port} another node now answers on.
     *
     * <p>Nothing reaps a departed node's documents, so a data plane restarted on the same port leaves its
     * predecessor's registration in the store — and a fresh supervisor process grants every such corpse a full
     * staleness window before giving up on it, because silence is measured from when <em>this</em> process started
     * watching. During that window both nodes are projected, both carry the same address and port, and a reader is
     * handed two nodes claiming one socket with no way to tell which is real. A real coordinator did exactly that
     * and declined to route to one of them, correctly and confusingly.
     *
     * <p>One process owns a port, so two nodes on one endpoint means one of them is history. <b>Only the
     * supervisor can resolve it</b>: each node sees its own registration and no other, so it cannot know it has a
     * predecessor, while the supervisor sees every registration at once. Same shape as the repository-mismatch
     * check — a fact assembled from the whole pool rather than from any one node.
     *
     * <p>Freshest wins: liveness first, then the more recent heartbeat, then the node id so that two supervisors
     * reading one observation publish the same thing. A node with no heartbeat at all ranks last, since it has
     * never demonstrated it is there.
     *
     * <p>This does not fix the accumulation, only its most damaging symptom. Reaping is the real answer and it is
     * a harder change than it looks — see {@code DESIGN.md} §20.
     */
    private static Set<NodeId> supersededOnTheirEndpoint(Observation observation) {
        Map<String, List<Map.Entry<NodeId, Observation.NodeObservation>>> byEndpoint = new LinkedHashMap<>();
        observation.nodes().forEach((nodeId, node) -> {
            if (node.registration() == null || node.copiesCount() == false) {
                return;
            }
            String endpoint = node.registration().address() + ":" + node.registration().port();
            byEndpoint.computeIfAbsent(endpoint, key -> new ArrayList<>())
                .add(Map.entry(nodeId, node));
        });

        Set<NodeId> superseded = new LinkedHashSet<>();
        byEndpoint.forEach((endpoint, sharing) -> {
            if (sharing.size() < 2) {
                return;
            }
            NodeId winner = sharing.stream()
                .max(Comparator
                    .comparingInt((Map.Entry<NodeId, Observation.NodeObservation> entry)
                        -> freshness(entry.getValue().liveness()))
                    .thenComparingLong(entry -> heartbeatAt(entry.getValue()))
                    .thenComparing(entry -> entry.getKey().value()))
                .orElseThrow()
                .getKey();
            sharing.forEach(entry -> {
                if (entry.getKey().equals(winner) == false) {
                    superseded.add(entry.getKey());
                }
            });
        });
        return superseded;
    }

    private static int freshness(Liveness liveness) {
        return switch (liveness) {
            case LIVE -> 3;
            case STALE -> 2;
            case UNKNOWN -> 1;
            case LOST -> 0;
        };
    }

    private static long heartbeatAt(Observation.NodeObservation node) {
        return node.heartbeat() == null ? Long.MIN_VALUE : node.heartbeat().timestamp();
    }

    private static boolean servesTenant(Observation.NodeObservation node, TenantId tenant) {
        if (node.assignment() != null && node.assignment().tenants().containsKey(tenant)) {
            return true;
        }
        return node.inventory() != null && node.inventory().tenants().containsKey(tenant);
    }

    // ---------------------------------------------------------------- awareness

    /**
     * The values of the pool's awareness attributes, across the nodes serving this tenant.
     *
     * <p><b>Only the declared attributes</b>, which is a narrowing and a correction rather than a tidy-up. This
     * document exists to validate weighted routing, and weighted routing in OpenSearch weights by an
     * <em>awareness attribute</em> — so offering any other attribute invites a request that is accepted and does
     * nothing, because the allocator spreads copies across the declared attributes and no others.
     *
     * <p>It published every attribute a node carried, which meant {@code remote_store.segment.repository} appeared
     * beside {@code zone} as though a client could weight traffic by which bucket the segments are in. In a shared
     * pool it also told one tenant the repository name the pool writes to, which is a neighbour's business.
     *
     * <p>A pool declaring no awareness attributes therefore gets an <b>empty</b> document, not every attribute.
     * That is the honest answer to "what may I weight by": nothing, until an operator says what the failure
     * domains are. Discovering which values exist before declaring the attribute is a different question, and
     * {@code status/nodes/<id>} answers it — it carries each node's attributes in full.
     */
    private static AwarenessStatus awareness(
        Map<NodeId, NodeStatus> nodes,
        Observation observation,
        List<String> awarenessAttributes
    ) {
        Map<String, Set<String>> values = new TreeMap<>();
        for (NodeStatus node : nodes.values()) {
            if (node.isReachable() == false) {
                continue;
            }
            node.attributes().forEach((key, value) -> {
                if (awarenessAttributes.contains(key)) {
                    values.computeIfAbsent(key, ignored -> new TreeSet<>()).add(value);
                }
            });
        }

        Map<String, List<String>> attributes = new LinkedHashMap<>();
        values.forEach((key, set) -> attributes.put(key, List.copyOf(new LinkedHashSet<>(set))));
        return new AwarenessStatus(attributes, observation.observedAt().toEpochMilli());
    }
}
