/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.IndexStatus;
import org.opensearch.clusterless.contract.Keys;
import org.opensearch.clusterless.contract.MalformedDocumentException;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.NodeStatus;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.controlplane.store.KeyValueStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Reads where other nodes' copies are, for the two reasons a node needs to know.
 *
 * <p><b>Coordinating</b> is the obvious one: a node serving queries for a tenant needs every shard of every index
 * that tenant has. <b>Peer resolution</b> is the one that is easy to miss, and it applies to a node that answers no
 * queries at all — a node holding a copy needs the <em>other copies of its own shards</em>, because:
 *
 * <ul>
 *   <li>a primary publishes segment-replication checkpoints to the replicas in its own routing table, so a primary
 *       that does not know its replicas publishes to nobody — observed as
 *       {@code UnavailableShardsException: Not enough active copies ... (have 0, needed DEFAULT)} once a minute;
 *   <li>a search replica whose primary is missing from the table needs {@code index.blocks.search_only} to satisfy
 *       {@code RoutingNodes}, and that setting disables {@code IndexService.AsyncReplicationTask} — the timer that
 *       keeps it current. So a replica that cannot see its primary is refreshed by nothing.
 * </ul>
 *
 * <p>Both are freshness rather than reach, and both are cheaper than coordinating: the shards of one index this node
 * holds, rather than every shard of every index a tenant has.
 *
 * <h2>One scan per tenant, whichever reason applies</h2>
 *
 * <p>The reads are the same for both, so they are done once: one {@code scanPrefix} of
 * {@code tenant/<t>/status/indices/} per tenant this node has <em>anything</em> to do with, and then a decision per
 * index about which shards to take. That keeps the cost bounded by tenants rather than by indices, which matters —
 * a node holding a thousand indices would otherwise make a thousand reads per pass, which is the cost the contract
 * avoided by keeping this table out of the assignment in the first place.
 *
 * <p>An index this node holds copies of also needs no {@code tenant/<t>/indices/<name>} read: its configuration is
 * already in the desired state, having been read to build the copies.
 */
public interface CoordinatorViewSource {

    /**
     * @param desired what this node holds and which tenants it coordinates — both halves of the question, because
     *     the answer is one set of reads
     */
    CoordinatorView read(DesiredState desired);

    /** For a node with nothing to route and nothing to hold, which is a node with an empty assignment. */
    CoordinatorViewSource NONE = desired -> CoordinatorView.nothing();

    /**
     * The real one.
     *
     * <p>Every skip carries a reason, and the reasons are the point. A node with no route to a shard looks exactly
     * like a node whose tenant has fewer shards than it thinks, and the difference is only visible if this says so.
     */
    final class FromStore implements CoordinatorViewSource {

        private static final Logger logger = LogManager.getLogger(FromStore.class);

        private final KeyValueStore store;
        private final IndexConfigSource indexConfigs;

        public FromStore(KeyValueStore store, IndexConfigSource indexConfigs) {
            this.store = Objects.requireNonNull(store, "store");
            this.indexConfigs = Objects.requireNonNull(indexConfigs, "indexConfigs");
        }

        @Override
        public CoordinatorView read(DesiredState desired) {
            Set<TenantId> coordinated = Set.copyOf(desired.coordinatorTenants());
            Map<TenantId, Map<String, DesiredState.DesiredIndex>> held = heldByTenant(desired);

            Set<TenantId> tenants = new LinkedHashSet<>(coordinated);
            tenants.addAll(held.keySet());
            if (tenants.isEmpty()) {
                return CoordinatorView.nothing();
            }

            List<CoordinatorView.RoutedIndex> indices = new ArrayList<>();
            List<String> deferred = new ArrayList<>();
            Set<NodeId> wanted = new LinkedHashSet<>();

            for (TenantId tenant : tenants) {
                readTenant(
                    tenant,
                    coordinated.contains(tenant),
                    held.getOrDefault(tenant, Map.of()),
                    indices,
                    deferred,
                    wanted
                );
            }
            return new CoordinatorView(indices, peers(tenants, wanted, deferred), deferred);
        }

        /** What this node holds, by tenant and index uuid, so a projection can be matched to it in one lookup. */
        private static Map<TenantId, Map<String, DesiredState.DesiredIndex>> heldByTenant(DesiredState desired) {
            Map<TenantId, Map<String, DesiredState.DesiredIndex>> byTenant = new LinkedHashMap<>();
            desired.indices()
                .values()
                .forEach(index -> byTenant.computeIfAbsent(index.tenant(), tenant -> new TreeMap<>()).put(index.uuid(), index));
            return byTenant;
        }

        private void readTenant(
            TenantId tenant,
            boolean coordinated,
            Map<String, DesiredState.DesiredIndex> held,
            List<CoordinatorView.RoutedIndex> indices,
            List<String> deferred,
            Set<NodeId> wanted
        ) {
            Map<String, KeyValueStore.Versioned> scanned;
            try {
                scanned = store.scanPrefix(Keys.indexStatusPrefix(tenant));
            } catch (Exception e) {
                // One tenant's projections being unreadable costs that tenant's routing and nothing else. The
                // node keeps whatever it last applied, which is the level-triggered answer everywhere here.
                deferred.add(tenant + ": could not read the index projections (" + e + ")");
                logger.warn("clusterless: could not scan the index projections for [{}]", tenant, e);
                return;
            }

            scanned.forEach((key, value) -> {
                IndexStatus placement;
                try {
                    placement = IndexStatus.read(value.value());
                } catch (MalformedDocumentException e) {
                    deferred.add(tenant + ": " + e.getMessage());
                    return;
                }

                DesiredState.DesiredIndex mine = held.get(placement.uuid());
                if (coordinated == false && mine == null) {
                    // Neither coordinated nor held: no reason to know anything about it, which is the whole point of
                    // a data node's state being small.
                    return;
                }

                CoordinatorView.RoutedIndex routed = coordinated
                    ? wholeIndex(tenant, placement, deferred)
                    // Only the shards this node holds, and no index-document read: the configuration was already
                    // read to build the copies, so the desired state has it. The roles travel with it, because
                    // which *copies* of those shards are wanted depends on what this node holds of each --
                    // see RoutedIndex.needs.
                    : new CoordinatorView.RoutedIndex(tenant, mine.config(), placement, mine.shards().keySet(), mine.shards());
                if (routed == null) {
                    return;
                }

                indices.add(routed);
                // Only the peers whose copies will actually reach the routing table. Resolving an address this node
                // has no use for would put a node in DiscoveryNodes for nothing, which is the same surplus one step
                // earlier -- and the whole point of narrowing is that a primary stops learning its replicas' addresses.
                CoordinatorView.RoutedIndex forPeers = routed;
                forPeers.shards()
                    .forEach(
                        shard -> placement.copiesOf(shard)
                            .stream()
                            .filter(copy -> forPeers.needs(shard, copy.role()))
                            .forEach(copy -> wanted.add(copy.node()))
                    );
            });
        }

        /**
         * A coordinated tenant's index, which needs its document read for the settings and mappings a cluster state
         * cannot describe an index without.
         */
        private CoordinatorView.RoutedIndex wholeIndex(TenantId tenant, IndexStatus placement, List<String> deferred) {
            Optional<IndexConfig> config;
            try {
                config = indexConfigs.read(tenant, placement.name());
            } catch (MalformedDocumentException e) {
                deferred.add(tenant + "/" + placement.uuid() + ": " + e.getMessage());
                return null;
            }
            if (config.isEmpty()) {
                // Ordinary and temporary: the supervisor projects an index whose document the control plane has
                // since deleted, or the two reads straddled a write.
                deferred.add(tenant + "/" + placement.uuid() + ": no index document at [" + placement.name() + "]");
                return null;
            }
            if (config.get().matches(placement.uuid()) == false) {
                // The same check every other reader here makes. A name is reused; a uuid is not, so a document
                // under this name may describe a different index, and routing to it would send a tenant's query to
                // another index's shards.
                deferred.add(
                    tenant + "/" + placement.uuid() + ": the document at [" + placement.name() + "] is index ["
                        + config.get().uuid() + "]"
                );
                return null;
            }
            return CoordinatorView.RoutedIndex.wholeIndex(tenant, config.get(), placement);
        }

        /**
         * The nodes named by the copies that this node can actually route to, looked up once each.
         *
         * <p>A node id can appear in several tenants' projections, and the document is per tenant — so the first
         * tenant that answers for a node is the answer used. The fields a route needs (address, port, ephemeral
         * id) are properties of the node rather than of the tenant, so any tenant's copy of them will do.
         *
         * <p><b>Three reasons a peer is dropped here rather than later</b>, each with a line in {@code deferred}:
         * it is {@code LOST}, so the supervisor has given up on it and is placing its copies elsewhere; it carries
         * no ephemeral id or address, so no {@code DiscoveryNode} built from it could connect; or another node
         * already claims its endpoint.
         *
         * <p>That last one is not hypothetical and not a misconfiguration. A node whose data directory was wiped
         * comes back with a <em>new</em> node id on the same port, and until the supervisor reaps the old
         * projection two ids claim {@code 127.0.0.1:9301}. Left alone that reaches
         * {@code DiscoveryNodes.Builder}, which refuses the second one — and the one it refuses is whichever
         * happened to be added later, so the live node's shards could be dropped in favour of a dead node's. Which
         * makes choosing deliberately the point: reachable beats unreachable, live beats stale, and node id breaks
         * the tie so that two passes agree.
         */
        private Map<NodeId, NodeStatus> peers(Set<TenantId> tenants, Set<NodeId> wanted, List<String> deferred) {
            List<NodeStatus> found = new ArrayList<>();
            for (NodeId node : wanted) {
                Optional<NodeStatus> status = Optional.empty();
                for (TenantId tenant : tenants) {
                    status = readPeer(tenant, node, deferred);
                    if (status.isPresent()) {
                        break;
                    }
                }
                if (status.isEmpty()) {
                    deferred.add(node + ": no node projection, so there is no address to route to it with");
                    continue;
                }
                if (status.get().isReachable() == false) {
                    deferred.add(node + ": the supervisor has given up on this node, so its copies are not routed to");
                    continue;
                }
                if (status.get().isAddressable() == false) {
                    deferred.add(
                        node + ": its projection carries no ephemeral id or address, and a peer without one cannot"
                            + " be connected to -- DiscoveryNode equality is the ephemeral id alone"
                    );
                    continue;
                }
                found.add(status.get());
            }
            return oneNodePerEndpoint(found, deferred);
        }

        /**
         * At most one peer per address and port, chosen rather than stumbled upon.
         *
         * <p>Sorted so the choice is stable across passes — an unstable one would rebuild the routing table every
         * pass, which is the mistake {@link CoordinatorState} carries entries forward to avoid.
         */
        private static Map<NodeId, NodeStatus> oneNodePerEndpoint(List<NodeStatus> found, List<String> deferred) {
            List<NodeStatus> ordered = new ArrayList<>(found);
            ordered.sort(
                java.util.Comparator.comparing((NodeStatus peer) -> peer.liveness() == NodeStatus.Liveness.LIVE ? 0 : 1)
                    .thenComparing(peer -> peer.nodeId().value())
            );

            Map<String, NodeId> byEndpoint = new LinkedHashMap<>();
            Map<NodeId, NodeStatus> peers = new LinkedHashMap<>();
            for (NodeStatus peer : ordered) {
                String endpoint = peer.address() + ":" + peer.port();
                NodeId incumbent = byEndpoint.putIfAbsent(endpoint, peer.nodeId());
                if (incumbent != null) {
                    deferred.add(
                        peer.nodeId() + ": [" + endpoint + "] is already claimed by node [" + incumbent + "], so this"
                            + " one is not routed to -- usually a projection for a node that was replaced on the"
                            + " same port and has not been reaped yet"
                    );
                    continue;
                }
                peers.put(peer.nodeId(), peer);
            }
            return peers;
        }

        private Optional<NodeStatus> readPeer(TenantId tenant, NodeId node, List<String> deferred) {
            try {
                return store.get(Keys.nodeStatus(tenant, node)).map(stored -> NodeStatus.read(stored.value()));
            } catch (MalformedDocumentException e) {
                deferred.add(node + ": " + e.getMessage());
                return Optional.empty();
            } catch (Exception e) {
                logger.warn("clusterless: could not read the node projection for [{}]", node, e);
                return Optional.empty();
            }
        }
    }
}
