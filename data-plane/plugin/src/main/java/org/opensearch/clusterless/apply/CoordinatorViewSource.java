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

/**
 * Reads the projections for the tenants this node coordinates.
 *
 * <p>An interface for the reason {@link IndexConfigSource} is one: the fabrication downstream is worth testing
 * against a stated view of the fleet rather than against a store that has to be arranged into one.
 *
 * <p>The store-backed implementation makes one prefix scan per coordinated tenant and then one read per index and
 * per node it finds. That is the cost of the design the contract chose, and it is the cheap side of the trade: the
 * alternative was inlining the table into every node's assignment, which stores it once per node and rewrites all
 * of them whenever one shard moves.
 */
public interface CoordinatorViewSource {

    CoordinatorView read(List<TenantId> tenants);

    /** For a node that coordinates nothing, which is most of them. */
    CoordinatorViewSource NONE = tenants -> CoordinatorView.nothing();

    /**
     * The real one.
     *
     * <p>Every skip carries a reason, and the reasons are the point. A coordinator with no route to a shard looks
     * exactly like a coordinator whose tenant has fewer shards than it thinks, and the difference is only visible
     * if this says so.
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
        public CoordinatorView read(List<TenantId> tenants) {
            if (tenants == null || tenants.isEmpty()) {
                return CoordinatorView.nothing();
            }

            List<CoordinatorView.RoutedIndex> indices = new ArrayList<>();
            List<String> deferred = new ArrayList<>();
            Set<NodeId> wanted = new LinkedHashSet<>();

            for (TenantId tenant : tenants) {
                readTenant(tenant, indices, deferred, wanted);
            }
            return new CoordinatorView(indices, peers(tenants, wanted, deferred), deferred);
        }

        private void readTenant(
            TenantId tenant,
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

                Optional<IndexConfig> config;
                try {
                    config = indexConfigs.read(tenant, placement.name());
                } catch (MalformedDocumentException e) {
                    deferred.add(tenant + "/" + placement.uuid() + ": " + e.getMessage());
                    return;
                }
                if (config.isEmpty()) {
                    // Ordinary and temporary: the supervisor projects an index whose document the control plane
                    // has since deleted, or the two reads straddled a write.
                    deferred.add(tenant + "/" + placement.uuid() + ": no index document at [" + placement.name() + "]");
                    return;
                }
                if (config.get().matches(placement.uuid()) == false) {
                    // The same check every other reader here makes. A name is reused; a uuid is not, so a
                    // document under this name may describe a different index and routing to it would send a
                    // tenant's query to another index's shards.
                    deferred.add(
                        tenant + "/" + placement.uuid() + ": the document at [" + placement.name() + "] is index ["
                            + config.get().uuid() + "]"
                    );
                    return;
                }

                indices.add(new CoordinatorView.RoutedIndex(tenant, config.get(), placement));
                placement.allCopies().forEach(copy -> wanted.add(copy.node()));
            });
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
        private Map<NodeId, NodeStatus> peers(List<TenantId> tenants, Set<NodeId> wanted, List<String> deferred) {
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
