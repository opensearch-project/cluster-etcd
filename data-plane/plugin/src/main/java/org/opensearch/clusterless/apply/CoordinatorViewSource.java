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
         * The nodes named by the copies, looked up once each.
         *
         * <p>A node id can appear in several tenants' projections, and the document is per tenant — so the first
         * tenant that answers for a node is the answer used. The fields a route needs (address, port, ephemeral
         * id) are properties of the node rather than of the tenant, so any tenant's copy of them will do.
         */
        private Map<NodeId, NodeStatus> peers(List<TenantId> tenants, Set<NodeId> wanted, List<String> deferred) {
            Map<NodeId, NodeStatus> peers = new LinkedHashMap<>();
            for (NodeId node : wanted) {
                for (TenantId tenant : tenants) {
                    Optional<NodeStatus> status = readPeer(tenant, node, deferred);
                    if (status.isPresent()) {
                        peers.put(node, status.get());
                        break;
                    }
                }
                if (peers.containsKey(node) == false) {
                    deferred.add(node + ": no node projection, so there is no address to route to it with");
                }
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
