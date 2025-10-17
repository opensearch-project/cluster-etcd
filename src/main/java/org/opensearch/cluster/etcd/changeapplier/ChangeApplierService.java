/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.indices.IndicesService;

public class ChangeApplierService implements NodeStateApplier {
    private final Logger logger = LogManager.getLogger(getClass());
    private final ClusterApplierService clusterApplierService;
    private final IndicesService indicesService;

    public ChangeApplierService(ClusterApplierService clusterApplierService, IndicesService indicesService) {
        this.clusterApplierService = clusterApplierService;
        this.indicesService = indicesService;
    }

    @Override
    public void applyNodeState(String source, NodeState nodeState) {
        clusterApplierService.updateClusterState(source, s -> nodeState.buildClusterState(s, indicesService), this::logError);
    }

    @Override
    public void removeNode(String source, DiscoveryNode localNode) {
        // Return to empty cluster state
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build())
            .build();
        clusterApplierService.onNewClusterState(source, () -> clusterState, this::logError);
    }

    private void logError(String source, Exception e) {
        logger.error("Failed to update cluster state from {}", source, e);
    }
}
