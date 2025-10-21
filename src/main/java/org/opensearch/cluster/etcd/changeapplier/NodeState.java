/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.indices.IndicesService;

public abstract class NodeState {
    protected final DiscoveryNode localNode;
    // TODO: Both coordinator and data nodes might need general metadata (not index metadata)

    public NodeState(DiscoveryNode localNode) {
        this.localNode = localNode;
    }

    public abstract ClusterState buildClusterState(ClusterState previous, IndicesService indicesService);

}
