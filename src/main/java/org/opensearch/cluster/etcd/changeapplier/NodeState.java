/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;

public abstract class NodeState {
    protected final boolean converged;
    protected final DiscoveryNode localNode;
    // TODO: Both coordinator and data nodes might need general metadata (not index metadata)

    public NodeState(DiscoveryNode localNode, boolean converged) {
        this.localNode = localNode;
        this.converged = converged;
    }

    public abstract ClusterState buildClusterState(ClusterState previous);

    public final boolean hasConverged() {
        return converged;
    }
}
