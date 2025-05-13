/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;

import java.util.Collection;

public abstract class NodeState {
    protected final DiscoveryNode localNode;
    // TODO: Both coordinator and data nodes might need general metadata (not index metadata)

    public NodeState(DiscoveryNode localNode) {
        this.localNode = localNode;
    }

    public abstract ClusterState buildClusterState(ClusterState previous);

    public abstract Collection<String> getIndices();
}
