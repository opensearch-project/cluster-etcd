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

/**
 * Abstract base class representing the state of a node in the etcd-coordinated cluster.
 * Different node types (coordinator, data nodes) extend this class to provide
 * specific cluster state building logic.
 */
public abstract class NodeState {
    /** The local node that this state represents */
    protected final DiscoveryNode localNode;
    // TODO: Both coordinator and data nodes might need general metadata (not index metadata)

    /**
     * Creates a new NodeState for the given local node.
     *
     * @param localNode the local discovery node
     */
    public NodeState(DiscoveryNode localNode) {
        this.localNode = localNode;
    }

    /**
     * Builds a cluster state based on this node's view of the cluster.
     *
     * @param previous the previous cluster state
     * @return the new cluster state reflecting this node's current state
     */
    public abstract ClusterState buildClusterState(ClusterState previous);

}
