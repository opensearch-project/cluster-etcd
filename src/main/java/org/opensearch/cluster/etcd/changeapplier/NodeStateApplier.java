/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.cluster.node.DiscoveryNode;

public interface NodeStateApplier {
    void applyNodeState(String source, NodeState nodeState);

    void removeNode(String source, DiscoveryNode localNode);
}
