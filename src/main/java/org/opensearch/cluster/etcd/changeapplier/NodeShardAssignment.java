/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd.changeapplier;

import java.util.HashMap;
import java.util.Map;

public class NodeShardAssignment {
    public enum ShardRole {
        PRIMARY,
        REPLICA,
        SEARCH_REPLICA
    }

    private final Map<String, Map<Integer, ShardRole>> assignedShards = new HashMap<>();

    public void assignShard(String index, int shardId, ShardRole role) {
        assignedShards.computeIfAbsent(index, k -> new HashMap<>()).put(shardId, role);
    }

    public Map<String, Map<Integer, ShardRole>> getAssignedShards() {
        return assignedShards;
    }
}
