/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

/**
 * Represents the assignment of a shard to a node, from the coordinator's perspective.
 */
public record NodeShardAssignment(String nodeId, ShardRole shardRole) {
}
