/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

public abstract class DataNodeShard {
    private final String indexName;
    private final int shardNum;
    private final String allocationId;

    public abstract ShardRole getShardRole();

    public DataNodeShard(String indexName, int shardNum, String allocationId) {
        this.indexName = indexName;
        this.shardNum = shardNum;
        this.allocationId = allocationId;
    }

    public int getShardNum() {
        return shardNum;
    }

    public String getAllocationId() {
        return allocationId;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        DataNodeShard that = (DataNodeShard) o;
        return shardNum == that.shardNum && Objects.equals(indexName, that.indexName) && Objects.equals(allocationId, that.allocationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, shardNum, allocationId);
    }

    public Collection<ShardAllocation> getReplicaAssignments() {
        return Collections.emptyList();
    }

    public enum ShardState {
        STARTED,
        RELOCATING,
        UNASSIGNED,
        INITIALIZING,
        CLOSING,
        CLOSED
    }

    public record ShardAllocation(RemoteNode node, String allocationId, ShardState shardState) {
    }

    public Optional<ShardAllocation> getPrimaryAllocation() {
        return Optional.empty();
    }

    public static class DocRepPrimary extends DataNodeShard {
        private final Collection<ShardAllocation> shardAllocations;

        public DocRepPrimary(String indexName, int shardNum, String allocationId, Collection<ShardAllocation> shardAllocations) {
            super(indexName, shardNum, allocationId);
            this.shardAllocations = shardAllocations;
        }

        @Override
        public ShardRole getShardRole() {
            return ShardRole.PRIMARY;
        }

        @Override
        public Collection<ShardAllocation> getReplicaAssignments() {
            return shardAllocations;
        }
    }

    public static class DocRepReplica extends DataNodeShard {
        private final ShardAllocation primaryNode;

        public DocRepReplica(String indexName, int shardNum, String allocationId, ShardAllocation primaryNode) {
            super(indexName, shardNum, allocationId);
            this.primaryNode = Objects.requireNonNull(primaryNode);
        }

        @Override
        public ShardRole getShardRole() {
            return ShardRole.REPLICA;
        }

        @Override
        public Optional<ShardAllocation> getPrimaryAllocation() {
            return Optional.of(primaryNode);
        }
    }

    public static class SegRepPrimary extends DataNodeShard {
        public SegRepPrimary(String indexName, int shardNum, String allocationId) {
            super(indexName, shardNum, allocationId);
        }

        @Override
        public ShardRole getShardRole() {
            return ShardRole.PRIMARY;
        }
    }

    public static class SegRepSearchReplica extends DataNodeShard {
        public SegRepSearchReplica(String indexName, int shardNum, String allocationId) {
            super(indexName, shardNum, allocationId);
        }

        @Override
        public ShardRole getShardRole() {
            return ShardRole.SEARCH_REPLICA;
        }

    }
}
