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
    private final SnapshotRestoreInfo restoreInfo;

    public abstract ShardRole getShardRole();

    public DataNodeShard(String indexName, int shardNum, String allocationId, SnapshotRestoreInfo restoreInfo) {
        this.indexName = indexName;
        this.shardNum = shardNum;
        this.allocationId = allocationId;
        this.restoreInfo = restoreInfo;
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

    public Optional<SnapshotRestoreInfo> getRestoreInfo() {
        return Optional.ofNullable(restoreInfo);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        DataNodeShard that = (DataNodeShard) o;
        return shardNum == that.shardNum
            && Objects.equals(indexName, that.indexName)
            && Objects.equals(allocationId, that.allocationId)
            && Objects.equals(restoreInfo, that.restoreInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, shardNum, allocationId, restoreInfo);
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

        public DocRepPrimary(
            String indexName,
            int shardNum,
            String allocationId,
            SnapshotRestoreInfo restoreInfo,
            Collection<ShardAllocation> shardAllocations
        ) {
            super(indexName, shardNum, allocationId, restoreInfo);
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

        public DocRepReplica(
            String indexName,
            int shardNum,
            String allocationId,
            SnapshotRestoreInfo restoreInfo,
            ShardAllocation primaryNode
        ) {
            super(indexName, shardNum, allocationId, restoreInfo);
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
        public SegRepPrimary(String indexName, int shardNum, String allocationId, SnapshotRestoreInfo restoreInfo) {
            super(indexName, shardNum, allocationId, restoreInfo);
        }

        @Override
        public ShardRole getShardRole() {
            return ShardRole.PRIMARY;
        }
    }

    public static class SegRepSearchReplica extends DataNodeShard {
        public SegRepSearchReplica(String indexName, int shardNum, String allocationId, SnapshotRestoreInfo restoreInfo) {
            super(indexName, shardNum, allocationId, restoreInfo);
        }

        @Override
        public ShardRole getShardRole() {
            return ShardRole.SEARCH_REPLICA;
        }

    }
}
