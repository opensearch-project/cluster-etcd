/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd.changeapplier;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

public abstract class DataNodeShard {
    private final String indexName;
    private final int shardNum;

    public abstract ShardRole getShardRole();

    public DataNodeShard(String indexName, int shardNum) {
        this.indexName = indexName;
        this.shardNum = shardNum;
    }


    public int getShardNum() {
        return shardNum;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        DataNodeShard that = (DataNodeShard) o;
        return shardNum == that.shardNum && Objects.equals(indexName, that.indexName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, shardNum);
    }

    public Collection<ShardAllocation> getReplicaAssignments() {
        return Collections.emptyList();
    }


    public enum ShardState {
        STARTED, RELOCATING, UNASSIGNED, INITIALIZING, CLOSING, CLOSED
    }

    public record ShardAllocation(RemoteNode node, String allocationId, ShardState shardState) {}

    public Optional<ShardAllocation> getPrimaryAllocation() {
        return Optional.empty();
    }

    public static class DocRepPrimary extends DataNodeShard {
        private final Collection<ShardAllocation> shardAllocations;
        public DocRepPrimary(String indexName, int shardNum, Collection<ShardAllocation> shardAllocations) {
            super(indexName, shardNum);
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

        public DocRepReplica(String indexName, int shardNum, ShardAllocation primaryNode) {
            super(indexName, shardNum);
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
        public SegRepPrimary(String indexName, int shardNum) {
            super(indexName, shardNum);
        }

        @Override
        public ShardRole getShardRole() {
            return ShardRole.PRIMARY;
        }
    }

    public static class SegRepSearchReplica extends DataNodeShard {
        public SegRepSearchReplica(String indexName, int shardNum) {
            super(indexName, shardNum);
        }

        @Override
        public ShardRole getShardRole() {
            return ShardRole.SEARCH_REPLICA;
        }

    }
}
