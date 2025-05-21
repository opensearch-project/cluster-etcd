/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CoordinatorNodeState extends NodeState {

    private final Map<String, List<List<RemoteNode>>> routingTable;

    public CoordinatorNodeState(DiscoveryNode localNode, Map<String, List<List<RemoteNode>>> routingTable) {
        super(localNode);
        this.routingTable = routingTable;
    }

    @Override
    public ClusterState buildClusterState(ClusterState previous) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode);

        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Set<RemoteNode> uniqueNodes = new HashSet<>();

        for (Map.Entry<String, List<List<RemoteNode>>> indexEntry : routingTable.entrySet()) {
            int shardNum = 0;
            int replicaCount = 0;
            for (List<RemoteNode> shardRouting : indexEntry.getValue()) {
                int shardReplicaCount = shardRouting.size() - 1;
                replicaCount = Math.min(replicaCount, shardReplicaCount);
            }
            IndexMetadata indexMetadata = IndexMetadata.builder(indexEntry.getKey())
                .settings(Settings.builder()
                    .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, indexEntry.getValue().size())
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicaCount)
                )
                .build();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (List<RemoteNode> shardRouting : indexEntry.getValue()) {

                ShardId shardId = new ShardId(indexMetadata.getIndex(), shardNum++);
                IndexShardRoutingTable.Builder shardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardId);
                for (RemoteNode remoteNode : shardRouting) {
                    uniqueNodes.add(remoteNode);
                    // TODO: I'm enforcing that coordinators will only ever point to search replicas.
                    // If we want to support push-based indexing, we will need coordinators to point to
                    // primary shards as well. This will require a change to the routingTable definition (or possibly
                    // to RemoteNode, since it's only used here) to incorporate the shard role.
                    ShardRouting nodeEntry = ShardRouting.newUnassigned(
                        shardId,
                        false,
                        true,
                        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "initializing")
                    );
                    nodeEntry = nodeEntry.initialize(remoteNode.nodeId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                    nodeEntry = nodeEntry.moveToStarted();
                    shardRoutingTableBuilder.addShard(nodeEntry);
                }
                indexRoutingTableBuilder.addIndexShard(shardRoutingTableBuilder.build());
                shardNum++;
            }
            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(indexMetadata, false);
        }
        for (RemoteNode remoteNode : uniqueNodes) {
            DiscoveryNode node = new DiscoveryNode(
                remoteNode.nodeId(),
                remoteNode.nodeId(),
                remoteNode.nodeId(),
                remoteNode.address(),
                remoteNode.address(),
                new TransportAddress(
                    new InetSocketAddress(
                        remoteNode.address(),
                        remoteNode.port()
                    )
                ),
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT);
            nodesBuilder.add(node);
        }

        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
    }

}
