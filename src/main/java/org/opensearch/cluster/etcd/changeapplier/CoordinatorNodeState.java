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
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents the state of a coordinator node in the etcd-coordinated cluster.
 * Coordinator nodes are responsible for routing requests to data nodes and
 * coordinating distributed operations across the cluster.
 */
public class CoordinatorNodeState extends NodeState {

    private final List<RemoteNode> remoteNodes;
    private final Map<Index, List<List<NodeShardAssignment>>> remoteShardAssignments;

    /**
     * Creates a new CoordinatorNodeState.
     *
     * @param localNode              the local coordinator node
     * @param remoteNodes            list of remote data nodes that this coordinator manages
     * @param remoteShardAssignments map of indices to their shard assignments across remote nodes
     */
    public CoordinatorNodeState(DiscoveryNode localNode, List<RemoteNode> remoteNodes, Map<Index, List<List<NodeShardAssignment>>> remoteShardAssignments) {
        super(localNode);
        this.remoteNodes = remoteNodes;
        this.remoteShardAssignments = remoteShardAssignments;
    }

    @Override
    public ClusterState buildClusterState(ClusterState previous) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode);

        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();

        for (Map.Entry<Index, List<List<NodeShardAssignment>>> indexEntry : remoteShardAssignments.entrySet()) {
            int shardNum = 0;

            boolean indexHasPrimary = false;
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexEntry.getKey());
            for (List<NodeShardAssignment> shardRouting : indexEntry.getValue()) {
                ShardId shardId = new ShardId(indexEntry.getKey(), shardNum);
                IndexShardRoutingTable.Builder shardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardId);
                boolean shardHasPrimary = false;
                for (NodeShardAssignment shardAssignment : shardRouting) {
                    ShardRole shardRole = shardAssignment.shardRole();
                    if (shardRole == ShardRole.PRIMARY) {
                        if (indexHasPrimary == false && shardNum > 0) {
                            // If the index has primary shards, we need to figure it out from the first shard.
                            throw new IllegalStateException("Index " + indexEntry.getKey().getName() + " has at least one primary shard, but the first shard has no primary assigned.");
                        }
                        indexHasPrimary = true;
                        shardHasPrimary = true;
                    }
                    ShardRouting nodeEntry = ShardRouting.newUnassigned(
                        shardId,
                        shardRole == ShardRole.PRIMARY,
                        shardRole == ShardRole.SEARCH_REPLICA,
                        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "initializing")
                    );
                    nodeEntry = nodeEntry.initialize(shardAssignment.nodeId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                    nodeEntry = nodeEntry.moveToStarted();
                    shardRoutingTableBuilder.addShard(nodeEntry);
                }
                if (indexHasPrimary == true && shardHasPrimary == false) {
                    throw new IllegalStateException("Index " + indexEntry.getKey().getName() + " has a primary shard, but shard " + shardNum + " has no primary assigned.");
                }
                indexRoutingTableBuilder.addIndexShard(shardRoutingTableBuilder.build());
                shardNum++;
            }
            Settings.Builder indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_INDEX_UUID, indexEntry.getKey().getUUID())
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, indexEntry.getValue().size())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
            if (indexHasPrimary == false) {
                indexSettings.put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true);
            }
            IndexMetadata indexMetadata = IndexMetadata.builder(indexEntry.getKey().getName())
                .settings(indexSettings)
                .build();
            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(indexMetadata, false);
        }
        for (RemoteNode remoteNode : remoteNodes) {
            DiscoveryNode node;
            try {
                node = new DiscoveryNode(
                    remoteNode.nodeId(),
                    remoteNode.nodeId(),
                    remoteNode.ephemeralId(),
                    remoteNode.address(),
                    remoteNode.address(),
                    new TransportAddress(
                        new InetSocketAddress(
                            InetAddress.getByAddress(InetAddresses.ipStringToBytes(remoteNode.address())),
                            remoteNode.port()
                        )
                    ),
                    Collections.emptyMap(),
                    Set.of(DiscoveryNodeRole.DATA_ROLE),
                    Version.CURRENT);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            nodesBuilder.add(node);
        }

        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
    }

}
