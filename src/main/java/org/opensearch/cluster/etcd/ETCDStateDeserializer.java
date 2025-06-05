/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.etcd.changeapplier.CoordinatorNodeState;
import org.opensearch.cluster.etcd.changeapplier.DataNodeState;
import org.opensearch.cluster.etcd.changeapplier.NodeShardAssignment;
import org.opensearch.cluster.etcd.changeapplier.NodeState;
import org.opensearch.cluster.etcd.changeapplier.RemoteNode;
import org.opensearch.cluster.etcd.changeapplier.ShardRole;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Sample JSON:
 * <pre>
 * {
 *   "local_shards": { // Data node content
 *       "idx1": {
 *           "0" : "PRIMARY",
 *           "1" : "SEARCH_REPLICA"
 *       },
 *       "idx2": {
 *           "2" : "REPLICA"
 *       }
 *   },
 *   "remote_shards": { // Coordinator node content
 *       "remote_nodes": [ // List of remote nodes that this coordinator node is aware of.
 *         {
 *             "node_id": "node1",
 *             "ephemeral_id": "ephemeral-id-1",
 *             "address": "192.168.2.1",
 *             "port": 9300
 *         },
 *         {
 *             "node_id": "node2",
 *             "ephemeral_id": "ephemeral-id-2",
 *             "address": "192.168.2.2",
 *             "port": 9300
 *         },
 *         {
 *             "node_id": "node3",
 *             "ephemeral_id": "ephemeral-id-3",
 *             "address": "192.168.2.3",
 *             "port": 9300
 *         }
 *       ],
 *       "indices": { // Map of indices that this coordinator node is aware of.
 *          "idx1": {
 *              "uuid": "index-uuid",
 *              "shard_routing": [ // Must have every shard for the index.
 *                  [
 *                    {"node_id":"node1"}
 *                    {"node_id":"node2", "primary": true } // If we have a primary for one shard, we must have a primary for all shards
 *                  ],
 *                  [ // We don't assume an equal number of replicas for each shard.
 *                    {"node_id":"node1", "primary": true},
 *                    {"node_id":"node2"}, // Any non-primary is assumed to be a search replica.
 *                    {"node_id":"node3"}
 *                  ]
 *              ]
 *          },
 *          "idx2": { ... }
 *       }
 *   }
 * }
 * </pre>
 */
public class ETCDStateDeserializer {
    private static final Logger LOGGER = LogManager.getLogger(ETCDStateDeserializer.class);
    /**
     * Deserializes the node configuration stored in ETCD. Will also read the k/v pairs for each index
     * referenced from a data node.
     * <p>
     * For now, let's assume that we store JSON bytes in ETCD.
     *
     * @param byteSequence the serialized node state
     * @param etcdClient   the ETCD client that we'll use to retrieve index metadata for local shards
     * @return the relevant node state
     */
    @SuppressWarnings("unchecked")
    public static NodeState deserializeNodeState(DiscoveryNode localNode, ByteSequence byteSequence, Client etcdClient) throws IOException {
        Map<String, Object> map;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, byteSequence.getBytes())) {
            map = parser.map();
        }
        if (map.containsKey("local_shards")) {
            if (map.containsKey("remote_shards")) {
                // TODO: For now, assume a node is either a data node or a coordinator node.
                throw new IllegalStateException("Both local and remote shards are present in the node state. This is not yet supported.");
            }
            return readDataNodeState(localNode, etcdClient, (Map<String, Map<String, String>>) map.get("local_shards"));
        } else if (map.containsKey("remote_shards")) {
            return readCoordinatorNodeState(localNode, (Map<String, Object>) map.get("remote_shards"));
        }
        throw new IllegalStateException("Neither local nor remote shards are present in the node state. Node state should have been removed.");

    }

    @SuppressWarnings("unchecked")
    private static CoordinatorNodeState readCoordinatorNodeState(DiscoveryNode localNode, Map<String, Object> remoteShards) {
        List<Map<String, Object>> remoteNodeSpecs = (List<Map<String, Object>>) remoteShards.get("remote_nodes");
        List<RemoteNode> remoteNodes = new ArrayList<>(remoteNodeSpecs.size());
        for (Map<String, Object> remoteNodeSpec : remoteNodeSpecs) {
            String nodeId = (String) remoteNodeSpec.get("node_id");
            String ephemeralId = (String) remoteNodeSpec.get("ephemeral_id");
            String address = (String) remoteNodeSpec.get("address");
            int port = ((Number) remoteNodeSpec.get("port")).intValue();
            remoteNodes.add(new RemoteNode(nodeId, ephemeralId, address, port));
        }

        Map<Index, List<List<NodeShardAssignment>>> remoteShardAssignment = new HashMap<>();
        Map<String, Object> indices = (Map<String, Object>) remoteShards.get("indices");
        for (Map.Entry<String, Object> indexEntry : indices.entrySet()) {
            Map<String, Object> indexConfig = (Map<String, Object>) indexEntry.getValue();
            String uuid = (String) indexConfig.get("uuid");

            List<List<Map<String, Object>>> shardRouting = (List<List<Map<String, Object>>>) indexConfig.get("shard_routing");
            List<List<NodeShardAssignment>> shardAssignments = new ArrayList<>(shardRouting.size());
            for (List<Map<String, Object>> shardEntry : shardRouting) {
                List<NodeShardAssignment> nodeShardAssignments = new ArrayList<>();
                for (Map<String, Object> nodeEntry : shardEntry) {
                    String nodeId = (String) nodeEntry.get("node_id");
                    boolean isPrimary = nodeEntry.containsKey("primary") && (Boolean) nodeEntry.get("primary");
                    nodeShardAssignments.add(new NodeShardAssignment(nodeId, isPrimary ? ShardRole.PRIMARY : ShardRole.SEARCH_REPLICA));
                }
                shardAssignments.add(nodeShardAssignments);
            }
            remoteShardAssignment.put(new Index(indexEntry.getKey(), uuid), shardAssignments);
        }
        return new CoordinatorNodeState(localNode, remoteNodes, remoteShardAssignment);
    }

    private static DataNodeState readDataNodeState(DiscoveryNode localNode, Client etcdClient, Map<String, Map<String, String>> localShards) throws IOException {
        Map<String, Map<Integer, ShardRole>> localShardAssignment = new HashMap<>();
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        try (KV kvClient = etcdClient.getKVClient()) {
            List<CompletableFuture<GetResponse>> futures = new ArrayList<>();
            for (Map.Entry<String, Map<String, String>> entry : localShards.entrySet()) {
                String indexName = entry.getKey();
                futures.add(kvClient.get(ByteSequence.from(indexName, StandardCharsets.UTF_8)));
                Map<String, String> shards = entry.getValue();
                for (Map.Entry<String, String> shardEntry : shards.entrySet()) {
                    String shardId = shardEntry.getKey();
                    String shardType = shardEntry.getValue();
                    localShardAssignment.computeIfAbsent(indexName, k -> new HashMap<>()).put(Integer.parseInt(shardId), ShardRole.valueOf(shardType));
                }

            }
            for (var future : futures) {
                GetResponse getResponse;
                try {
                    getResponse = future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                for (KeyValue kv : getResponse.getKvs()) {
                    try(XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, kv.getValue().getBytes())) {
                        IndexMetadata indexMetadata = IndexMetadata.fromXContent(parser);
                        indexMetadataMap.put(kv.getKey().toString(), indexMetadata);
                    }
                }
            }
        }
        return new DataNodeState(localNode, indexMetadataMap, localShardAssignment);
    }
}
