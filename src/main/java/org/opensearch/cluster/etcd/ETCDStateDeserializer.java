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
 *       "indices": { // Map of indices that this coordinator node is aware of.
 *          "idx1": {
 *              "uuid": "index-uuid",
 *              "shard_routing": [ // Must have every shard for the index.
 *                  [
 *                    {"node_name":"node1"},
 *                    {"node_name":"node2", "primary": true } // If we have a primary for one shard, we must have a primary for all shards
 *                  ],
 *                  [ // We don't assume an equal number of replicas for each shard.
 *                    {"node_name":"node1", "primary": true},
 *                    {"node_name":"node2"}, // Any non-primary is assumed to be a search replica.
 *                    {"node_name":"node3"}
 *                  ]
 *              ]
 *          },
 *          "idx2": { ... }
 *       }
 *   }
 * }
 * </pre>
 * 
 * Health check format (stored at {cluster_name}/search-unit/{node_name}/actual-state):
 * <pre>
 * {
 *   "nodeId": "unique-node-id",
 *   "ephemeralId": "ephemeral-id-123",
 *   "address": "xxx.xxx.x.xxx",
 *   "port": xxxx,
 *   "timestamp": 1750099493841,
 *   "heartbeatIntervalSeconds": 5
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
     * @param localNode the local discovery node
     * @param byteSequence the serialized node state
     * @param etcdClient the ETCD client that we'll use to retrieve index metadata for local shards
     * @param clusterName the cluster name used to build paths for health lookups
     * @return the relevant node state
     */
    @SuppressWarnings("unchecked")
    public static NodeState deserializeNodeState(DiscoveryNode localNode, ByteSequence byteSequence, Client etcdClient, String clusterName) throws IOException {
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
            return readCoordinatorNodeState(localNode, etcdClient, (Map<String, Object>) map.get("remote_shards"), clusterName);
        }
        throw new IllegalStateException("Neither local nor remote shards are present in the node state. Node state should have been removed.");
    }

    @SuppressWarnings("unchecked")
    private static CoordinatorNodeState readCoordinatorNodeState(DiscoveryNode localNode, Client etcdClient, Map<String, Object> remoteShards, String clusterName) throws IOException {
        Map<String, Object> indices = (Map<String, Object>) remoteShards.get("indices");
        Map<String, NodeHealthInfo> nodeHealthMap = new HashMap<>();
        
        for (Map.Entry<String, Object> indexEntry : indices.entrySet()) {
            Map<String, Object> indexConfig = (Map<String, Object>) indexEntry.getValue();
            List<List<Map<String, Object>>> shardRouting = (List<List<Map<String, Object>>>) indexConfig.get("shard_routing");
            
            for (List<Map<String, Object>> shardEntry : shardRouting) {
                for (Map<String, Object> nodeEntry : shardEntry) {
                    String nodeName = (String) nodeEntry.get("node_name");
                    if (nodeName != null && !nodeHealthMap.containsKey(nodeName)) {
                        nodeHealthMap.put(nodeName, null); 
                    }
                }
            }
        }
        
        lookupNodeHealthInfo(etcdClient, nodeHealthMap, clusterName);
        
        List<RemoteNode> remoteNodes = new ArrayList<>();
        for (Map.Entry<String, NodeHealthInfo> entry : nodeHealthMap.entrySet()) {
            NodeHealthInfo healthInfo = entry.getValue();
            if (healthInfo != null) {
                remoteNodes.add(new RemoteNode(healthInfo.nodeId, healthInfo.ephemeralId, healthInfo.address, healthInfo.port));
            } else {
                LOGGER.warn("Health information not found for node: {}", entry.getKey());
            }
        }

        Map<Index, List<List<NodeShardAssignment>>> remoteShardAssignment = new HashMap<>();
        for (Map.Entry<String, Object> indexEntry : indices.entrySet()) {
            Map<String, Object> indexConfig = (Map<String, Object>) indexEntry.getValue();
            String uuid = (String) indexConfig.get("uuid");
            List<List<Map<String, Object>>> shardRouting = (List<List<Map<String, Object>>>) indexConfig.get("shard_routing");
            List<List<NodeShardAssignment>> shardAssignments = new ArrayList<>(shardRouting.size());
            
            for (List<Map<String, Object>> shardEntry : shardRouting) {
                List<NodeShardAssignment> nodeShardAssignments = new ArrayList<>();
                for (Map<String, Object> nodeEntry : shardEntry) {
                    String nodeName = (String) nodeEntry.get("node_name");
                    boolean isPrimary = nodeEntry.containsKey("primary") && (Boolean) nodeEntry.get("primary");
                    
                    NodeHealthInfo healthInfo = nodeHealthMap.get(nodeName);
                    if (healthInfo != null) {
                        nodeShardAssignments.add(new NodeShardAssignment(healthInfo.nodeId, isPrimary ? ShardRole.PRIMARY : ShardRole.SEARCH_REPLICA));
                    } else {
                        LOGGER.error("Cannot resolve node name '{}' to node ID - health info not available", nodeName);
                        throw new IllegalStateException("Cannot resolve node name '" + nodeName + "' to node ID");
                    }
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


    private static void lookupNodeHealthInfo(Client etcdClient, Map<String, NodeHealthInfo> nodeHealthMap, String clusterName) throws IOException {
        try (KV kvClient = etcdClient.getKVClient()) {
            List<CompletableFuture<GetResponse>> futures = new ArrayList<>();
            List<String> nodeNames = new ArrayList<>();
            
            for (String nodeName : nodeHealthMap.keySet()) {
                String healthKey = ETCDPathUtils.buildNodeActualStatePath(clusterName, nodeName);
                futures.add(kvClient.get(ByteSequence.from(healthKey, StandardCharsets.UTF_8)));
                nodeNames.add(nodeName);
            }
            
            for (int i = 0; i < futures.size(); i++) {
                String nodeName = nodeNames.get(i);
                try {
                    GetResponse response = futures.get(i).get();
                    if (!response.getKvs().isEmpty()) {
                        KeyValue kv = response.getKvs().get(0);
                        NodeHealthInfo healthInfo = parseHealthInfo(kv);
                        nodeHealthMap.put(nodeName, healthInfo);
                        LOGGER.debug("Resolved node '{}' to ID '{}' with ephemeral ID '{}'", nodeName, healthInfo.nodeId, healthInfo.ephemeralId);
                    } else {
                        LOGGER.warn("No health information found for node: {}", nodeName);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("Failed to lookup health info for node: {}", nodeName, e);
                    throw new IOException("Failed to lookup health info for node: " + nodeName, e);
                }
            }
        }
    }
    
    /**
     * Parses health information from ETCD key-value pair.
     */
    @SuppressWarnings("unchecked")
    private static NodeHealthInfo parseHealthInfo(KeyValue kv) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, kv.getValue().getBytes())) {
            Map<String, Object> healthMap = parser.map();
            
            String nodeId = (String) healthMap.get("nodeId");
            String ephemeralId = (String) healthMap.get("ephemeralId");
            String address = (String) healthMap.get("address");
            int port = ((Number) healthMap.get("port")).intValue();
            
            return new NodeHealthInfo(nodeId, ephemeralId, address, port);
        }
    }
    

    private static class NodeHealthInfo {
        final String nodeId;
        final String ephemeralId;
        final String address;
        final int port;
        
        NodeHealthInfo(String nodeId, String ephemeralId, String address, int port) {
            this.nodeId = nodeId;
            this.ephemeralId = ephemeralId;
            this.address = address;
            this.port = port;
        }
    }
}

