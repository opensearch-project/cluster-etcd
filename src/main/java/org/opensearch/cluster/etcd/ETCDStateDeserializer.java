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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.json.JsonXContent;
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
 *       "idx1": [ // Must have every shard for the index.
 *         [
 *           {"node_id":"node1", "address":"192.168.2.1", "port": 9300 },
 *           {"node_id":"node2", "address":"192.168.2.2", "port": 9300 }
 *         ],
 *         [ // We don't assume an equal number of replicas for each shard.
 *           {"node_id":"node1", "address":"192.168.2.1", "port": 9300 },
 *           {"node_id":"node2", "address":"192.168.2.2", "port": 9300 },
 *           {"node_id":"node3", "address":"192.168.2.3", "port": 9300 }
 *         ]
 *       ],
 *       "idx2": [ ... ]
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
            Map<String, Map<String, String>> localShards = (Map<String, Map<String, String>>) map.get("local_shards");
            NodeShardAssignment localShardAssignment = new NodeShardAssignment();
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
                        localShardAssignment.assignShard(indexName, Integer.parseInt(shardId), NodeShardAssignment.ShardRole.valueOf(shardType));
                    }
                    for (var future : futures) {
                        GetResponse getResponse = null;
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
            }
            return new DataNodeState(localNode, indexMetadataMap, localShardAssignment);
        } else if (map.containsKey("remote_shards")) {
            Map<String, List<List<Map<String, Object>>>> remoteShards = (Map<String, List<List<Map<String, Object>>>>) map.get("remote_shards");
            Map<String, List<List<RemoteNode>>> remoteShardAssignment = new HashMap<>();
            for (Map.Entry<String, List<List<Map<String, Object>>>> indexEntry : remoteShards.entrySet()) {
                List<List<RemoteNode>> shardAssignments = new ArrayList<>(indexEntry.getValue().size());
                for (List<Map<String, Object>> shardEntry : indexEntry.getValue()) {
                    List<RemoteNode> remoteNodes = new ArrayList<>(shardEntry.size());
                    for (Map<String, Object> nodeEntry : shardEntry) {
                        String nodeId = (String) nodeEntry.get("node_id");
                        String address = (String) nodeEntry.get("address");
                        int port = ((Number) nodeEntry.get("port")).intValue();
                        remoteNodes.add(new RemoteNode(nodeId, address, port));
                    }
                    shardAssignments.add(remoteNodes);
                }
                remoteShardAssignment.put(indexEntry.getKey(), shardAssignments);
            }
            return new CoordinatorNodeState(localNode, remoteShardAssignment);

        }
        throw new IllegalStateException("Neither local nor remote shards are present in the node state. Node state should have been removed.");

    }
}
