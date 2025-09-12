/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.etcd.changeapplier.CoordinatorNodeState;
import org.opensearch.cluster.etcd.changeapplier.DataNodeShard;
import org.opensearch.cluster.etcd.changeapplier.DataNodeState;
import org.opensearch.cluster.etcd.changeapplier.NodeShardAssignment;
import org.opensearch.cluster.etcd.changeapplier.NodeState;
import org.opensearch.cluster.etcd.changeapplier.RemoteNode;
import org.opensearch.cluster.etcd.changeapplier.ShardRole;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Sample JSON for data node:
 * <pre>
 * {
 *   "local_shards": { // Data node content
 *       "idx1": {
 *           "0" : {
 *               "type": "PRIMARY", // Docrep primary shard
 *               "replica_nodes": [
 *                 "node1",
 *                 "node2"
 *               ]
 *           }
 *           "1" : {
 *               "type": "REPLICA", // Docrep replica shard
 *               "primary_node": "node2"
 *           }
 *       },
 *       "idx2": {
 *           "0" : "PRIMARY", // Segrep primary shard
 *           "1" : "SEARCH_REPLICA" // Segrep search replica shard
 *       }
 *   }
 * }
 * </pre>
 * <p>
 * Sample JSON for coordinator node:
 * <pre>
 * {
 *   "remote_shards": { // Coordinator node content
 *       "indices": { // Map of indices that this coordinator node is aware of.
 *          "idx1": {
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
 *       },
 *       "aliases": { // Map of alias names to their target indices
 *          "logs-current": "idx1",                    // Simple alias pointing to one index
 *          "logs-recent": ["idx1", "idx2"]            // Multi-index alias pointing to multiple indices
 *       }
 *   }
 * }
 * </pre>
 * <p>
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
public final class ETCDStateDeserializer {
    private ETCDStateDeserializer() {}

    private static final Logger LOGGER = LogManager.getLogger(ETCDStateDeserializer.class);

    /**
     * Deserializes the node configuration stored in ETCD. Will also read the k/v pairs for each index
     * referenced from a data node.
     * <p>
     * For now, let's assume that we store JSON bytes in ETCD.
     *
     * @param localNode    the local discovery node
     * @param byteSequence the serialized node state
     * @param etcdClient   the ETCD client that we'll use to retrieve index metadata for local shards
     * @param clusterName  the cluster name used to build paths for health lookups
     * @param isInitialLoad whether this is the initial load (true) or a subsequent update (false)
     * @return the relevant node state
     */
    @SuppressWarnings("unchecked")
    public static NodeState deserializeNodeState(
        DiscoveryNode localNode,
        ByteSequence byteSequence,
        Client etcdClient,
        String clusterName,
        boolean isInitialLoad
    ) throws IOException {
        Map<String, Object> map;
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                byteSequence.getBytes()
            )
        ) {
            map = parser.map();
        }
        if (map.containsKey("local_shards")) {
            if (map.containsKey("remote_shards")) {
                // TODO: For now, assume a node is either a data node or a coordinator node.
                throw new IllegalStateException("Both local and remote shards are present in the node state. This is not yet supported.");
            }
            return readDataNodeState(
                localNode,
                etcdClient,
                (Map<String, Map<String, Object>>) map.get("local_shards"),
                clusterName,
                isInitialLoad
            );
        } else if (map.containsKey("remote_shards")) {
            return readCoordinatorNodeState(localNode, etcdClient, (Map<String, Object>) map.get("remote_shards"), clusterName);
        }
        throw new IllegalStateException(
            "Neither local nor remote shards are present in the node state. Node state should have been removed."
        );
    }

    @SuppressWarnings("unchecked")
    private static CoordinatorNodeState readCoordinatorNodeState(
        DiscoveryNode localNode,
        Client etcdClient,
        Map<String, Object> remoteShards,
        String clusterName
    ) throws IOException {
        Map<String, Object> indices = (Map<String, Object>) remoteShards.get("indices");
        Map<String, Object> aliases = (Map<String, Object>) remoteShards.getOrDefault("aliases", new HashMap<>());
        Set<String> remoteNodeNames = new HashSet<>();

        for (Map.Entry<String, Object> indexEntry : indices.entrySet()) {
            Map<String, Object> indexConfig = (Map<String, Object>) indexEntry.getValue();
            List<List<Map<String, Object>>> shardRouting = (List<List<Map<String, Object>>>) indexConfig.get("shard_routing");

            for (List<Map<String, Object>> shardEntry : shardRouting) {
                for (Map<String, Object> nodeEntry : shardEntry) {
                    String nodeName = (String) nodeEntry.get("node_name");
                    if (nodeName != null) {
                        remoteNodeNames.add(nodeName);
                    }
                }
            }
        }

        Map<String, NodeHealthInfo> remoteNodeHealthMap = fetchNodeHealthInfo(etcdClient, remoteNodeNames, clusterName);

        boolean converged = true;
        Map<Index, List<List<NodeShardAssignment>>> remoteShardAssignment = new HashMap<>();
        Set<RemoteNode> remoteNodes = new HashSet<>();
        for (Map.Entry<String, Object> indexEntry : indices.entrySet()) {
            Map<String, Object> indexConfig = (Map<String, Object>) indexEntry.getValue();
            // Use index name as UUID if not explicitly provided in etcd
            // This ensures consistency across all nodes while keeping it simple
            String uuid = (String) indexConfig.getOrDefault("uuid", indexEntry.getKey());
            List<List<Map<String, Object>>> shardRouting = (List<List<Map<String, Object>>>) indexConfig.get("shard_routing");
            List<List<NodeShardAssignment>> shardAssignments = new ArrayList<>(shardRouting.size());

            for (List<Map<String, Object>> shardEntry : shardRouting) {
                List<NodeShardAssignment> nodeShardAssignments = new ArrayList<>();
                for (Map<String, Object> nodeEntry : shardEntry) {
                    String nodeName = (String) nodeEntry.get("node_name");
                    boolean isPrimary = nodeEntry.containsKey("primary") && (Boolean) nodeEntry.get("primary");

                    NodeHealthInfo remoteNodeHealth = remoteNodeHealthMap.get(nodeName);
                    if (remoteNodeHealth == null) {
                        // TODO -- Should we look at the node routing info to confirm that the target node has the
                        // specified shard?
                        converged = false;
                    } else {
                        RemoteNode remoteNode = new RemoteNode(
                            nodeName,
                            remoteNodeHealth.nodeId,
                            remoteNodeHealth.ephemeralId,
                            remoteNodeHealth.address,
                            remoteNodeHealth.port
                        );
                        remoteNodes.add(remoteNode);
                        nodeShardAssignments.add(
                            new NodeShardAssignment(remoteNode.nodeId(), isPrimary ? ShardRole.PRIMARY : ShardRole.SEARCH_REPLICA)
                        );
                    }
                }
                shardAssignments.add(nodeShardAssignments);
            }
            remoteShardAssignment.put(new Index(indexEntry.getKey(), uuid), shardAssignments);
        }

        return new CoordinatorNodeState(localNode, remoteNodes, remoteShardAssignment, aliases, converged);
    }

    private static DataNodeState readDataNodeState(
        DiscoveryNode localNode,
        Client etcdClient,
        Map<String, Map<String, Object>> localShards,
        String clusterName,
        boolean isInitialLoad
    ) throws IOException {
        boolean converged = true;
        Map<String, Set<DataNodeShard>> localShardAssignment = new HashMap<>();
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();

        // Fetch allocation ID information on initial load
        Map<String, Map<Integer, String>> allocationInfoMap = new HashMap<>();
        if (isInitialLoad) {
            Set<String> indexNames = localShards.keySet();
            allocationInfoMap = fetchShardAllocationInfo(localNode, etcdClient, clusterName, indexNames);
            LOGGER.debug("Fetched allocation info for initial load: {} indices", allocationInfoMap.size());
        } else {
            LOGGER.debug("Skipping allocation info fetch for subsequent update");
        }

        try (KV kvClient = etcdClient.getKVClient()) {
            // Prepare futures for fetching settings and mappings separately
            List<CompletableFuture<GetResponse>> settingsFutures = new ArrayList<>();
            List<CompletableFuture<GetResponse>> mappingsFutures = new ArrayList<>();
            List<String> indexNames = new ArrayList<>();

            for (Map.Entry<String, Map<String, Object>> entry : localShards.entrySet()) {
                String indexName = entry.getKey();

                // Fetch settings and mappings from separate etcd paths
                String indexSettingsPath = ETCDPathUtils.buildIndexSettingsPath(clusterName, indexName);
                String indexMappingsPath = ETCDPathUtils.buildIndexMappingsPath(clusterName, indexName);

                settingsFutures.add(kvClient.get(ByteSequence.from(indexSettingsPath, StandardCharsets.UTF_8)));
                mappingsFutures.add(kvClient.get(ByteSequence.from(indexMappingsPath, StandardCharsets.UTF_8)));
                Set<DataNodeShard> dataNodeShards = new HashSet<>();

                // Process shard assignments
                Map<String, Object> shards = entry.getValue();
                for (Map.Entry<String, Object> shardEntry : shards.entrySet()) {
                    int shardId = Integer.parseInt(shardEntry.getKey());
                    // Get allocation ID from the allocation info map
                    String allocationId = null;
                    Map<Integer, String> indexAllocationInfo = allocationInfoMap.get(indexName);
                    if (indexAllocationInfo != null) {
                        allocationId = indexAllocationInfo.get(shardId);
                    }
                    DataNodeShardConvergence dataNodeShardConvergence = readDataNodeShard(
                        indexName,
                        shardId,
                        shardEntry.getValue(),
                        etcdClient,
                        clusterName,
                        allocationId
                    );
                    converged &= dataNodeShardConvergence.converged();
                    if (dataNodeShardConvergence.shard() != null) {
                        dataNodeShards.add(dataNodeShardConvergence.shard());
                    }
                }

                if (dataNodeShards.isEmpty() == false) {
                    localShardAssignment.put(indexName, dataNodeShards);
                    indexNames.add(indexName);
                }
            }

            // Process the results
            for (int i = 0; i < indexNames.size(); i++) {
                String indexName = indexNames.get(i);
                IndexMetadata indexMetadata = buildIndexMetadataFromSeparateParts(
                    indexName,
                    settingsFutures.get(i),
                    mappingsFutures.get(i)
                );
                indexMetadataMap.put(indexName, indexMetadata);
            }
        }

        return new DataNodeState(localNode, indexMetadataMap, localShardAssignment, converged);
    }

    private record DataNodeShardConvergence(DataNodeShard shard, boolean converged) {
    }

    /**
     * Fetches allocation ID information for all shards from ETCD heartbeat data.
     * Reuses fetchNodeHealthInfo logic to avoid duplicating deserialization code.
     */
    private static Map<String, Map<Integer, String>> fetchShardAllocationInfo(
        DiscoveryNode localNode,
        Client etcdClient,
        String clusterName,
        Set<String> indexNames
    ) {
        Map<String, Map<Integer, String>> result = new HashMap<>();

        try {
            // Reuse fetchNodeHealthInfo logic to get health info for the local node
            Map<String, NodeHealthInfo> healthInfoMap = fetchNodeHealthInfo(etcdClient, List.of(localNode.getName()), clusterName);
            NodeHealthInfo localNodeHealth = healthInfoMap.get(localNode.getName());

            if (localNodeHealth == null) {
                LOGGER.debug("No health information found for local node: {}", localNode.getName());
                return result;
            }

            // Extract allocation info for each index and shard
            for (String indexName : indexNames) {
                Map<Integer, String> indexAllocationInfo = new HashMap<>();

                // Check both primary and replica allocations
                for (NodeShardAllocation allocation : localNodeHealth.primaryAllocations()) {
                    if (indexName.equals(allocation.indexName())) {
                        indexAllocationInfo.put(allocation.shardNum(), allocation.allocationId());
                        LOGGER.debug(
                            "Found primary allocation info for shard {}[{}]: {}",
                            allocation.indexName(),
                            allocation.shardNum(),
                            allocation.allocationId()
                        );
                    }
                }

                for (NodeShardAllocation allocation : localNodeHealth.replicaAllocations()) {
                    if (indexName.equals(allocation.indexName())) {
                        indexAllocationInfo.put(allocation.shardNum(), allocation.allocationId());
                        LOGGER.debug(
                            "Found replica allocation info for shard {}[{}]: {}",
                            allocation.indexName(),
                            allocation.shardNum(),
                            allocation.allocationId()
                        );
                    }
                }

                result.put(indexName, indexAllocationInfo);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to fetch shard allocation info from ETCD", e);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private static DataNodeShardConvergence readDataNodeShard(
        String indexName,
        int shardNum,
        Object shardConfig,
        Client etcdClient,
        String clusterName,
        String allocationId
    ) throws IOException {
        if (shardConfig instanceof String role) {
            // Single string value indicates a primary or search replica shard
            if ("PRIMARY".equalsIgnoreCase(role)) {
                return new DataNodeShardConvergence(new DataNodeShard.SegRepPrimary(indexName, shardNum, allocationId), true);
            } else if ("SEARCH_REPLICA".equalsIgnoreCase(role)) {
                return new DataNodeShardConvergence(new DataNodeShard.SegRepSearchReplica(indexName, shardNum, allocationId), true);
            } else {
                throw new IllegalArgumentException("Unknown shard role: " + role);
            }
        }
        Map<String, Object> shardMap = (Map<String, Object>) shardConfig;
        String role = shardMap.get("type").toString();
        switch (role.toUpperCase(Locale.ROOT)) {
            case "PRIMARY":
                if (shardMap.containsKey("replica_nodes")) {
                    // Docrep primary shard with replicas
                    List<String> replicaNodes = (List<String>) shardMap.get("replica_nodes");

                    Map<String, NodeHealthInfo> nodes = fetchNodeHealthInfo(etcdClient, replicaNodes, clusterName);
                    List<DataNodeShard.ShardAllocation> shardAllocations = new ArrayList<>();

                    boolean converged = true;
                    for (Map.Entry<String, NodeHealthInfo> entry : nodes.entrySet()) {
                        if (entry.getValue() == null) {
                            converged = false;
                            continue;
                        }
                        boolean replicaConverged = false;
                        for (NodeShardAllocation allocation : entry.getValue().replicaAllocations) {
                            if (allocation.indexName.equals(indexName) && allocation.shardNum == shardNum) {
                                RemoteNode replicaNode = new RemoteNode(
                                    entry.getKey(),
                                    entry.getValue().nodeId,
                                    entry.getValue().ephemeralId,
                                    entry.getValue().address,
                                    entry.getValue().port
                                );
                                DataNodeShard.ShardState shardState = DataNodeShard.ShardState.valueOf(allocation.state);
                                if (shardState == DataNodeShard.ShardState.STARTED) {
                                    replicaConverged = true;
                                }
                                shardAllocations.add(new DataNodeShard.ShardAllocation(replicaNode, allocation.allocationId, shardState));
                            }
                        }
                        converged &= replicaConverged;
                    }
                    return new DataNodeShardConvergence(
                        new DataNodeShard.DocRepPrimary(indexName, shardNum, allocationId, shardAllocations),
                        converged
                    );
                } else {
                    // Segrep primary shard
                    return new DataNodeShardConvergence(new DataNodeShard.SegRepPrimary(indexName, shardNum, allocationId), true);
                }
            case "REPLICA":
                String primaryNodeName = (String) shardMap.get("primary_node");
                if (primaryNodeName == null) {
                    throw new IllegalArgumentException("Replica shard must have a primary node specified");
                }
                DataNodeShard.ShardAllocation primaryAllocation = null;
                Map<String, NodeHealthInfo> nodes = fetchNodeHealthInfo(etcdClient, List.of(primaryNodeName), clusterName);
                NodeHealthInfo primaryNodeInfo = nodes.get(primaryNodeName);
                if (primaryNodeInfo == null) {
                    return new DataNodeShardConvergence(null, false);
                }
                RemoteNode primaryNode = new RemoteNode(
                    primaryNodeName,
                    primaryNodeInfo.nodeId,
                    primaryNodeInfo.ephemeralId,
                    primaryNodeInfo.address,
                    primaryNodeInfo.port
                );
                for (NodeShardAllocation allocation : primaryNodeInfo.primaryAllocations) {
                    if (allocation.indexName.equals(indexName) && allocation.shardNum == shardNum) {
                        if (!allocation.state.equals("STARTED")) {
                            // We cannot allocate the replica shard until after the primary has started
                            return new DataNodeShardConvergence(null, false);
                        }
                        primaryAllocation = new DataNodeShard.ShardAllocation(
                            primaryNode,
                            allocation.allocationId,
                            DataNodeShard.ShardState.STARTED
                        );
                        break;
                    }
                }
                if (primaryAllocation == null) {
                    // Primary shard not allocated on primary node
                    return new DataNodeShardConvergence(null, false);
                }
                return new DataNodeShardConvergence(
                    new DataNodeShard.DocRepReplica(indexName, shardNum, allocationId, primaryAllocation),
                    true
                );
            case "SEARCH_REPLICA":
                // Segrep search replica shard
                return new DataNodeShardConvergence(new DataNodeShard.SegRepSearchReplica(indexName, shardNum, allocationId), true);
            default:
                throw new IllegalArgumentException("Unknown shard role: " + role);
        }
    }

    /**
     * Builds IndexMetadata from separate settings and mappings etcd responses.
     * Also populates constant values that don't need to be stored in etcd.
     */
    private static IndexMetadata buildIndexMetadataFromSeparateParts(
        String indexName,
        CompletableFuture<GetResponse> settingsFuture,
        CompletableFuture<GetResponse> mappingsFuture
    ) throws IOException {
        Settings indexSettings;
        MappingMetadata mappingMetadata;

        try {
            // Fetch and parse settings
            GetResponse settingsResponse = settingsFuture.get();
            if (settingsResponse.getKvs().isEmpty()) {
                throw new IllegalStateException("Settings response is empty");
            }
            KeyValue settingsKv = settingsResponse.getKvs().getFirst();
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    settingsKv.getValue().getBytes()
                )
            ) {
                indexSettings = Settings.fromXContent(parser);
            }

            // Fetch and parse mappings
            GetResponse mappingsResponse = mappingsFuture.get();
            if (mappingsResponse.getKvs().isEmpty()) {
                throw new IllegalStateException("Mappings response is empty");
            }
            KeyValue mappingsKv = mappingsResponse.getKvs().getFirst();
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                mappingsKv.getValue().getBytes()
            );
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (XContentBuilder mappingsBuilder = new XContentBuilder(JsonXContent.jsonXContent, byteArrayOutputStream)) {
                mappingsBuilder.startObject();
                mappingsBuilder.field("_doc");
                mappingsBuilder.copyCurrentStructure(parser);
                mappingsBuilder.endObject();
            }
            CompressedXContent mappingsXContent = new CompressedXContent(new BytesArray(byteArrayOutputStream.toByteArray()));
            mappingMetadata = new MappingMetadata(mappingsXContent);

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to fetch index metadata parts from etcd", e);
        }

        // Build IndexMetadata with both etcd-sourced and constant values
        return buildIndexMetadataWithConstants(indexName, indexSettings, mappingMetadata);
    }

    /**
     * Builds IndexMetadata with settings and mappings from etcd, plus constant values
     * that are populated in the plugin rather than stored in etcd.
     */
    private static IndexMetadata buildIndexMetadataWithConstants(
        String indexName,
        Settings indexSettings,
        MappingMetadata mappingMetadata
    ) {
        // Start with etcd-sourced settings
        Settings.Builder settingsBuilder = indexSettings != null ? Settings.builder().put(indexSettings) : Settings.builder();

        // Add required system constants that must be present for IndexMetadata
        // Use index name as UUID for simplicity and consistency
        settingsBuilder.put(IndexMetadata.SETTING_INDEX_UUID, indexName);

        // Set version to current OpenSearch version
        settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);

        // Set creation date deterministically if not already present (for stateless operation)
        if (!settingsBuilder.keys().contains(IndexMetadata.SETTING_CREATION_DATE)) {
            // Use a deterministic timestamp based on index name to ensure all nodes generate the same value
            settingsBuilder.put(IndexMetadata.SETTING_CREATION_DATE, generateDeterministicCreationDate(indexName));
        }

        // Build IndexMetadata
        Settings finalSettings = settingsBuilder.build();
        IndexMetadata.Builder metadataBuilder = IndexMetadata.builder(indexName).settings(finalSettings);

        // Add mapping if present
        if (mappingMetadata != null) {
            metadataBuilder.putMapping(mappingMetadata);
        }

        // Set primary terms for each shard (these are constants that don't need to be in etcd)
        // Get number of shards from settings
        int numberOfShards = finalSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        for (int i = 0; i < numberOfShards; i++) {
            metadataBuilder.primaryTerm(i, 1);
        }

        return metadataBuilder.build();
    }

    private static Map<String, NodeHealthInfo> fetchNodeHealthInfo(Client etcdClient, Collection<String> nodeNames, String clusterName)
        throws IOException {
        Map<String, NodeHealthInfo> healthInfoMap = new HashMap<>();
        for (String nodeName : nodeNames) {
            String healthKey = ETCDPathUtils.buildSearchUnitActualStatePath(clusterName, nodeName);
            try (KV kvClient = etcdClient.getKVClient()) {
                GetResponse response = kvClient.get(ByteSequence.from(healthKey, StandardCharsets.UTF_8)).get();
                if (!response.getKvs().isEmpty()) {
                    KeyValue kv = response.getKvs().getFirst();
                    NodeHealthInfo healthInfo = parseHealthInfo(kv);
                    healthInfoMap.put(nodeName, healthInfo);
                } else {
                    LOGGER.warn("No health information found for node: {}", nodeName);
                    healthInfoMap.put(nodeName, null);
                }
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Failed to fetch health info for node: {}", nodeName, e);
                throw new IOException("Failed to fetch health info for node: " + nodeName, e);
            }
        }
        return healthInfoMap;
    }

    private record NodeShardAllocation(String indexName, int shardNum, String allocationId, String state) {
    }

    /**
     * Parses health information from ETCD key-value pair.
     */
    @SuppressWarnings("unchecked")
    private static NodeHealthInfo parseHealthInfo(KeyValue kv) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                kv.getValue().getBytes()
            )
        ) {
            Map<String, Object> healthMap = parser.map();

            String nodeId = (String) healthMap.get("nodeId");
            String ephemeralId = (String) healthMap.get("ephemeralId");
            String address = (String) healthMap.get("address");
            int port = ((Number) healthMap.get("port")).intValue();
            List<NodeShardAllocation> replicaAllocations = new ArrayList<>();
            List<NodeShardAllocation> primaryAllocations = new ArrayList<>();
            if (healthMap.containsKey("nodeRouting")) {
                Map<String, List<Map<String, Object>>> nodeRouting = (Map<String, List<Map<String, Object>>>) healthMap.get("nodeRouting");
                for (Map.Entry<String, List<Map<String, Object>>> entry : nodeRouting.entrySet()) {
                    String indexName = entry.getKey();
                    List<Map<String, Object>> shardRouting = entry.getValue();
                    for (Map<String, Object> shardEntry : shardRouting) {
                        if (shardEntry.get("currentNodeId").equals(nodeId)) {
                            int shardNum = (int) shardEntry.get("shardId");
                            String role = (String) shardEntry.get("role");
                            String allocationId = (String) shardEntry.get("allocationId");
                            String state = (String) shardEntry.get("state");
                            if ("replica".equalsIgnoreCase(role)) {
                                replicaAllocations.add(new NodeShardAllocation(indexName, shardNum, allocationId, state));
                            } else if ("primary".equalsIgnoreCase(role)) {
                                primaryAllocations.add(new NodeShardAllocation(indexName, shardNum, allocationId, state));
                            }
                        }
                    }

                }
            }
            return new NodeHealthInfo(nodeId, ephemeralId, address, port, replicaAllocations, primaryAllocations);
        }
    }

    /**
     * Generates a deterministic creation date based on the index name.
     * This ensures all nodes generate the same creation date for the same index.
     */
    private static long generateDeterministicCreationDate(String indexName) {
        // Generate a deterministic timestamp based on index name
        // This ensures all nodes generate the same creation date for the same index
        // Use a fixed epoch time (e.g., 2024-01-01) plus a hash of the index name
        long baseEpoch = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Generate a deterministic offset based on index name hash
        int hashOffset = (indexName.hashCode() & Integer.MAX_VALUE) % (24 * 60 * 60 * 1000);

        return baseEpoch + hashOffset;
    }

    private record NodeHealthInfo(String nodeId, String ephemeralId, String address, int port, List<NodeShardAllocation> replicaAllocations,
        List<NodeShardAllocation> primaryAllocations) {
    }
}
