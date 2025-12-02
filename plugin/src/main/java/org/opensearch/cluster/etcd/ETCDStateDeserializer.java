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
import org.opensearch.cluster.etcd.changeapplier.CoordinatorNodeState;
import org.opensearch.cluster.etcd.changeapplier.DataNodeShard;
import org.opensearch.cluster.etcd.changeapplier.DataNodeState;
import org.opensearch.cluster.etcd.changeapplier.IndexMetadataComponents;
import org.opensearch.cluster.etcd.changeapplier.NodeShardAssignment;
import org.opensearch.cluster.etcd.changeapplier.NodeState;
import org.opensearch.cluster.etcd.changeapplier.RemoteNode;
import org.opensearch.cluster.etcd.changeapplier.ShardRole;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
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
 *   "transportPort": xxxx,
 *   "timestamp": 1750099493841,
 *   "heartbeatIntervalSeconds": 5
 * }
 * </pre>
 *
 * <b>Developer note:</b> This class should not depend on any OpenSearch-internal classes. We can depend on XContent
 * classes for basic JSON deserialization. The OpenSearch-specific classes should be in the changeapplier package, which
 * should not have any dependency on ETCD classes. The basic premise is that one package is ETCD-specific, and the other
 * is OpenSearch-specific, and they should communicate only via the NodeState implementations.
 * <p>
 * TODO: We currently depend on DiscoveryNode, which is OpenSearch-specific. We could replace it with node name and
 * pass the DiscoveryNode instance to buildClusterState.
 */
public final class ETCDStateDeserializer {
    public record NodeStateResult(NodeState nodeState, Collection<String> keysToWatch) {
    }

    private ETCDStateDeserializer() {}

    private static final Logger LOGGER = LogManager.getLogger(ETCDStateDeserializer.class);

    // Index settings keys
    private static final String INDEX_SETTINGS_KEY = "index";
    private static final String PAUSE_PULL_INGESTION_METADATA_KEY = "pause_pull_ingestion";

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
    public static NodeStateResult deserializeNodeState(
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
    private static NodeStateResult readCoordinatorNodeState(
        DiscoveryNode localNode,
        Client etcdClient,
        Map<String, Object> remoteShards,
        String clusterName
    ) throws IOException {
        Map<String, Object> indices = (Map<String, Object>) remoteShards.get("indices");
        Map<String, Object> aliases = (Map<String, Object>) remoteShards.getOrDefault("aliases", new HashMap<>());
        Map<String, Object> remoteClustersConfig = (Map<String, Object>) remoteShards.getOrDefault("remote_clusters", new HashMap<>());
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

        Map<String, Map<String, Object>> remoteClusters = new HashMap<>();
        for (Map.Entry<String, Object> entry : remoteClustersConfig.entrySet()) {
            String alias = entry.getKey();
            Map<String, Object> config = (Map<String, Object>) entry.getValue();
            Map<String, Object> remoteSettings = new HashMap<>();

            // Check if mode is specified (proxy mode or seed mode)
            String mode = config.getOrDefault("mode", "sniff").toString();

            if ("proxy".equalsIgnoreCase(mode)) {
                // Proxy mode configuration
                if (config.containsKey("proxy_address")) {
                    String proxyAddress = (String) config.get("proxy_address");
                    remoteSettings.put("cluster.remote." + alias + ".mode", "proxy");
                    remoteSettings.put("cluster.remote." + alias + ".proxy_address", proxyAddress);
                    remoteClusters.put(alias, remoteSettings);
                } else {
                    LOGGER.warn("Proxy mode specified for remote cluster '{}' but proxy_address is missing", alias);
                }
            } else if (config.containsKey("seeds")) {
                // Seed mode configuration (default/sniff mode)
                List<String> seeds = (List<String>) config.get("seeds");
                remoteSettings.put("cluster.remote." + alias + ".seeds", seeds);
                remoteClusters.put(alias, remoteSettings);
            }
        }

        List<String> keysToWatch = new ArrayList<>();
        Map<String, List<List<NodeShardAssignment>>> remoteShardAssignment = new HashMap<>();
        Set<RemoteNode> remoteNodes = new HashSet<>();
        for (Map.Entry<String, Object> indexEntry : indices.entrySet()) {
            Map<String, Object> indexConfig = (Map<String, Object>) indexEntry.getValue();
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
                        keysToWatch.add(ETCDPathUtils.buildSearchUnitActualStatePath(clusterName, nodeName));
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
            remoteShardAssignment.put(indexEntry.getKey(), shardAssignments);
        }

        CoordinatorNodeState coordinatorNodeState = new CoordinatorNodeState(
            localNode,
            remoteNodes,
            remoteShardAssignment,
            aliases,
            remoteClusters
        );
        return new NodeStateResult(coordinatorNodeState, keysToWatch);
    }

    private static NodeStateResult readDataNodeState(
        DiscoveryNode localNode,
        Client etcdClient,
        Map<String, Map<String, Object>> localShards,
        String clusterName,
        boolean isInitialLoad
    ) throws IOException {
        boolean converged = true;
        Set<String> pathsToWatch = new HashSet<>();
        Map<String, Set<DataNodeShard>> localShardAssignment = new HashMap<>();
        Map<String, IndexMetadataComponents> indexMetadataMap = new HashMap<>();

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
                pathsToWatch.add(indexSettingsPath);
                pathsToWatch.add(indexMappingsPath);

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
                    pathsToWatch.addAll(dataNodeShardConvergence.nonConvergedPaths());
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
                IndexMetadataComponents indexMetadata = buildIndexMetadataFromSeparateParts(settingsFutures.get(i), mappingsFutures.get(i));
                indexMetadataMap.put(indexName, indexMetadata);
            }
        }

        return new NodeStateResult(new DataNodeState(localNode, indexMetadataMap, localShardAssignment), pathsToWatch);
    }

    private record DataNodeShardConvergence(DataNodeShard shard, Collection<String> nonConvergedPaths) {
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
                return new DataNodeShardConvergence(
                    new DataNodeShard.SegRepPrimary(indexName, shardNum, allocationId),
                    Collections.emptySet()
                );
            } else if ("SEARCH_REPLICA".equalsIgnoreCase(role)) {
                return new DataNodeShardConvergence(
                    new DataNodeShard.SegRepSearchReplica(indexName, shardNum, allocationId),
                    Collections.emptySet()
                );
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

                    List<String> nonConvergedPaths = new ArrayList<>();
                    for (Map.Entry<String, NodeHealthInfo> entry : nodes.entrySet()) {
                        if (entry.getValue() == null) {
                            nonConvergedPaths.add(ETCDPathUtils.buildSearchUnitActualStatePath(clusterName, entry.getKey()));
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
                        if (replicaConverged == false) {
                            nonConvergedPaths.add(ETCDPathUtils.buildSearchUnitActualStatePath(clusterName, entry.getKey()));
                        }
                    }
                    return new DataNodeShardConvergence(
                        new DataNodeShard.DocRepPrimary(indexName, shardNum, allocationId, shardAllocations),
                        nonConvergedPaths
                    );
                } else {
                    // Segrep primary shard
                    return new DataNodeShardConvergence(
                        new DataNodeShard.SegRepPrimary(indexName, shardNum, allocationId),
                        Collections.emptySet()
                    );
                }
            case "REPLICA":
                String primaryNodeName = (String) shardMap.get("primary_node");
                if (primaryNodeName == null) {
                    throw new IllegalArgumentException("Replica shard must have a primary node specified");
                }
                String primaryNodeActualStatePath = ETCDPathUtils.buildSearchUnitActualStatePath(clusterName, primaryNodeName);
                DataNodeShard.ShardAllocation primaryAllocation = null;
                Map<String, NodeHealthInfo> nodes = fetchNodeHealthInfo(etcdClient, List.of(primaryNodeName), clusterName);
                NodeHealthInfo primaryNodeInfo = nodes.get(primaryNodeName);
                if (primaryNodeInfo == null) {
                    return new DataNodeShardConvergence(null, List.of(primaryNodeActualStatePath));
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
                            return new DataNodeShardConvergence(null, List.of(primaryNodeActualStatePath));
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
                    return new DataNodeShardConvergence(null, List.of(primaryNodeActualStatePath));
                }
                return new DataNodeShardConvergence(
                    new DataNodeShard.DocRepReplica(indexName, shardNum, allocationId, primaryAllocation),
                    Collections.emptySet()
                );
            case "SEARCH_REPLICA":
                // Segrep search replica shard
                return new DataNodeShardConvergence(
                    new DataNodeShard.SegRepSearchReplica(indexName, shardNum, allocationId),
                    Collections.emptySet()
                );
            default:
                throw new IllegalArgumentException("Unknown shard role: " + role);
        }
    }

    /**
     * Builds IndexMetadata from separate settings and mappings etcd responses.
     */
    private static IndexMetadataComponents buildIndexMetadataFromSeparateParts(
        CompletableFuture<GetResponse> settingsFuture,
        CompletableFuture<GetResponse> mappingsFuture
    ) {
        try {
            // Fetch and parse settings
            GetResponse settingsResponse = settingsFuture.get();
            if (settingsResponse.getKvs().isEmpty()) {
                throw new IllegalStateException("Settings response is empty");
            }
            KeyValue settingsKv = settingsResponse.getKvs().getFirst();
            // Convert to mutable map to allow modification
            Map<String, Object> settingsMap = new HashMap<>(
                XContentHelper.convertToMap(new BytesArray(settingsKv.getValue().getBytes()), true, MediaTypeRegistry.JSON).v2()
            );

            // Extract additional metadata from settings
            Map<String, Object> additionalMetadata = extractAdditionalMetadata(settingsMap);

            // Fetch and parse mappings
            GetResponse mappingsResponse = mappingsFuture.get();
            if (mappingsResponse.getKvs().isEmpty()) {
                throw new IllegalStateException("Mappings response is empty");
            }
            KeyValue mappingsKv = mappingsResponse.getKvs().getFirst();
            Map<String, Object> mappingsMap = XContentHelper.convertToMap(
                new BytesArray(mappingsKv.getValue().getBytes()),
                true,
                MediaTypeRegistry.JSON
            ).v2();

            return new IndexMetadataComponents(settingsMap, mappingsMap, additionalMetadata);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to fetch index metadata parts from etcd", e);
        }
    }

    /**
     * Extracts additional metadata fields from index settings and removes them from the settings map.
     * Currently extracts pause_pull_ingestion which controls whether data ingestion should be paused for the index
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractAdditionalMetadata(Map<String, Object> settingsMap) {
        Map<String, Object> additionalMetadata = new HashMap<>();

        // Early return if no index settings
        Object indexSettingsObj = settingsMap.get(INDEX_SETTINGS_KEY);
        if (!(indexSettingsObj instanceof Map)) {
            return additionalMetadata;
        }

        Map<String, Object> indexSettings = (Map<String, Object>) indexSettingsObj;
        Object pausePullIngestion = indexSettings.get(PAUSE_PULL_INGESTION_METADATA_KEY);
        if (pausePullIngestion == null) {
            return additionalMetadata;
        }

        // Create mutable copy and remove the metadata field
        Map<String, Object> mutableIndexSettings = new HashMap<>(indexSettings);
        mutableIndexSettings.remove(PAUSE_PULL_INGESTION_METADATA_KEY);
        settingsMap.put(INDEX_SETTINGS_KEY, mutableIndexSettings);

        additionalMetadata.put(PAUSE_PULL_INGESTION_METADATA_KEY, pausePullIngestion);
        return additionalMetadata;
    }

    @SuppressWarnings("unchecked")
    private static SortedMap<String, Object> sortMapRecursively(Map<String, Object> inputMap) {
        SortedMap<String, Object> sortedMap = new TreeMap<>();
        for (Map.Entry<String, Object> entry : inputMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                value = sortMapRecursively((Map<String, Object>) value);
            }
            sortedMap.put(entry.getKey(), value);
        }
        return sortedMap;
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

            String nodeId = (String) healthMap.get(ETCDHeartbeat.NODE_ID);
            String ephemeralId = (String) healthMap.get(ETCDHeartbeat.EPHEMERAL_ID);
            String address = (String) healthMap.get(ETCDHeartbeat.ADDRESS);
            int port = ((Number) healthMap.get(ETCDHeartbeat.TRANSPORT_PORT)).intValue();
            List<NodeShardAllocation> replicaAllocations = new ArrayList<>();
            List<NodeShardAllocation> primaryAllocations = new ArrayList<>();
            if (healthMap.containsKey(ETCDHeartbeat.NODE_ROUTING)) {
                Map<String, List<Map<String, Object>>> nodeRouting = (Map<String, List<Map<String, Object>>>) healthMap.get(
                    ETCDHeartbeat.NODE_ROUTING
                );
                for (Map.Entry<String, List<Map<String, Object>>> entry : nodeRouting.entrySet()) {
                    String indexName = entry.getKey();
                    List<Map<String, Object>> shardRouting = entry.getValue();
                    for (Map<String, Object> shardEntry : shardRouting) {
                        if (shardEntry.get(ETCDHeartbeat.CURRENT_NODE_ID).equals(nodeId)) {
                            int shardNum = (int) shardEntry.get(ETCDHeartbeat.SHARD_ID);
                            String role = (String) shardEntry.get(ETCDHeartbeat.ROLE);
                            String allocationId = (String) shardEntry.get(ETCDHeartbeat.ALLOCATION_ID);
                            String state = (String) shardEntry.get(ETCDHeartbeat.STATE);
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

    private record NodeHealthInfo(String nodeId, String ephemeralId, String address, int port, List<NodeShardAllocation> replicaAllocations,
        List<NodeShardAllocation> primaryAllocations) {
    }
}
