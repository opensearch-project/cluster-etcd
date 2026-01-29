/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.indices.NodeIndicesStats;
import org.opensearch.monitor.fs.FsProbe;
import java.io.IOException;
import org.opensearch.monitor.fs.FsInfo;
import io.etcd.jetcd.ByteSequence;
import org.opensearch.env.NodeEnvironment;
import io.etcd.jetcd.KV;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.monitor.jvm.JvmStats;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Requests;

import java.util.List;
import java.util.ArrayList;

public class ETCDHeartbeat {
    public static final String THREAD_POOL_NAME = "etcd-heartbeat";
    private static final long DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 5000; // 5 seconds
    private static final String CLUSTERLESS_ROLE_ATTRIBUTE = "clusterless_role";
    private static final String CLUSTERLESS_SHARD_ID_ATTRIBUTE = "clusterless_shard_id";

    // The following are needed to fetch and publish shard stats
    private static final NodesStatsRequest NODES_STATS_REQUEST = Requests.nodesStatsRequest()
        .indices(new CommonStatsFlags(CommonStatsFlags.Flag.Docs).setLevels(new String[] { "shards" }));
    private static final ToXContent.MapParams STATS_PARAMS = new ToXContent.MapParams(Map.of("level", "shards"));

    static final String TIMESTAMP = "timestamp";
    static final String NODE_NAME = "nodeName";
    static final String NODE_ID = "nodeId";
    static final String EPHEMERAL_ID = "ephemeralId";
    static final String ADDRESS = "address";
    static final String TRANSPORT_PORT = "transportPort";
    static final String HTTP_PORT = "httpPort";
    static final String HEARTBEAT_INTERVAL_MILLIS = "heartbeatIntervalMillis";
    static final String CLUSTERLESS_ROLE = "clusterlessRole";
    static final String CLUSTERLESS_SHARD_ID = "clusterlessShardId";
    static final String CPU_USED_PERCENT = "cpuUsedPercent";
    static final String MEMORY_USED_PERCENT = "memoryUsedPercent";
    static final String MEMORY_MAX_MB = "memoryMaxMB";
    static final String MEMORY_USED_MB = "memoryUsedMB";
    static final String HEAP_MAX_MB = "heapMaxMB";
    static final String HEAP_USED_MB = "heapUsedMB";
    static final String HEAP_USED_PERCENT = "heapUsedPercent";
    static final String DISK_TOTAL_MB = "diskTotalMB";
    static final String DISK_AVAILABLE_MB = "diskAvailableMB";
    static final String NODE_ROUTING = "nodeRouting";
    static final String SHARD_ID = "shardId";
    static final String ROLE = "role";
    static final String STATE = "state";
    static final String RELOCATING = "relocating";
    static final String RELOCATING_NODE_ID = "relocatingNodeId";
    static final String ALLOCATION_ID = "allocationId";
    static final String CURRENT_NODE_ID = "currentNodeId";
    static final String CURRENT_NODE_NAME = "currentNodeName";
    static final String STATS = "stats";

    private final Logger logger = LogManager.getLogger(getClass());
    private final String nodeName;
    private final String nodeId;
    private final String ephemeralId;
    private final String address;
    private final int transportPort;
    private final Integer httpPort;
    private final String clusterlessRole;
    private final String clusterlessShardId;
    private final ETCDClientHolder etcdClientHolder;
    private final org.opensearch.transport.client.Client openSearchClient;
    private final ThreadPool threadPool;
    private final ByteSequence nodeStateKey;
    private final NodeEnvironment nodeEnvironment;
    private final ClusterService clusterService;
    private final long heartbeatIntervalMillis;

    public ETCDHeartbeat(
        DiscoveryNode localNode,
        ETCDClientHolder etcdClientHolder,
        org.opensearch.transport.client.Client openSearchClient,
        NodeEnvironment nodeEnvironment,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this(localNode, etcdClientHolder, openSearchClient, nodeEnvironment, clusterService, threadPool, DEFAULT_HEARTBEAT_INTERVAL_MILLIS);
    }

    public ETCDHeartbeat(
        DiscoveryNode localNode,
        ETCDClientHolder etcdClientHolder,
        org.opensearch.transport.client.Client openSearchClient,
        NodeEnvironment nodeEnvironment,
        ClusterService clusterService,
        ThreadPool threadPool,
        long heartbeatIntervalMillis
    ) {
        this.nodeName = localNode.getName();
        this.nodeId = localNode.getId();
        this.ephemeralId = localNode.getEphemeralId();
        this.address = localNode.getAddress().getAddress();
        this.transportPort = localNode.getAddress().getPort();

        // Get HTTP port from settings (prefer http.publish_port, fall back to http.port)
        Settings settings = clusterService.getSettings();
        Integer httpPortValue = null;
        if (settings.hasValue("http.publish_port")) {
            httpPortValue = settings.getAsInt("http.publish_port", null);
        } else if (settings.hasValue("http.port")) {
            String httpPortSetting = settings.get("http.port");
            try {
                httpPortValue = Integer.parseInt(httpPortSetting);
            } catch (NumberFormatException e) {
                logger.debug("Could not parse http.port setting: {}", httpPortSetting);
            }
        }
        this.httpPort = httpPortValue;

        this.clusterlessRole = localNode.getAttributes()
            .getOrDefault(
                this.nodeName + "." + CLUSTERLESS_ROLE_ATTRIBUTE, // Key: Try node-specific first
                localNode.getAttributes().get(CLUSTERLESS_ROLE_ATTRIBUTE)  // DefaultValue: Fall back to generic key
            );
        this.clusterlessShardId = localNode.getAttributes()
            .getOrDefault(
                this.nodeName + "." + CLUSTERLESS_SHARD_ID_ATTRIBUTE, // Key: Try node-specific first
                localNode.getAttributes().get(CLUSTERLESS_SHARD_ID_ATTRIBUTE) // DefaultValue: Fall back to generic key
            );
        this.etcdClientHolder = etcdClientHolder;
        this.openSearchClient = openSearchClient;
        this.threadPool = threadPool;
        String clusterName = clusterService.getClusterName().value();
        String statePath = ETCDPathUtils.buildSearchUnitActualStatePath(localNode, clusterName);
        this.nodeStateKey = ByteSequence.from(statePath, StandardCharsets.UTF_8);
        this.nodeEnvironment = nodeEnvironment;
        this.clusterService = clusterService;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    }

    private static ScheduledExecutorService createScheduler() {
        return Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "etcd-heartbeat-scheduler"));
    }

    public static ExecutorBuilder<?> createExecutorBuilder(Settings settings) {
        return new FixedExecutorBuilder(settings, THREAD_POOL_NAME, 1, 1, THREAD_POOL_NAME);
    }

    public void start() {
        threadPool.scheduleWithFixedDelay(this::publishHeartbeat, TimeValue.timeValueMillis(heartbeatIntervalMillis), THREAD_POOL_NAME);
    }

    // Package-private for testing
    void publishHeartbeat() {
        try {
            // Get cpu info
            OsStats osStats = OsProbe.getInstance().osStats();
            int cpuPercent = osStats.getCpu().getPercent();

            // Get memory info
            int memoryPercent = osStats.getMem().getUsedPercent();
            ByteSizeValue memoryMax = osStats.getMem().getTotal();
            ByteSizeValue memoryUsed = osStats.getMem().getUsed();

            // Disk
            long diskTotalMB = 0;
            long diskAvailableMB = 0;
            if (nodeEnvironment != null) {
                try {
                    FsProbe fsProbe = new FsProbe(nodeEnvironment, null);
                    FsInfo fsInfo = fsProbe.stats(null);
                    for (FsInfo.Path path : fsInfo) {
                        diskTotalMB += path.getTotal().getMb();
                        diskAvailableMB += path.getAvailable().getMb();
                    }
                } catch (IOException e) {
                    logger.error("Failed to get fs info", e);
                }
            }

            // Get heap info
            JvmStats jvmStats = JvmStats.jvmStats();
            int heapUsedPercent = jvmStats.getMem().getHeapUsedPercent();
            ByteSizeValue heapMax = jvmStats.getMem().getHeapMax();
            ByteSizeValue heapUsed = jvmStats.getMem().getHeapUsed();

            // Build heartbeat data as a Map
            Map<String, Object> heartbeatData = new HashMap<>();
            heartbeatData.put(TIMESTAMP, System.currentTimeMillis());
            heartbeatData.put(NODE_NAME, nodeName);
            heartbeatData.put(NODE_ID, nodeId);
            heartbeatData.put(EPHEMERAL_ID, ephemeralId);
            heartbeatData.put(ADDRESS, address);
            heartbeatData.put(TRANSPORT_PORT, transportPort);

            // Add HTTP port if available
            if (httpPort != null) {
                heartbeatData.put(HTTP_PORT, httpPort);
            }

            heartbeatData.put(HEARTBEAT_INTERVAL_MILLIS, heartbeatIntervalMillis);

            // Add cloud native node attributes
            if (clusterlessRole != null) {
                heartbeatData.put(CLUSTERLESS_ROLE, clusterlessRole);
            }
            if (clusterlessShardId != null) {
                heartbeatData.put(CLUSTERLESS_SHARD_ID, clusterlessShardId);
            }
            heartbeatData.put(CPU_USED_PERCENT, cpuPercent);
            heartbeatData.put(MEMORY_USED_PERCENT, memoryPercent);
            heartbeatData.put(MEMORY_MAX_MB, memoryMax.getMb());
            heartbeatData.put(MEMORY_USED_MB, memoryUsed.getMb());
            heartbeatData.put(HEAP_MAX_MB, heapMax.getMb());
            heartbeatData.put(HEAP_USED_MB, heapUsed.getMb());
            heartbeatData.put(HEAP_USED_PERCENT, heapUsedPercent);
            heartbeatData.put(DISK_TOTAL_MB, diskTotalMB);
            heartbeatData.put(DISK_AVAILABLE_MB, diskAvailableMB);

            // Add node shard routing information
            try {
                ClusterState clusterState = clusterService.state();
                Map<String, List<Map<String, Object>>> nodeRoutingMap = getNodeRoutingMap(clusterState);
                heartbeatData.put(NODE_ROUTING, nodeRoutingMap);
            } catch (Exception e) {
                logger.error("Failed to get node routing information", e);
            }

            NodesStatsResponse nodesStatsResponse = openSearchClient.admin().cluster().nodesStats(NODES_STATS_REQUEST).actionGet();
            NodeStats nodeStats = nodesStatsResponse.getNodes().getFirst();
            NodeIndicesStats nodeIndicesStats = nodeStats.getIndices();

            // Publish to ETCD
            KV kvClient = etcdClientHolder.getClient().getKVClient();

            // Convert Map to JSON using XContent
            ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
            try (XContentBuilder jsonBuilder = XContentType.JSON.contentBuilder(jsonStream)) {
                jsonBuilder.startObject();
                jsonBuilder.mapContents(heartbeatData);
                if (nodeIndicesStats != null) {
                    jsonBuilder.startObject(STATS);
                    nodeIndicesStats.toXContent(jsonBuilder, STATS_PARAMS);
                    jsonBuilder.endObject();
                }
                jsonBuilder.endObject();
            }
            byte[] jsonBytes = jsonStream.toByteArray();

            ByteSequence value = ByteSequence.from(jsonBytes);
            kvClient.put(nodeStateKey, value).get();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            logger.error("Failed to publish heartbeat", e);
            // Don't throw the exception - let the scheduler continue with the next heartbeat
        }
    }

    // Get routing map for node by filtering through clusterState's routing table
    private Map<String, List<Map<String, Object>>> getNodeRoutingMap(ClusterState clusterState) {
        Map<String, List<Map<String, Object>>> nodeRoutingMap = new HashMap<>();

        // Iterate through all indices and their shards - report full cluster view
        for (IndexRoutingTable indexRoutingTable : clusterState.getRoutingTable()) {
            String indexName = indexRoutingTable.getIndex().getName();
            List<Map<String, Object>> allShards = new ArrayList<>();

            for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                int shardId = shardRoutingTable.shardId().id();
                // Include all shards regardless of which node they're assigned to
                for (ShardRouting shardRouting : shardRoutingTable) {
                    Map<String, Object> shardInfo = new HashMap<>();
                    shardInfo.put(SHARD_ID, shardId);
                    String role;
                    if (shardRouting.primary()) {
                        role = "primary";
                    } else if (shardRouting.isSearchOnly()) {
                        role = "search_replica";
                    } else {
                        role = "replica";
                    }
                    shardInfo.put(ROLE, role);
                    shardInfo.put(STATE, shardRouting.state().name());
                    shardInfo.put(RELOCATING, shardRouting.relocating());
                    if (shardRouting.relocating()) {
                        shardInfo.put(RELOCATING_NODE_ID, shardRouting.relocatingNodeId());
                    }
                    shardInfo.put(ALLOCATION_ID, shardRouting.allocationId().getId());
                    shardInfo.put(CURRENT_NODE_ID, shardRouting.currentNodeId());
                    shardInfo.put(CURRENT_NODE_NAME, clusterState.nodes().get(shardRouting.currentNodeId()).getName());
                    allShards.add(shardInfo);
                }
            }

            if (!allShards.isEmpty()) {
                nodeRoutingMap.put(indexName, allShards);
            }
        }

        return nodeRoutingMap;
    }
}
