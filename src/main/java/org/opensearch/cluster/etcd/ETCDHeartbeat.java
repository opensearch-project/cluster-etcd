/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import io.etcd.jetcd.Client;
import org.opensearch.core.common.unit.ByteSizeValue;
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
import java.util.concurrent.TimeUnit;
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
import java.util.List;
import java.util.ArrayList;

public class ETCDHeartbeat {
    private static final long DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 5000; // 5 seconds
    private final Logger logger = LogManager.getLogger(getClass());
    private final String nodeName;
    private final String nodeId;
    private final String ephemeralId;
    private final String address;
    private final int port;
    private final Client etcdClient;
    private final ScheduledExecutorService scheduler;
    private final ByteSequence nodeStateKey;
    private final NodeEnvironment nodeEnvironment;
    private final ClusterService clusterService;
    private final long heartbeatIntervalMillis;

    public ETCDHeartbeat(DiscoveryNode localNode, Client etcdClient, NodeEnvironment nodeEnvironment, ClusterService clusterService) {
        this(localNode, etcdClient, nodeEnvironment, clusterService, DEFAULT_HEARTBEAT_INTERVAL_MILLIS);
    }

    public ETCDHeartbeat(
        DiscoveryNode localNode,
        Client etcdClient,
        NodeEnvironment nodeEnvironment,
        ClusterService clusterService,
        long heartbeatIntervalMillis
    ) {
        this.nodeName = localNode.getName();
        this.nodeId = localNode.getId();
        this.ephemeralId = localNode.getEphemeralId();
        this.address = localNode.getAddress().getAddress();
        this.port = localNode.getAddress().getPort();
        this.etcdClient = etcdClient;
        this.scheduler = createScheduler();
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

    public void start() {
        scheduler.scheduleAtFixedRate(this::publishHeartbeat, 0, heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Scheduler did not terminate in 5 seconds");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn("Scheduler interrupted", e);
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
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
            heartbeatData.put("timestamp", System.currentTimeMillis());
            heartbeatData.put("nodeName", nodeName);
            heartbeatData.put("nodeId", nodeId);
            heartbeatData.put("ephemeralId", ephemeralId);
            heartbeatData.put("address", address);
            heartbeatData.put("port", port);
            heartbeatData.put("heartbeatIntervalMillis", heartbeatIntervalMillis);
            heartbeatData.put("cpuUsedPercent", cpuPercent);
            heartbeatData.put("memoryUsedPercent", memoryPercent);
            heartbeatData.put("memoryMaxMB", memoryMax.getMb());
            heartbeatData.put("memoryUsedMB", memoryUsed.getMb());
            heartbeatData.put("heapMaxMB", heapMax.getMb());
            heartbeatData.put("heapUsedMB", heapUsed.getMb());
            heartbeatData.put("heapUsedPercent", heapUsedPercent);
            heartbeatData.put("diskTotalMB", diskTotalMB);
            heartbeatData.put("diskAvailableMB", diskAvailableMB);

            // Add node shard routing information
            try {
                ClusterState clusterState = clusterService.state();
                Map<String, List<Map<String, Object>>> nodeRoutingMap = getNodeRoutingMap(clusterState);
                heartbeatData.put("nodeRouting", nodeRoutingMap);
            } catch (Exception e) {
                logger.error("Failed to get node routing information", e);
            }

            // Publish to ETCD
            KV kvClient = etcdClient.getKVClient();

            // Convert Map to JSON using XContent
            ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
            try (XContentBuilder jsonBuilder = XContentType.JSON.contentBuilder(jsonStream)) {
                jsonBuilder.map(heartbeatData);
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
                    shardInfo.put("shardId", shardId);
                    String role;
                    if (shardRouting.primary()) {
                        role = "primary";
                    } else if (shardRouting.isSearchOnly()) {
                        role = "search_replica";
                    } else {
                        role = "replica";
                    }
                    shardInfo.put("role", role);
                    shardInfo.put("state", shardRouting.state().name());
                    shardInfo.put("relocating", shardRouting.relocating());
                    if (shardRouting.relocating()) {
                        shardInfo.put("relocatingNodeId", shardRouting.relocatingNodeId());
                    }
                    shardInfo.put("allocationId", shardRouting.allocationId().getId());
                    shardInfo.put("currentNodeId", shardRouting.currentNodeId());
                    shardInfo.put("currentNodeName", clusterState.nodes().get(shardRouting.currentNodeId()).getName());
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
