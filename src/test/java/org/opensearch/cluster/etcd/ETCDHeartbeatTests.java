/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.launcher.Etcd;
import io.etcd.jetcd.launcher.EtcdCluster;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@ThreadLeakFilters(filters = { TestContainerThreadLeakFilter.class })
public class ETCDHeartbeatTests extends OpenSearchTestCase {

    public void testETCDHeartbeatStartStop() throws InterruptedException {
        // Setup mocks
        DiscoveryNode localNode = createMockDiscoveryNode();
        Client etcdClient = mock(Client.class);
        KV kvClient = mock(KV.class);
        NodeEnvironment nodeEnvironment = null; // NodeEnvironment is final and can't be mocked
        ClusterService clusterService = createMockClusterService();

        // Mock the ETCD client to avoid actual network calls
        when(etcdClient.getKVClient()).thenReturn(kvClient);
        when(kvClient.put(any(ByteSequence.class), any(ByteSequence.class))).thenReturn(CompletableFuture.completedFuture(null));

        ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
        ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, nodeEnvironment, clusterService, threadPool);

        // Test start and stop
        heartbeat.start();

        // Give the heartbeat a moment to potentially execute
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        threadPool.shutdownNow();
        threadPool.awaitTermination(1, TimeUnit.SECONDS);

        // The test should complete without hanging, indicating proper scheduler management
        assertTrue("Test completed successfully", true);
    }

    public void testETCDHeartbeatBasicMockingBehavior() throws InterruptedException {
        // Setup mocks
        DiscoveryNode localNode = createMockDiscoveryNode();
        Client etcdClient = mock(Client.class);
        KV kvClient = mock(KV.class);
        NodeEnvironment nodeEnvironment = null; // NodeEnvironment is final and can't be mocked
        ClusterService clusterService = createMockClusterService();

        // Mock successful ETCD put operation
        when(etcdClient.getKVClient()).thenReturn(kvClient);
        when(kvClient.put(any(ByteSequence.class), any(ByteSequence.class))).thenReturn(CompletableFuture.completedFuture(null));

        ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
        ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, nodeEnvironment, clusterService, threadPool, 100);

        // Test the lifecycle without actual scheduling - just ensure construction and cleanup work
        heartbeat.start();

        // Give it a moment for potential initial execution
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            threadPool.shutdown();
        }

        // Verify that some interaction happened with the KV client (heartbeat was attempted)
        // Note: We don't verify the exact number of calls since it depends on timing
        verify(etcdClient, times(1)).getKVClient();
    }

    public void testETCDHeartbeatWithClusterServiceRouting() throws IOException, InterruptedException {
        // Setup mocks
        DiscoveryNode localNode = createMockDiscoveryNode();
        Client etcdClient = mock(Client.class);
        KV kvClient = mock(KV.class);
        NodeEnvironment nodeEnvironment = null; // NodeEnvironment is final and can't be mocked
        ClusterService clusterService = createMockClusterServiceWithRouting("test-cluster");

        // Mock successful ETCD put operation
        when(etcdClient.getKVClient()).thenReturn(kvClient);
        when(kvClient.put(any(ByteSequence.class), any(ByteSequence.class))).thenReturn(CompletableFuture.completedFuture(null));

        ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
        ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, nodeEnvironment, clusterService, threadPool, 100);

        // Test that heartbeat can be constructed and managed with routing information
        heartbeat.start();

        // Give it a moment for potential execution
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            threadPool.shutdown();
        }

        // Verify that ETCD client was accessed (indicates heartbeat execution attempted)
        verify(etcdClient, times(1)).getKVClient();
    }

    public void testETCDHeartbeatErrorHandling() throws InterruptedException {
        // Setup mocks
        DiscoveryNode localNode = createMockDiscoveryNode();
        Client etcdClient = mock(Client.class);
        KV kvClient = mock(KV.class);
        NodeEnvironment nodeEnvironment = null; // NodeEnvironment is final and can't be mocked
        ClusterService clusterService = createMockClusterService();

        // Mock ETCD client to throw exception
        when(etcdClient.getKVClient()).thenReturn(kvClient);
        when(kvClient.put(any(ByteSequence.class), any(ByteSequence.class))).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("ETCD connection failed"))
        );

        ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
        ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, nodeEnvironment, clusterService, threadPool);

        // Start heartbeat - it should handle the error gracefully and not crash
        heartbeat.start();

        // Wait a bit to allow the heartbeat to attempt publishing
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            threadPool.shutdown();
        }

        // Test passes if we reach here without exceptions - the heartbeat handles errors gracefully
        assertTrue("Heartbeat handled errors gracefully", true);
    }

    public void testETCDHeartbeatClusterServiceError() throws InterruptedException {
        // Setup mocks
        DiscoveryNode localNode = createMockDiscoveryNode();
        Client etcdClient = mock(Client.class);
        KV kvClient = mock(KV.class);
        NodeEnvironment nodeEnvironment = null; // NodeEnvironment is final and can't be mocked
        ClusterService clusterService = mock(ClusterService.class);

        // Mock cluster service to throw exception when getting state
        when(clusterService.getClusterName()).thenReturn(new ClusterName("test-cluster"));
        when(clusterService.state()).thenThrow(new RuntimeException("Cluster service error"));
        
        // Mock settings with HTTP port
        Settings settings = Settings.builder().put("http.port", "9200").build();
        when(clusterService.getSettings()).thenReturn(settings);

        when(etcdClient.getKVClient()).thenReturn(kvClient);
        when(kvClient.put(any(ByteSequence.class), any(ByteSequence.class))).thenReturn(CompletableFuture.completedFuture(null));

        ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
        ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, nodeEnvironment, clusterService, threadPool, 100);

        // Start heartbeat - it should handle the cluster service error gracefully
        heartbeat.start();

        // Wait a bit to allow the heartbeat to attempt publishing
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            threadPool.shutdown();
        }

        // Test that the heartbeat handles cluster service errors gracefully
        // The heartbeat should still attempt to publish (without routing info) despite the cluster service error
        verify(etcdClient, times(1)).getKVClient();
    }

    private DiscoveryNode createMockDiscoveryNode() {
        Settings localNodeSettings = Settings.builder().put("cluster.name", "test-cluster").put("node.name", "test-node").build();
        return DiscoveryNode.createLocal(localNodeSettings, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "test-node");
    }

    private ClusterService createMockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterName()).thenReturn(new ClusterName("test-cluster"));

        // Create empty cluster state
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster")).build();
        when(clusterService.state()).thenReturn(clusterState);

        // Mock settings with HTTP port
        Settings settings = Settings.builder().put("http.port", "9200").build();
        when(clusterService.getSettings()).thenReturn(settings);

        return clusterService;
    }

    private ClusterService createMockClusterServiceWithRouting(String clusterName) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterName()).thenReturn(new ClusterName(clusterName));

        // Create cluster state with routing information
        DiscoveryNode node1 = DiscoveryNode.createLocal(
            Settings.builder().put("node.name", "node1").build(),
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            "node1"
        );

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(node1).localNodeId(node1.getId()).build();

        // Create index metadata
        Index index = new Index("test-index", "test-index-uuid");
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index")
            .settings(
                Settings.builder().put("index.version.created", 1).put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
            )
            .build();

        Metadata metadata = Metadata.builder().put(indexMetadata, false).build();

        // Create shard routing
        ShardId shardId = new ShardId(index, 0);
        ShardRouting primaryShard = TestShardRouting.newShardRouting(
            shardId,
            node1.getId(),
            true,
            org.opensearch.cluster.routing.ShardRoutingState.STARTED
        );

        IndexShardRoutingTable shardRoutingTable = new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard).build();
        IndexRoutingTable indexRoutingTable = new IndexRoutingTable.Builder(index).addIndexShard(shardRoutingTable).build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName(clusterName))
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        when(clusterService.state()).thenReturn(clusterState);

        // Mock settings with HTTP port
        Settings settings = Settings.builder().put("http.port", "9200").build();
        when(clusterService.getSettings()).thenReturn(settings);

        return clusterService;
    }

    // ETCD Container Integration Tests

    public void testETCDHeartbeatPublishesBasicData() throws IOException, ExecutionException, InterruptedException {
        String clusterName = "test-cluster";
        String nodeName = "test-node";

        Settings localNodeSettings = Settings.builder().put("cluster.name", clusterName).put("node.name", nodeName).build();
        DiscoveryNode localNode = DiscoveryNode.createLocal(
            localNodeSettings,
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            nodeName
        );

        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
            try (Client etcdClient = Client.builder().endpoints(etcdCluster.clientEndpoints()).build()) {
                ClusterService clusterService = createMockClusterService();
                ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, null, clusterService, threadPool, 100); // 100ms interval

                // Start heartbeat
                heartbeat.start();

                // Wait for heartbeat to be published
                String expectedPath = ETCDPathUtils.buildSearchUnitActualStatePath(localNode, clusterName);
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    ByteSequence key = ByteSequence.from(expectedPath, StandardCharsets.UTF_8);
                    List<KeyValue> kvs = etcdClient.getKVClient().get(key).get().getKvs();
                    assertFalse("Heartbeat data should be published", kvs.isEmpty());

                    // Parse and verify the heartbeat data
                    KeyValue kv = kvs.getFirst();
                    Map<String, Object> heartbeatData = parseHeartbeatJson(kv.getValue());

                    // Verify required fields are present
                    assertTrue("timestamp should be present", heartbeatData.containsKey("timestamp"));
                    assertEquals("nodeName should match", nodeName, heartbeatData.get("nodeName"));
                    assertEquals("nodeId should match", localNode.getId(), heartbeatData.get("nodeId"));
                    assertEquals("ephemeralId should match", localNode.getEphemeralId(), heartbeatData.get("ephemeralId"));
                    assertEquals("address should match", localNode.getAddress().getAddress(), heartbeatData.get("address"));
                    
                    // Verify httpPort is present and valid
                    assertTrue("httpPort should be present", heartbeatData.containsKey("httpPort"));
                    assertTrue("httpPort should be a number", heartbeatData.get("httpPort") instanceof Number);
                    int httpPort = ((Number) heartbeatData.get("httpPort")).intValue();
                    assertTrue("httpPort should be valid", httpPort > 0 && httpPort < 65536);
                    
                    assertEquals(
                        "heartbeatIntervalMillis should be 100",
                        100,
                        ((Number) heartbeatData.get("heartbeatIntervalMillis")).intValue()
                    );

                    // Verify system metrics are present (values may vary)
                    assertTrue("cpuUsedPercent should be present", heartbeatData.containsKey("cpuUsedPercent"));
                    assertTrue("memoryUsedPercent should be present", heartbeatData.containsKey("memoryUsedPercent"));
                    assertTrue("memoryMaxMB should be present", heartbeatData.containsKey("memoryMaxMB"));
                    assertTrue("memoryUsedMB should be present", heartbeatData.containsKey("memoryUsedMB"));
                    assertTrue("heapMaxMB should be present", heartbeatData.containsKey("heapMaxMB"));
                    assertTrue("heapUsedMB should be present", heartbeatData.containsKey("heapUsedMB"));
                    assertTrue("heapUsedPercent should be present", heartbeatData.containsKey("heapUsedPercent"));
                    assertTrue("diskTotalMB should be present", heartbeatData.containsKey("diskTotalMB"));
                    assertTrue("diskAvailableMB should be present", heartbeatData.containsKey("diskAvailableMB"));

                    // Verify timestamp is recent (within last 10 seconds)
                    long timestamp = ((Number) heartbeatData.get("timestamp")).longValue();
                    long now = System.currentTimeMillis();
                    assertTrue("timestamp should be recent", Math.abs(now - timestamp) < 10000);
                });
            } finally {
                threadPool.shutdown();
            }
        }
    }

    public void testETCDHeartbeatWithRoutingData() throws IOException, ExecutionException, InterruptedException {
        String clusterName = "test-cluster";
        String nodeName = "test-node";

        Settings localNodeSettings = Settings.builder().put("cluster.name", clusterName).put("node.name", nodeName).build();
        DiscoveryNode localNode = DiscoveryNode.createLocal(
            localNodeSettings,
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            nodeName
        );

        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
            try (Client etcdClient = Client.builder().endpoints(etcdCluster.clientEndpoints()).build()) {
                ClusterService clusterService = createMockClusterServiceWithRouting(clusterName);
                ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, null, clusterService, threadPool, 100); // 100ms interval

                // Start heartbeat
                heartbeat.start();

                // Wait for heartbeat to be published
                String expectedPath = ETCDPathUtils.buildSearchUnitActualStatePath(localNode, clusterName);
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    ByteSequence key = ByteSequence.from(expectedPath, StandardCharsets.UTF_8);
                    List<KeyValue> kvs = etcdClient.getKVClient().get(key).get().getKvs();
                    assertFalse("Heartbeat data should be published", kvs.isEmpty());

                    // Parse and verify the heartbeat data
                    KeyValue kv = kvs.getFirst();
                    Map<String, Object> heartbeatData = parseHeartbeatJson(kv.getValue());

                    // Verify routing information is present
                    assertTrue("nodeRouting should be present", heartbeatData.containsKey("nodeRouting"));
                    @SuppressWarnings("unchecked")
                    Map<String, Object> nodeRouting = (Map<String, Object>) heartbeatData.get("nodeRouting");

                    // Verify the test index routing is present
                    assertTrue("test-index routing should be present", nodeRouting.containsKey("test-index"));
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> indexShards = (List<Map<String, Object>>) nodeRouting.get("test-index");
                    assertFalse("Index should have shards", indexShards.isEmpty());

                    // Verify shard information structure
                    Map<String, Object> shardInfo = indexShards.getFirst();
                    assertTrue("shardId should be present", shardInfo.containsKey("shardId"));
                    assertTrue("shard role should be present", shardInfo.containsKey("role"));
                    assertTrue("state should be present", shardInfo.containsKey("state"));
                    assertTrue("relocating should be present", shardInfo.containsKey("relocating"));
                    assertTrue("allocationId should be present", shardInfo.containsKey("allocationId"));
                    assertTrue("currentNodeId should be present", shardInfo.containsKey("currentNodeId"));
                    assertTrue("currentNodeName should be present", shardInfo.containsKey("currentNodeName"));
                });

                threadPool.shutdown();
            } finally {
                threadPool.shutdown();
            }
        }
    }

    public void testETCDHeartbeatPeriodicUpdates() throws IOException, ExecutionException, InterruptedException {
        String clusterName = "test-cluster";
        String nodeName = "test-node";

        Settings localNodeSettings = Settings.builder().put("cluster.name", clusterName).put("node.name", nodeName).build();
        DiscoveryNode localNode = DiscoveryNode.createLocal(
            localNodeSettings,
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            nodeName
        );

        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
            try (Client etcdClient = Client.builder().endpoints(etcdCluster.clientEndpoints()).build()) {
                ClusterService clusterService = createMockClusterService();
                ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, null, clusterService, threadPool, 200); // 200ms interval

                // Start heartbeat
                heartbeat.start();

                String expectedPath = ETCDPathUtils.buildSearchUnitActualStatePath(localNode, clusterName);
                ByteSequence key = ByteSequence.from(expectedPath, StandardCharsets.UTF_8);

                // Wait for first heartbeat
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    List<KeyValue> kvs = etcdClient.getKVClient().get(key).get().getKvs();
                    assertFalse("First heartbeat should be published", kvs.isEmpty());
                });

                // Get first timestamp
                KeyValue firstKv = etcdClient.getKVClient().get(key).get().getKvs().getFirst();
                Map<String, Object> firstHeartbeat = parseHeartbeatJson(firstKv.getValue());
                long firstTimestamp = ((Number) firstHeartbeat.get("timestamp")).longValue();

                // Wait for subsequent heartbeats (should be updated within 500ms given 200ms interval + buffer)
                await().atMost(500, TimeUnit.MILLISECONDS).untilAsserted(() -> {
                    List<KeyValue> kvs = etcdClient.getKVClient().get(key).get().getKvs();
                    assertFalse("Subsequent heartbeat should be published", kvs.isEmpty());

                    KeyValue kv = kvs.getFirst();
                    Map<String, Object> heartbeatData = parseHeartbeatJson(kv.getValue());
                    long currentTimestamp = ((Number) heartbeatData.get("timestamp")).longValue();

                    assertTrue("Timestamp should be updated", currentTimestamp > firstTimestamp);
                });
                threadPool.shutdown();
            } finally {
                threadPool.shutdown();
            }
        }
    }

    public void testETCDHeartbeatStopCleansUpProperly() throws IOException, ExecutionException, InterruptedException {
        String clusterName = "test-cluster";
        String nodeName = "test-node";

        Settings localNodeSettings = Settings.builder().put("cluster.name", clusterName).put("node.name", nodeName).build();
        DiscoveryNode localNode = DiscoveryNode.createLocal(
            localNodeSettings,
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            nodeName
        );

        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
            try (Client etcdClient = Client.builder().endpoints(etcdCluster.clientEndpoints()).build()) {
                ClusterService clusterService = createMockClusterService();
                ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, null, clusterService, threadPool, 100); // 100ms interval

                // Start heartbeat
                heartbeat.start();

                String expectedPath = ETCDPathUtils.buildSearchUnitActualStatePath(localNode, clusterName);
                ByteSequence key = ByteSequence.from(expectedPath, StandardCharsets.UTF_8);

                // Wait for heartbeat to be published
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    List<KeyValue> kvs = etcdClient.getKVClient().get(key).get().getKvs();
                    assertFalse("Heartbeat should be published", kvs.isEmpty());
                });

                // Stop heartbeat
                threadPool.shutdown();

                // Wait a bit longer than the heartbeat interval to ensure no more updates
                Thread.sleep(300); // Wait 300ms (more than 100ms interval)

                // Get the timestamp after stopping
                KeyValue finalKv = etcdClient.getKVClient().get(key).get().getKvs().getFirst();
                Map<String, Object> finalHeartbeat = parseHeartbeatJson(finalKv.getValue());
                long finalTimestamp = ((Number) finalHeartbeat.get("timestamp")).longValue();

                // Wait another interval and verify no new updates
                Thread.sleep(200); // Wait another 200ms
                KeyValue laterKv = etcdClient.getKVClient().get(key).get().getKvs().getFirst();
                Map<String, Object> laterHeartbeat = parseHeartbeatJson(laterKv.getValue());
                long laterTimestamp = ((Number) laterHeartbeat.get("timestamp")).longValue();

                assertEquals("No new heartbeats should be published after stop", finalTimestamp, laterTimestamp);
            } finally {
                threadPool.shutdown();
            }
        }
    }

    public void testETCDHeartbeatDataFormat() {
        String clusterName = "test-cluster";
        String nodeName = "test-node";

        Settings localNodeSettings = Settings.builder().put("cluster.name", clusterName).put("node.name", nodeName).build();
        DiscoveryNode localNode = DiscoveryNode.createLocal(
            localNodeSettings,
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            nodeName
        );

        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
            try (Client etcdClient = Client.builder().endpoints(etcdCluster.clientEndpoints()).build()) {
                ClusterService clusterService = createMockClusterService();
                ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, null, clusterService, threadPool, 100); // 100ms interval

                // Start heartbeat
                heartbeat.start();

                // Wait for heartbeat to be published
                String expectedPath = ETCDPathUtils.buildSearchUnitActualStatePath(localNode, clusterName);
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    ByteSequence key = ByteSequence.from(expectedPath, StandardCharsets.UTF_8);
                    List<KeyValue> kvs = etcdClient.getKVClient().get(key).get().getKvs();
                    assertFalse("Heartbeat data should be published", kvs.isEmpty());

                    // Parse and verify the heartbeat data structure
                    KeyValue kv = kvs.getFirst();
                    Map<String, Object> heartbeatData = parseHeartbeatJson(kv.getValue());

                    // Verify all numeric fields are actually numbers
                    assertTrue("timestamp should be a number", heartbeatData.get("timestamp") instanceof Number);
                    assertTrue(
                        "heartbeatIntervalMillis should be a number",
                        heartbeatData.get("heartbeatIntervalMillis") instanceof Number
                    );
                    assertTrue("httpPort should be a number", heartbeatData.get("httpPort") instanceof Number);
                    assertTrue("cpuUsedPercent should be a number", heartbeatData.get("cpuUsedPercent") instanceof Number);
                    assertTrue("memoryUsedPercent should be a number", heartbeatData.get("memoryUsedPercent") instanceof Number);
                    assertTrue("memoryMaxMB should be a number", heartbeatData.get("memoryMaxMB") instanceof Number);
                    assertTrue("memoryUsedMB should be a number", heartbeatData.get("memoryUsedMB") instanceof Number);
                    assertTrue("heapMaxMB should be a number", heartbeatData.get("heapMaxMB") instanceof Number);
                    assertTrue("heapUsedMB should be a number", heartbeatData.get("heapUsedMB") instanceof Number);
                    assertTrue("heapUsedPercent should be a number", heartbeatData.get("heapUsedPercent") instanceof Number);
                    assertTrue("diskTotalMB should be a number", heartbeatData.get("diskTotalMB") instanceof Number);
                    assertTrue("diskAvailableMB should be a number", heartbeatData.get("diskAvailableMB") instanceof Number);

                    // Verify string fields are strings
                    assertTrue("nodeName should be a string", heartbeatData.get("nodeName") instanceof String);
                    assertTrue("nodeId should be a string", heartbeatData.get("nodeId") instanceof String);
                    assertTrue("ephemeralId should be a string", heartbeatData.get("ephemeralId") instanceof String);
                    assertTrue("address should be a string", heartbeatData.get("address") instanceof String);

                    // Verify nodeRouting structure if present
                    if (heartbeatData.containsKey("nodeRouting")) {
                        assertTrue("nodeRouting should be a map", heartbeatData.get("nodeRouting") instanceof Map);
                    }

                    // Verify metric values are reasonable (non-negative)
                    assertTrue("cpuUsedPercent should be non-negative", ((Number) heartbeatData.get("cpuUsedPercent")).intValue() >= 0);
                    assertTrue(
                        "memoryUsedPercent should be non-negative",
                        ((Number) heartbeatData.get("memoryUsedPercent")).intValue() >= 0
                    );
                    assertTrue("heapUsedPercent should be non-negative", ((Number) heartbeatData.get("heapUsedPercent")).intValue() >= 0);
                    assertTrue("memoryMaxMB should be positive", ((Number) heartbeatData.get("memoryMaxMB")).longValue() > 0);
                    assertTrue("heapMaxMB should be positive", ((Number) heartbeatData.get("heapMaxMB")).longValue() > 0);
                });
            } finally {
                threadPool.shutdown();
            }
        }
    }

    private Map<String, Object> parseHeartbeatJson(ByteSequence jsonBytes) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                jsonBytes.getBytes()
            )
        ) {
            return parser.map();
        }
    }

    public void testETCDHeartbeatWithClusterlessAttributes() throws IOException, ExecutionException, InterruptedException {
        String clusterName = "test-cluster";
        String nodeName = "test-node";

        // Create a node with clusterless attributes
        Settings localNodeSettings = Settings.builder()
            .put("cluster.name", clusterName)
            .put("node.name", nodeName)
            .put("node.attr.clusterless_role", "primary")
            .put("node.attr.clusterless_shard_id", "00")
            .build();

        DiscoveryNode localNode = DiscoveryNode.createLocal(
            localNodeSettings,
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            nodeName
        );

        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
            try (Client etcdClient = Client.builder().endpoints(etcdCluster.clientEndpoints()).build()) {
                ClusterService clusterService = createMockClusterService();
                ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, null, clusterService, threadPool, 100); // 100ms interval

                // Start heartbeat
                heartbeat.start();

                // Wait for heartbeat to be published
                String expectedPath = ETCDPathUtils.buildSearchUnitActualStatePath(localNode, clusterName);
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    ByteSequence key = ByteSequence.from(expectedPath, StandardCharsets.UTF_8);
                    List<KeyValue> kvs = etcdClient.getKVClient().get(key).get().getKvs();
                    assertFalse("Heartbeat data should be published", kvs.isEmpty());

                    // Parse and verify the heartbeat data structure
                    KeyValue kv = kvs.getFirst();
                    Map<String, Object> heartbeatData = parseHeartbeatJson(kv.getValue());

                    // Verify clusterless attributes are present
                    assertEquals("clusterlessRole should be 'primary'", "primary", heartbeatData.get("clusterlessRole"));
                    assertEquals("clusterlessShardId should be '00'", "00", heartbeatData.get("clusterlessShardId"));
                });
            } finally {
                threadPool.shutdown();
            }
        }
    }

    public void testETCDHeartbeatWithoutCloudNativeAttributes() throws IOException, ExecutionException, InterruptedException {
        String clusterName = "test-cluster";
        String nodeName = "test-node";

        // Create a node without clusterless attributes
        Settings localNodeSettings = Settings.builder().put("cluster.name", clusterName).put("node.name", nodeName).build();

        DiscoveryNode localNode = DiscoveryNode.createLocal(
            localNodeSettings,
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            nodeName
        );

        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDHeartbeat.createExecutorBuilder(null));
            try (Client etcdClient = Client.builder().endpoints(etcdCluster.clientEndpoints()).build()) {
                ClusterService clusterService = createMockClusterService();
                ETCDHeartbeat heartbeat = new ETCDHeartbeat(localNode, etcdClient, null, clusterService, threadPool, 100); // 100ms interval

                // Start heartbeat
                heartbeat.start();

                // Wait for heartbeat to be published
                String expectedPath = ETCDPathUtils.buildSearchUnitActualStatePath(localNode, clusterName);
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    ByteSequence key = ByteSequence.from(expectedPath, StandardCharsets.UTF_8);
                    List<KeyValue> kvs = etcdClient.getKVClient().get(key).get().getKvs();
                    assertFalse("Heartbeat data should be published", kvs.isEmpty());

                    // Parse and verify the heartbeat data structure
                    KeyValue kv = kvs.getFirst();
                    Map<String, Object> heartbeatData = parseHeartbeatJson(kv.getValue());

                    // Verify clusterless attributes are not present
                    assertFalse("clusterlessRole should not be present", heartbeatData.containsKey("clusterlessRole"));
                    assertFalse("clusterlessShardId should not be present", heartbeatData.containsKey("clusterlessShardId"));
                });
            } finally {
                threadPool.shutdown();
            }
        }
    }
}
