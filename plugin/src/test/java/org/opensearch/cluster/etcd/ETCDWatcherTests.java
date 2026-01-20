/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.Etcd;
import io.etcd.jetcd.launcher.EtcdCluster;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.etcd.changeapplier.CoordinatorNodeState;
import org.opensearch.cluster.etcd.changeapplier.DataNodeState;
import org.opensearch.cluster.etcd.changeapplier.NodeState;
import org.opensearch.cluster.etcd.changeapplier.NodeStateApplier;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@ThreadLeakFilters(filters = { TestContainerThreadLeakFilter.class })
public class ETCDWatcherTests extends OpenSearchTestCase {
    private final IndicesService indicesService = mock(IndicesService.class);

    private static class MockNodeStateApplier implements NodeStateApplier {
        private NodeState appliedNodeState = null;

        @Override
        public void applyNodeState(String source, NodeState nodeState) {
            appliedNodeState = nodeState;
        }

        @Override
        public void removeNode(String source, DiscoveryNode localNode) {
            appliedNodeState = null;
        }
    }

    public void testETCDWatcherDataNode() throws IOException, ExecutionException, InterruptedException {

        String clusterName = "test-cluster";
        String nodeName = "test-node";
        String indexName = "test-index";
        Settings localNodeSettings = Settings.builder().put("cluster.name", clusterName).put("node.name", nodeName).build();
        DiscoveryNode localNode = DiscoveryNode.createLocal(
            localNodeSettings,
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            nodeName
        );

        MockNodeStateApplier mockNodeStateApplier = new MockNodeStateApplier();
        String configPath = ETCDPathUtils.buildSearchUnitGoalStatePath(localNode, clusterName);
        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDWatcher.createExecutorBuilder(null));
            try (
                ETCDClientHolder etcdClientHolder = new ETCDClientHolder(
                    () -> Client.builder().endpoints(etcdCluster.clientEndpoints()).build()
                );
                ETCDWatcher etcdWatcher = new ETCDWatcher(
                    localNode,
                    ByteSequence.from(configPath, StandardCharsets.UTF_8),
                    mockNodeStateApplier,
                    etcdClientHolder,
                    threadPool,
                    clusterName
                )
            ) {
                assertNull(mockNodeStateApplier.appliedNodeState);

                // Set up index metadata
                String mappingPath = ETCDPathUtils.buildIndexMappingsPath(clusterName, indexName);
                String settingsPath = ETCDPathUtils.buildIndexSettingsPath(clusterName, indexName);
                etcdPut(etcdClientHolder, mappingPath, "{\"properties\": {\"field1\": {\"type\": \"text\"}}}");
                etcdPut(etcdClientHolder, settingsPath, """
                    {
                       "index": {
                           "number_of_shards": "2",
                           "number_of_replicas": "0"
                       }
                    }
                    """);

                // Add the current node as a data node
                etcdPut(etcdClientHolder, configPath, """
                    {
                       "local_shards": {
                         "test-index": {
                           "0": "PRIMARY"
                         }
                       }
                    }
                    """);
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertNotNull(mockNodeStateApplier.appliedNodeState);
                    assertTrue(mockNodeStateApplier.appliedNodeState instanceof DataNodeState);
                    DataNodeState dataNodeState = (DataNodeState) mockNodeStateApplier.appliedNodeState;
                    ClusterState clusterState = dataNodeState.buildClusterState(ClusterState.EMPTY_STATE, indicesService);
                    assertTrue(clusterState.metadata().hasIndex(indexName));
                    assertEquals(2, clusterState.metadata().index(indexName).getNumberOfShards());
                    assertEquals(1, clusterState.routingTable().index(indexName).shards().size());
                    ShardRouting primaryShardRouting = clusterState.routingTable().index(indexName).shard(0).primaryShard();
                    assertTrue(primaryShardRouting.assignedToNode());
                    assertEquals(localNode.getId(), primaryShardRouting.currentNodeId());
                });

                // Remove the config to trigger removal of node state
                etcdClientHolder.getClient().getKVClient().delete(ByteSequence.from(configPath, StandardCharsets.UTF_8)).get();

                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> { assertNull(mockNodeStateApplier.appliedNodeState); });

            } finally {
                threadPool.shutdown();
            }
        }
    }

    public void testETCDWatcherCoordinatorNode() throws IOException, ExecutionException, InterruptedException {
        String clusterName = "test-cluster";
        String nodeName = "test-coordinator-node";
        String indexName = "test-index";
        Settings localNodeSettings = Settings.builder().put("cluster.name", clusterName).put("node.name", nodeName).build();
        DiscoveryNode localNode = DiscoveryNode.createLocal(
            localNodeSettings,
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            nodeName
        );

        MockNodeStateApplier mockNodeStateApplier = new MockNodeStateApplier();
        String configPath = ETCDPathUtils.buildSearchUnitGoalStatePath(localNode, clusterName);

        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDWatcher.createExecutorBuilder(null));
            try (
                ETCDClientHolder etcdClientHolder = new ETCDClientHolder(
                    () -> Client.builder().endpoints(etcdCluster.clientEndpoints()).build()
                );
                ETCDWatcher etcdWatcher = new ETCDWatcher(
                    localNode,
                    ByteSequence.from(configPath, StandardCharsets.UTF_8),
                    mockNodeStateApplier,
                    etcdClientHolder,
                    threadPool,
                    clusterName
                )
            ) {
                assertNull(mockNodeStateApplier.appliedNodeState);

                // Set up health information for remote nodes
                String remoteNodeName1 = "remote-node-1";
                String remoteNodeName2 = "remote-node-2";
                String healthPath1 = ETCDPathUtils.buildSearchUnitActualStatePath(clusterName, remoteNodeName1);
                String healthPath2 = ETCDPathUtils.buildSearchUnitActualStatePath(clusterName, remoteNodeName2);

                etcdPut(etcdClientHolder, healthPath1, """
                    {
                        "nodeId": "remote-node-id-1",
                        "ephemeralId": "ephemeral-id-1",
                        "address": "192.168.1.1",
                        "transportPort": 9300,
                        "timestamp": 1750099493841,
                        "heartbeatIntervalSeconds": 5
                    }
                    """);
                etcdPut(etcdClientHolder, healthPath2, """
                    {
                        "nodeId": "remote-node-id-2",
                        "ephemeralId": "ephemeral-id-2",
                        "address": "192.168.1.2",
                        "transportPort": 9300,
                        "timestamp": 1750099493841,
                        "heartbeatIntervalSeconds": 5
                    }
                    """);

                // Add coordinator node configuration with remote_shards and aliases
                etcdPut(etcdClientHolder, configPath, """
                    {
                       "remote_shards": {
                         "indices": {
                           "test-index": {
                             "uuid": "test-index-uuid",
                             "shard_routing": [
                               [
                                 {"node_name": "remote-node-1", "primary": true},
                                 {"node_name": "remote-node-2"}
                               ],
                               [
                                 {"node_name": "remote-node-2", "primary": true}
                               ]
                             ]
                           }
                         },
                         "aliases": {
                           "logs-current": "test-index",
                           "logs-recent": ["test-index"]
                         }
                       }
                    }
                    """);

                // Verify coordinator node state is applied
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertNotNull(mockNodeStateApplier.appliedNodeState);
                    assertTrue(mockNodeStateApplier.appliedNodeState instanceof CoordinatorNodeState);
                    CoordinatorNodeState coordinatorNodeState = (CoordinatorNodeState) mockNodeStateApplier.appliedNodeState;
                    ClusterState clusterState = coordinatorNodeState.buildClusterState(ClusterState.EMPTY_STATE, indicesService);

                    // Verify the coordinator node sees the index
                    assertTrue(clusterState.metadata().hasIndex(indexName));
                    assertEquals(2, clusterState.metadata().index(indexName).getNumberOfShards());

                    // Verify routing table has the correct shard assignments
                    assertEquals(2, clusterState.routingTable().index(indexName).shards().size());

                    // Verify shard 0 has primary on remote-node-1 and replica on remote-node-2
                    ShardRouting shard0Primary = clusterState.routingTable().index(indexName).shard(0).primaryShard();
                    assertTrue(shard0Primary.primary());
                    assertEquals("remote-node-id-1", shard0Primary.currentNodeId());
                    assertEquals(1, clusterState.routingTable().index(indexName).shard(0).replicaShards().size());
                    ShardRouting shard0Replica = clusterState.routingTable().index(indexName).shard(0).replicaShards().getFirst();
                    assertEquals("remote-node-id-2", shard0Replica.currentNodeId());

                    // Verify shard 1 has primary on remote-node-2
                    ShardRouting shard1Primary = clusterState.routingTable().index(indexName).shard(1).primaryShard();
                    assertTrue(shard1Primary.primary());
                    assertEquals("remote-node-id-2", shard1Primary.currentNodeId());

                    // Verify remote nodes are in the cluster state
                    assertNotNull(clusterState.nodes().get("remote-node-id-1"));
                    assertNotNull(clusterState.nodes().get("remote-node-id-2"));

                    // Verify aliases are present in the index metadata
                    var indexMetadata = clusterState.metadata().index(indexName);
                    assertTrue(indexMetadata.getAliases().containsKey("logs-current"));
                    assertTrue(indexMetadata.getAliases().containsKey("logs-recent"));
                });

                // Remove the coordinator node config to trigger removal
                etcdClientHolder.getClient().getKVClient().delete(ByteSequence.from(configPath, StandardCharsets.UTF_8)).get();

                // Verify coordinator node state is removed
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> { assertNull(mockNodeStateApplier.appliedNodeState); });
            } finally {
                threadPool.shutdown();
            }
        }
    }

    public void testETCDWatcherCoordinatorNodeWithRemoteClusters() throws IOException, ExecutionException, InterruptedException {
        String clusterName = "test-cluster";
        String nodeName = "test-coordinator-node-ccs";
        Settings localNodeSettings = Settings.builder().put("cluster.name", clusterName).put("node.name", nodeName).build();
        DiscoveryNode localNode = DiscoveryNode.createLocal(
            localNodeSettings,
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            nodeName
        );

        MockNodeStateApplier mockNodeStateApplier = new MockNodeStateApplier();
        String configPath = ETCDPathUtils.buildSearchUnitGoalStatePath(localNode, clusterName);

        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            ThreadPool threadPool = new TestThreadPool(localNode.getName(), ETCDWatcher.createExecutorBuilder(null));
            try (
                ETCDClientHolder etcdClientHolder = new ETCDClientHolder(
                    () -> Client.builder().endpoints(etcdCluster.clientEndpoints()).build()
                );
                ETCDWatcher etcdWatcher = new ETCDWatcher(
                    localNode,
                    ByteSequence.from(configPath, StandardCharsets.UTF_8),
                    mockNodeStateApplier,
                    etcdClientHolder,
                    threadPool,
                    clusterName
                )
            ) {
                // Add coordinator node configuration with remote_clusters
                etcdPut(etcdClientHolder, configPath, """
                        {
                          "remote_shards": {
                            "indices": {},
                            "remote_clusters": {
                              "cluster_one": {
                                "seeds": [
                                  "10.0.1.10:9300",
                                  "10.0.1.11:9300"
                                ]
                              },
                              "cluster_two": {
                                "seeds": [
                                  "10.0.2.20:9300"
                                ]
                              },
                              "cluster_three": {
                                "mode": "proxy",
                                "proxy_address": "remote-cluster-proxy:8030"
                               }
                            }
                          }
                        }
                    """);

                // Verify coordinator node state is applied and contains remote cluster settings
                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertNotNull(mockNodeStateApplier.appliedNodeState);
                    assertTrue(mockNodeStateApplier.appliedNodeState instanceof CoordinatorNodeState);
                    CoordinatorNodeState coordinatorNodeState = (CoordinatorNodeState) mockNodeStateApplier.appliedNodeState;
                    ClusterState clusterState = coordinatorNodeState.buildClusterState(ClusterState.EMPTY_STATE, indicesService);

                    // Verify persistent settings contain the remote cluster configurations
                    Settings persistentSettings = clusterState.metadata().persistentSettings();
                    assertNotNull(persistentSettings);
                    List<String> clusterOneSeeds = persistentSettings.getAsList("cluster.remote.cluster_one.seeds");
                    List<String> clusterTwoSeeds = persistentSettings.getAsList("cluster.remote.cluster_two.seeds");
                    String clusterThreeMode = persistentSettings.get("cluster.remote.cluster_three.mode");
                    String clusterThreeProxyAddress = persistentSettings.get("cluster.remote.cluster_three.proxy_address");

                    assertEquals(2, clusterOneSeeds.size());
                    assertEquals("10.0.1.10:9300", clusterOneSeeds.get(0));
                    assertEquals("10.0.1.11:9300", clusterOneSeeds.get(1));

                    assertEquals(1, clusterTwoSeeds.size());
                    assertEquals("10.0.2.20:9300", clusterTwoSeeds.get(0));

                    assertEquals("proxy", clusterThreeMode);
                    assertEquals("remote-cluster-proxy:8030", clusterThreeProxyAddress);

                });
            } finally {
                threadPool.shutdown();
            }
        }
    }

    private static void etcdPut(ETCDClientHolder etcdClientHolder, String key, String value) throws ExecutionException,
        InterruptedException {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        ByteSequence valueBytes = ByteSequence.from(value, StandardCharsets.UTF_8);
        etcdClientHolder.getClient().getKVClient().put(keyBytes, valueBytes).get();
    }

}
