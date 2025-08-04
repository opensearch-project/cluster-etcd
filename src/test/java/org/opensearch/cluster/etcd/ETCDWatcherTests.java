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
import org.opensearch.cluster.etcd.changeapplier.DataNodeState;
import org.opensearch.cluster.etcd.changeapplier.NodeState;
import org.opensearch.cluster.etcd.changeapplier.NodeStateApplier;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@ThreadLeakFilters(filters = { TestContainerThreadLeakFilter.class })
public class ETCDWatcherTests extends OpenSearchTestCase {

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

    public void testETCDWatcher() throws IOException, ExecutionException, InterruptedException {

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
        String configPath = ETCDPathUtils.buildSearchUnitConfigPath(clusterName, nodeName);
        try (EtcdCluster etcdCluster = Etcd.builder().withNodes(1).build()) {
            etcdCluster.start();
            try (
                Client etcdClient = Client.builder().endpoints(etcdCluster.clientEndpoints()).build();
                ETCDWatcher etcdWatcher = new ETCDWatcher(
                    localNode,
                    ByteSequence.from(configPath, StandardCharsets.UTF_8),
                    mockNodeStateApplier,
                    etcdClient,
                    clusterName
                )
            ) {
                assertNull(mockNodeStateApplier.appliedNodeState);

                // Set up index metadata
                String mappingPath = ETCDPathUtils.buildIndexMappingsPath(clusterName, indexName);
                String settingsPath = ETCDPathUtils.buildIndexSettingsPath(clusterName, indexName);
                etcdPut(etcdClient, mappingPath, "{\"properties\": {\"field1\": {\"type\": \"text\"}}}");
                etcdPut(etcdClient, settingsPath, """
                    {
                       "index": {
                           "number_of_shards": "2",
                           "number_of_replicas": "0"
                       }
                    }
                    """);

                // Add the current node as a data node
                etcdPut(etcdClient, configPath, """
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
                    ClusterState clusterState = dataNodeState.buildClusterState(ClusterState.EMPTY_STATE);
                    assertTrue(clusterState.metadata().hasIndex(indexName));
                    assertEquals(2, clusterState.metadata().index(indexName).getNumberOfShards());
                    assertEquals(1, clusterState.routingTable().index(indexName).shards().size());
                    ShardRouting primaryShardRouting = clusterState.routingTable().index(indexName).shard(0).primaryShard();
                    assertTrue(primaryShardRouting.assignedToNode());
                    assertEquals(localNode.getId(), primaryShardRouting.currentNodeId());
                });

                // Remove the config to trigger removal of node state
                etcdClient.getKVClient().delete(ByteSequence.from(configPath, StandardCharsets.UTF_8)).get();

                await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> { assertNull(mockNodeStateApplier.appliedNodeState); });

            }
        }
    }

    private static void etcdPut(Client etcdClient, String key, String value) throws ExecutionException, InterruptedException {
        ByteSequence keyBytes = ByteSequence.from(key, StandardCharsets.UTF_8);
        ByteSequence valueBytes = ByteSequence.from(value, StandardCharsets.UTF_8);
        etcdClient.getKVClient().put(keyBytes, valueBytes).get();
    }

}
