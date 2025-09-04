/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import com.google.protobuf.ByteString;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.api.KeyValue;
import io.etcd.jetcd.api.RangeResponse;
import io.etcd.jetcd.kv.GetResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.etcd.changeapplier.DataNodeState;
import org.opensearch.cluster.etcd.changeapplier.NodeState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ETCDStateDeserializerTests extends OpenSearchTestCase {

    public void testDeserializeDataNodeState() throws IOException {
        String nodeConfiguration = """
            {
                "local_shards": {
                    "idx1": {
                        "0" : "PRIMARY",
                        "1" : "SEARCH_REPLICA"
                    }
                }
            }
            """;

        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.getId()).thenReturn("local-node-id");
        Client client = mock(Client.class);
        KV kvClient = mock(KV.class);
        when(client.getKVClient()).thenReturn(kvClient);
        ByteSequence idx1SettingsPath = ByteSequence.from(
            ETCDPathUtils.buildIndexSettingsPath("test-cluster", "idx1"),
            StandardCharsets.UTF_8
        );
        ByteSequence idx1MappingsPath = ByteSequence.from(
            ETCDPathUtils.buildIndexMappingsPath("test-cluster", "idx1"),
            StandardCharsets.UTF_8
        );

        RangeResponse idx1SettingsResponse = RangeResponse.newBuilder().addKvs(KeyValue.newBuilder().setValue(ByteString.copyFrom("""
            {
              "index": {
                "number_of_shards": "2",
                "number_of_replicas": "0",
                "uuid": "E8F2-ebqQ1-U4SL6NoPEyw",
                "version": {
                  "created": "137227827"
                }
              }
            }
            """, StandardCharsets.UTF_8)).build()).build();
        RangeResponse idx1MappingsResponse = RangeResponse.newBuilder().addKvs(KeyValue.newBuilder().setValue(ByteString.copyFrom("""
            {
              "properties": {
                "field1": {
                  "type": "text"
                },
                "field2": {
                  "type": "keyword"
                }
              }
            }
            """, StandardCharsets.UTF_8)).build()).build();

        when(kvClient.get(eq(idx1SettingsPath))).thenReturn(CompletableFuture.completedFuture(new GetResponse(idx1SettingsResponse, null)));
        when(kvClient.get(eq(idx1MappingsPath))).thenReturn(CompletableFuture.completedFuture(new GetResponse(idx1MappingsResponse, null)));

        NodeState nodeState = ETCDStateDeserializer.deserializeNodeState(
            localNode,
            ByteSequence.from(nodeConfiguration, StandardCharsets.UTF_8),
            client,
            "test-cluster",
            true
        );

        assertTrue(nodeState instanceof DataNodeState);
        DataNodeState dataNodeState = (DataNodeState) nodeState;
        ClusterState emptyClusterState = ClusterState.EMPTY_STATE;
        ClusterState clusterState = dataNodeState.buildClusterState(emptyClusterState);
        assertEquals(1, clusterState.getMetadata().indices().size());
        assertTrue(clusterState.getMetadata().hasIndex("idx1"));
    }

    public void testDocumentReplicationSetup() throws IOException {
        // Step 1: Create node state for node1 with a single primary shard
        String node1Configuration = """
            {
                "local_shards": {
                    "idx1": {
                        "0" : "PRIMARY"
                    }
                }
            }
            """;

        DiscoveryNode node1 = mock(DiscoveryNode.class);
        when(node1.getId()).thenReturn("node1-id");
        when(node1.getName()).thenReturn("node1");

        DiscoveryNode node2 = mock(DiscoveryNode.class);
        when(node2.getId()).thenReturn("node2-id");
        when(node2.getName()).thenReturn("node2");

        Client client = mock(Client.class);
        KV kvClient = mock(KV.class);
        when(client.getKVClient()).thenReturn(kvClient);

        // Mock index metadata
        setupIndexMetadataMocks(kvClient);

        // Step 1: Deserialize node1 state
        NodeState node1State = ETCDStateDeserializer.deserializeNodeState(
            node1,
            ByteSequence.from(node1Configuration, StandardCharsets.UTF_8),
            client,
            "test-cluster",
            true
        );

        assertTrue(node1State instanceof DataNodeState);
        DataNodeState dataNode1State = (DataNodeState) node1State;

        // Step 2: Verify cluster state has primary shard in INITIALIZING state
        ClusterState node1ClusterState = dataNode1State.buildClusterState(ClusterState.EMPTY_STATE);
        assertEquals(1, node1ClusterState.getMetadata().indices().size());
        assertTrue(node1ClusterState.getMetadata().hasIndex("idx1"));
        assertTrue(node1ClusterState.getRoutingTable().index("idx1").shard(0).primaryShard().initializing());

        // Step 3: Write heartbeat for node1 with primary as STARTED
        String node1HealthPath = ETCDPathUtils.buildSearchUnitActualStatePath("test-cluster", "node1");
        String node1HealthInfo = """
            {
                "nodeId": "node1-id",
                "ephemeralId": "node1-ephemeral",
                "address": "127.0.0.1",
                "port": 9200,
                "timestamp": 1750099493841,
                "heartbeatIntervalSeconds": 5,
                "nodeRouting": {
                    "idx1": [
                        {
                            "shardId": 0,
                            "role": "primary",
                            "state": "STARTED",
                            "allocationId": "alloc1",
                            "currentNodeId": "node1-id",
                            "currentNodeName": "node1"
                        }
                    ]
                }
            }
            """;

        setupHealthMock(kvClient, node1HealthPath, node1HealthInfo);

        // Step 4: Create node state for node2 with a replica pointing to node1
        String node2Configuration = """
            {
                "local_shards": {
                    "idx1": {
                        "0" : {
                            "type": "REPLICA",
                            "primary_node": "node1"
                        }
                    }
                }
            }
            """;

        NodeState node2State = ETCDStateDeserializer.deserializeNodeState(
            node2,
            ByteSequence.from(node2Configuration, StandardCharsets.UTF_8),
            client,
            "test-cluster",
            true
        );

        assertTrue(node2State instanceof DataNodeState);
        DataNodeState dataNode2State = (DataNodeState) node2State;

        // Step 5: Verify cluster state for node2 has two shards
        ClusterState node2ClusterState = dataNode2State.buildClusterState(ClusterState.EMPTY_STATE);
        assertEquals(1, node2ClusterState.getMetadata().indices().size());
        assertTrue(node2ClusterState.getMetadata().hasIndex("idx1"));

        // Should have primary shard (STARTED) and replica shard (INITIALIZING)
        assertEquals(2, node2ClusterState.getRoutingTable().index("idx1").shard(0).size());

        // Primary should be STARTED on node1
        assertTrue(node2ClusterState.getRoutingTable().index("idx1").shard(0).primaryShard().started());
        assertEquals("node1-id", node2ClusterState.getRoutingTable().index("idx1").shard(0).primaryShard().currentNodeId());

        // Replica should be INITIALIZING on node2
        assertTrue(node2ClusterState.getRoutingTable().index("idx1").shard(0).replicaShards().get(0).initializing());
        assertEquals("node2-id", node2ClusterState.getRoutingTable().index("idx1").shard(0).replicaShards().get(0).currentNodeId());

        // Step 6: Write heartbeat for node2 showing STARTED primary and INITIALIZING replica
        String node2HealthPath = ETCDPathUtils.buildSearchUnitActualStatePath("test-cluster", "node2");
        String node2HealthInfo = """
            {
                "nodeId": "node2-id",
                "ephemeralId": "node2-ephemeral",
                "address": "127.0.0.1",
                "port": 9201,
                "timestamp": 1750099493842,
                "heartbeatIntervalSeconds": 5,
                "nodeRouting": {
                    "idx1": [
                        {
                            "shardId": 0,
                            "role": "primary",
                            "state": "STARTED",
                            "allocationId": "alloc1",
                            "currentNodeId": "node1-id",
                            "currentNodeName": "node1"
                        },
                        {
                            "shardId": 0,
                            "role": "replica",
                            "state": "INITIALIZING",
                            "allocationId": "alloc2",
                            "currentNodeId": "node2-id",
                            "currentNodeName": "node2"
                        }
                    ]
                }
            }
            """;

        setupHealthMock(kvClient, node2HealthPath, node2HealthInfo);

        // Step 7: Deserialize new state on node1 where primary has replica pointing to node2
        String node1UpdatedConfiguration = """
            {
                "local_shards": {
                    "idx1": {
                        "0" : {
                            "type": "PRIMARY",
                            "replica_nodes": ["node2"]
                        }
                    }
                }
            }
            """;

        NodeState node1UpdatedState = ETCDStateDeserializer.deserializeNodeState(
            node1,
            ByteSequence.from(node1UpdatedConfiguration, StandardCharsets.UTF_8),
            client,
            "test-cluster",
            false
        );

        assertTrue(node1UpdatedState instanceof DataNodeState);
        DataNodeState dataNode1UpdatedState = (DataNodeState) node1UpdatedState;

        // Create a previous cluster state with only the primary shard (STARTED) from node2's state
        ShardId shardId = node2ClusterState.getRoutingTable().index("idx1").shard(0).shardId();
        ClusterState previousStateWithStartedPrimary = ClusterState.builder(node2ClusterState)
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(node2ClusterState.getMetadata().index("idx1").getIndex())
                            .addIndexShard(
                                new IndexShardRoutingTable.Builder(shardId).addShard(
                                    node2ClusterState.getRoutingTable().index("idx1").shard(0).primaryShard()
                                )  // Only add primary shard
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        ClusterState node1UpdatedClusterState = dataNode1UpdatedState.buildClusterState(previousStateWithStartedPrimary);
        assertEquals(1, node1UpdatedClusterState.getMetadata().indices().size());
        assertTrue(node1UpdatedClusterState.getMetadata().hasIndex("idx1"));

        // Should have primary shard (STARTED) and replica shard (INITIALIZING)
        assertEquals(2, node1UpdatedClusterState.getRoutingTable().index("idx1").shard(0).size());

        // Primary should be STARTED on node1 (carried over from previous state)
        assertTrue(node1UpdatedClusterState.getRoutingTable().index("idx1").shard(0).primaryShard().started());
        assertEquals("node1-id", node1UpdatedClusterState.getRoutingTable().index("idx1").shard(0).primaryShard().currentNodeId());

        // Replica should be INITIALIZING on node2
        assertTrue(node1UpdatedClusterState.getRoutingTable().index("idx1").shard(0).replicaShards().get(0).initializing());
        assertEquals("node2-id", node1UpdatedClusterState.getRoutingTable().index("idx1").shard(0).replicaShards().get(0).currentNodeId());
    }

    private void setupIndexMetadataMocks(KV kvClient) {
        ByteSequence idx1SettingsPath = ByteSequence.from(
            ETCDPathUtils.buildIndexSettingsPath("test-cluster", "idx1"),
            StandardCharsets.UTF_8
        );
        ByteSequence idx1MappingsPath = ByteSequence.from(
            ETCDPathUtils.buildIndexMappingsPath("test-cluster", "idx1"),
            StandardCharsets.UTF_8
        );

        RangeResponse idx1SettingsResponse = RangeResponse.newBuilder()
            .addKvs(io.etcd.jetcd.api.KeyValue.newBuilder().setValue(ByteString.copyFrom("""
                {
                  "index": {
                    "number_of_shards": "1",
                    "number_of_replicas": "1",
                    "uuid": "test-idx1-uuid",
                    "version": {
                      "created": "137227827"
                    }
                  }
                }
                """, StandardCharsets.UTF_8)).build())
            .build();

        RangeResponse idx1MappingsResponse = RangeResponse.newBuilder()
            .addKvs(io.etcd.jetcd.api.KeyValue.newBuilder().setValue(ByteString.copyFrom("""
                {
                  "properties": {
                    "field1": {
                      "type": "text"
                    }
                  }
                }
                """, StandardCharsets.UTF_8)).build())
            .build();

        when(kvClient.get(eq(idx1SettingsPath))).thenReturn(CompletableFuture.completedFuture(new GetResponse(idx1SettingsResponse, null)));
        when(kvClient.get(eq(idx1MappingsPath))).thenReturn(CompletableFuture.completedFuture(new GetResponse(idx1MappingsResponse, null)));
    }

    private void setupHealthMock(KV kvClient, String healthPath, String healthInfo) {
        ByteSequence healthKey = ByteSequence.from(healthPath, StandardCharsets.UTF_8);
        RangeResponse healthResponse = RangeResponse.newBuilder()
            .addKvs(io.etcd.jetcd.api.KeyValue.newBuilder().setValue(ByteString.copyFrom(healthInfo, StandardCharsets.UTF_8)).build())
            .build();
        when(kvClient.get(eq(healthKey))).thenReturn(CompletableFuture.completedFuture(new GetResponse(healthResponse, null)));
    }

}
