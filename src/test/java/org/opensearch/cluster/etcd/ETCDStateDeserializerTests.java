/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;
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
        ByteSequence idx1SettingsPath = ByteSequence.from(ETCDPathUtils.buildIndexSettingsPath("test-cluster", "idx1"), StandardCharsets.UTF_8);
        ByteSequence idx1MappingsPath = ByteSequence.from(ETCDPathUtils.buildIndexMappingsPath("test-cluster", "idx1"), StandardCharsets.UTF_8);

        RangeResponse idx1SettingsResponse = RangeResponse.newBuilder()
                .addKvs(
                        KeyValue.newBuilder()
                                .setValue(ByteString.copyFrom("""
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
                                        """, StandardCharsets.UTF_8))
                                .build()
                )
                .build();
        RangeResponse idx1MappingsResponse = RangeResponse.newBuilder()
                        .addKvs(
                                KeyValue.newBuilder()
                                        .setValue(ByteString.copyFrom("""
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
                                                """, StandardCharsets.UTF_8))
                                        .build()
                        )
                .build();

        when(kvClient.get(eq(idx1SettingsPath))).thenReturn(CompletableFuture.completedFuture(new GetResponse(idx1SettingsResponse, null)));
        when(kvClient.get(eq(idx1MappingsPath))).thenReturn(CompletableFuture.completedFuture(new GetResponse(idx1MappingsResponse, null)));

        NodeState nodeState = ETCDStateDeserializer.deserializeNodeState(localNode, ByteSequence.from(nodeConfiguration, StandardCharsets.UTF_8), client, "test-cluster");

        assertTrue(nodeState instanceof DataNodeState);
        DataNodeState dataNodeState = (DataNodeState) nodeState;
        ClusterState emptyClusterState = ClusterState.EMPTY_STATE;
        ClusterState clusterState = dataNodeState.buildClusterState(emptyClusterState);
        assertEquals(1, clusterState.getMetadata().indices().size());
        assertTrue(clusterState.getMetadata().hasIndex("idx1"));
    }

}