/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package tools;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.clusterless.apply.AssignmentPoller;
import org.opensearch.clusterless.apply.DataNodeState;
import org.opensearch.clusterless.apply.DesiredStateResolver;
import org.opensearch.clusterless.apply.LocalShardStore;
import org.opensearch.clusterless.apply.MappingSource;
import org.opensearch.clusterless.apply.RetainedIndexReaper;
import org.opensearch.clusterless.apply.StoreIndexConfigSource;
import org.opensearch.clusterless.contract.Heartbeat;
import org.opensearch.clusterless.contract.NodeId;
import org.opensearch.clusterless.contract.NodeRole;
import org.opensearch.clusterless.contract.PoolId;
import org.opensearch.clusterless.contract.Registration;
import org.opensearch.clusterless.publish.CapacityProbe;
import org.opensearch.clusterless.publish.NodePublisher;
import org.opensearch.clusterless.publish.ShardInventorySource;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.KeyValueStores;
import org.opensearch.core.common.transport.TransportAddress;

import java.net.InetAddress;
import java.util.EnumSet;
import java.util.Map;

/**
 * Runs one read-only pass for a node and prints what it would have applied.
 *
 * <p>Milestone 3's acceptance check, made reproducible: <b>a supervisor in {@code --write} mode assigns a shard,
 * and the node logs a complete intended state — without touching the node.</b> Driven by
 * {@code tools/supervisor-assigns-me.sh}, which writes the node documents, runs a real supervisor until it
 * assigns something, and then runs this.
 *
 * <p>What it does <em>not</em> do is start OpenSearch, which is the whole reason it can run in a sandbox with no
 * sockets. Everything from the store to the fabricated {@link ClusterState} is ordinary Java that needs no node;
 * only the last step — handing the state to {@code ClusterApplierService} — needs one, and that step is the
 * milestone after this. The local node is fabricated too, which is honest for a check about reading rather than
 * applying: it is the identity the assignment is keyed on, and nothing here uses it for anything else.
 */
public final class PollAssignment {

    private PollAssignment() {}

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("usage: PollAssignment <store-uri> <node-id>");
            System.exit(2);
        }
        String uri = args[0];
        NodeId nodeId = NodeId.of(args[1]);

        try (KeyValueStore store = KeyValueStores.open(uri)) {
            DiscoveryNode localNode = new DiscoveryNode(
                "data-" + nodeId,
                nodeId.value(),
                "eph-" + System.nanoTime(),
                "localhost",
                "127.0.0.1",
                new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                Map.of(),
                java.util.Set.of(DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );

            NodePublisher publisher = new NodePublisher(
                store,
                nodeId,
                localNode.getEphemeralId(),
                () -> new Registration(
                    nodeId,
                    localNode.getEphemeralId(),
                    localNode.getName(),
                    PoolId.of("shared-a"),
                    EnumSet.of(NodeRole.DATA),
                    "10.0.3.14",
                    9300,
                    Version.CURRENT.toString(),
                    Map.of("zone", "z1"),
                    5000,
                    0L
                ),
                ShardInventorySource.EMPTY,
                () -> new Heartbeat.Capacity(994_663_170_048L, 856_882_249_728L, 41, 5, 1.2),
                System::currentTimeMillis
            );

            StoreIndexConfigSource indexConfigs = new StoreIndexConfigSource(store);
            AssignmentPoller poller = new AssignmentPoller(
                store,
                nodeId,
                new DesiredStateResolver(indexConfigs),
                // No node, so no state has ever been applied. Which is exactly true of a node in read-only
                // mode however it is run: the intended state is always fabricated against an empty previous one.
                () -> ClusterState.EMPTY_STATE,
                (desired, stopRetaining) -> new DataNodeState(
                    new ClusterName("clusterless"),
                    localNode,
                    desired,
                    MappingSource.WRAPPED,
                    LocalShardStore.EMPTY,
                    stopRetaining
                ),
                publisher,
                // Nothing to apply to: there is no node here. Read-only mode is the only mode this tool has,
                // which is why it needs no applier and can run in a sandbox with no sockets.
                null,
                new RetainedIndexReaper(indexConfigs, 12),
                false
            );

            poller.poll();
            publisher.publish();

            System.out.println();
            System.out.println("poll status: " + poller.status());
        } catch (Exception e) {
            System.err.println("failed: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }
}
