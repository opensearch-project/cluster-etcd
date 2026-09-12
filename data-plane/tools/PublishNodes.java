/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package tools;

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

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Map;

/**
 * Publishes node documents into a store with the real publisher, so a real supervisor can be pointed at
 * them. Also seeds the pool and tenant an operator would otherwise have to create.
 *
 * <p>This is milestone 2's acceptance check, made reproducible: <b>done when the real supervisor sees the
 * node</b>. It needs no OpenSearch node and no container, because the only part of the plugin involved is
 * the part that talks to the store — which is exactly the part the milestone is about.
 *
 * <pre>
 *   tools/jbuild.sh --no-tests
 *   tools/supervisor-sees-me.sh          # runs this, then a supervisor, then prints the projection
 * </pre>
 *
 * <p>Two things that cost time when this was first done by hand, both worth knowing before writing into a
 * store directory with an editor: the filesystem backend puts a revision header in front of the value, so
 * a hand-written file parses as garbage and the supervisor reports it under {@code unreadable} rather than
 * failing; and a node id must match {@code [A-Za-z0-9_-]{1,64}}, so a friendly name with a space in it is
 * rejected at construction.
 */
public final class PublishNodes {

    private PublishNodes() {}

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("usage: PublishNodes <store-uri> [node-count]");
            System.exit(2);
        }
        String uri = args[0];
        int nodeCount = args.length > 1 ? Integer.parseInt(args[1]) : 2;

        try (KeyValueStore store = KeyValueStores.open(uri)) {
            seedControlPlaneState(store);
            for (int i = 0; i < nodeCount; i++) {
                publishNode(store, i);
            }
        } catch (Exception e) {
            System.err.println("failed: " + e);
            System.exit(1);
        }
    }

    private static void publishNode(KeyValueStore store, int index) {
        NodeId nodeId = NodeId.of("dp-node-" + index);
        String ephemeralId = "eph-" + System.nanoTime();

        NodePublisher publisher = new NodePublisher(
            store,
            nodeId,
            ephemeralId,
            () -> new Registration(
                nodeId,
                ephemeralId,
                "data-" + index,
                PoolId.of("shared-a"),
                EnumSet.of(NodeRole.DATA),
                "10.0.3." + (14 + index),
                9300,
                "3.9.0",
                // Spread across zones, so the pool's awareness rule has something to work with and the
                // supervisor's placement is not trivially satisfiable.
                Map.of("zone", "z" + (index % 3 + 1)),
                5000,
                0L
            ),
            // Nothing is applied at this milestone, so the node holds nothing -- and publishes an empty
            // inventory to say so, which is a different fact from having said nothing.
            ShardInventorySource.EMPTY,
            () -> new Heartbeat.Capacity(994_663_170_048L, 856_882_249_728L, 41, 5, 1.2),
            System::currentTimeMillis
        );

        publisher.publish();
        System.out.println("published " + nodeId + ": " + publisher.status());
    }

    /**
     * What an operator and the control plane would have written. Seeded through the store API rather than
     * by writing files, because the backend owns the on-disk format.
     */
    private static void seedControlPlaneState(KeyValueStore store) {
        put(
            store,
            "pools/definitions/shared-a",
            "{\"pool\":\"shared-a\",\"kind\":\"shared\",\"default\":true,"
                + "\"max_shard_copies_per_node\":2000,\"awareness_attributes\":[\"zone\"]}"
        );
        put(store, "tenants/acme", "{\"tenant\":\"acme\",\"registered_at\":1789165809000}");
        put(store, "pools/bindings/acme", "{\"pool\":\"shared-a\",\"generation\":1}");
        put(
            store,
            "tenant/acme/indices/my-index",
            "{\"index\":\"my-index\",\"uuid\":\"uzKxLWZu4nPSrTknEcDpoA\",\"creation_date\":\"1789165809000\","
                + "\"state\":\"open\",\"settings\":{\"index.number_of_replicas\":\"0\","
                + "\"index.number_of_shards\":\"2\",\"index.replication.type\":\"SEGMENT\"},"
                + "\"mappings\":{\"properties\":{\"host\":{\"type\":\"keyword\"}}}}"
        );
    }

    private static void put(KeyValueStore store, String key, String value) {
        store.put(key, value.getBytes(StandardCharsets.UTF_8));
        System.out.println("seeded " + key);
    }
}
