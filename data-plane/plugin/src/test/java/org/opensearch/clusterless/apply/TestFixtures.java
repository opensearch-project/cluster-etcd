/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.core.common.transport.TransportAddress;

import java.net.InetAddress;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;

/** Fixtures shared by the tests in this package, so that each one says only what it is about. */
final class TestFixtures {

    private TestFixtures() {}

    static IndexConfig indexConfig(String uuid) {
        return new IndexConfig(
            "my-index",
            uuid,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "2", "index.number_of_replicas", "0"),
            Map.of("properties", Map.of("host", Map.of("type", "keyword"))),
            3041
        );
    }

    /** The same index, asking for segment replication -- which is what makes a search replica of it buildable. */
    static IndexConfig segmentReplicatedIndexConfig(String uuid) {
        return new IndexConfig(
            "my-index",
            uuid,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "2", "index.number_of_replicas", "0", "index.replication.type", "SEGMENT"),
            Map.of("properties", Map.of("host", Map.of("type", "keyword"))),
            3041
        );
    }

    static DiscoveryNode localNode(String nodeId) {
        return new DiscoveryNode(
            "data-3",
            nodeId,
            "ephemeral",
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }

    /**
     * A node configured with a remote store, through the real node attributes an operator would set.
     *
     * <p>Built as attributes rather than by handing {@link RemoteStore} the repository names directly, so that the
     * tests exercise the derivation the server will do: {@code isRemoteStoreNode()} demands a cluster-state
     * repository as well as a segment one, and {@code RemoteStoreNodeAttribute} throws on a node that names a
     * segment repository without the other two. Getting that wrong is exactly the kind of thing a test that
     * bypassed the attributes would not notice.
     */
    static DiscoveryNode remoteStoreNode(String nodeId, String repository) {
        Map<String, String> attributes = new java.util.HashMap<>();
        attributes.put("remote_store.segment.repository", repository);
        attributes.put("remote_store.translog.repository", repository);
        attributes.put("remote_store.state.repository", repository);
        attributes.put("remote_store.repository." + repository + ".type", "fs");
        attributes.put("remote_store.repository." + repository + ".settings.location", "/tmp/clusterless-remote-store");
        return new DiscoveryNode(
            "data-3",
            nodeId,
            "ephemeral",
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Map.copyOf(attributes),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }

    /**
     * A node that keeps segments in a repository and its translog on local disk.
     *
     * <p>{@code remote_store.mode=segments_only} is an upstream attribute added for this architecture, and it is
     * what makes the configuration expressible: it tells {@code RemoteStoreNodeAttribute} to require the segment
     * repository alone, where the default mode demands a translog and a cluster-state repository alongside it.
     * Note that such a node is <em>not</em> a remote store node by {@code DiscoveryNode.isRemoteStoreNode()},
     * which wants a cluster-state repository — see {@link RemoteStore#of}.
     */
    static DiscoveryNode segmentsOnlyNode(String nodeId, String repository) {
        Map<String, String> attributes = new java.util.HashMap<>();
        attributes.put("remote_store.mode", "segments_only");
        attributes.put("remote_store.segment.repository", repository);
        attributes.put("remote_store.repository." + repository + ".type", "fs");
        attributes.put("remote_store.repository." + repository + ".settings.location", "/tmp/clusterless-remote-store");
        return new DiscoveryNode(
            "data-3",
            nodeId,
            "ephemeral",
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Map.copyOf(attributes),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }

    /** A store that is not there, for the passes that have to survive one. */
    static final class FailingStore implements KeyValueStore {

        private final String message;

        FailingStore(String message) {
            this.message = message;
        }

        @Override
        public Optional<Versioned> get(String key) {
            throw new IllegalStateException(message);
        }

        @Override
        public NavigableMap<String, Versioned> scanPrefix(String keyPrefix) {
            throw new IllegalStateException(message);
        }

        @Override
        public long put(String key, byte[] value) {
            throw new IllegalStateException(message);
        }

        @Override
        public long put(String key, byte[] value, long expectedRevision) {
            throw new IllegalStateException(message);
        }

        @Override
        public boolean delete(String key) {
            throw new IllegalStateException(message);
        }

        @Override
        public boolean delete(String key, long expectedRevision) {
            throw new IllegalStateException(message);
        }
    }
}
