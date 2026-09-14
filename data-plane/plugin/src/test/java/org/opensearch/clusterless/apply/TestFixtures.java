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
