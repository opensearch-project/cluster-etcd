/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Reading a node's own remote-store configuration.
 *
 * <p>Small, and worth its own class because everything here is a question about <em>this</em> node rather than
 * about the desired state, and the two are easy to conflate. The repository is operator configuration; whether an
 * index uses it is desired state; and the interesting cases are where those two disagree.
 */
public class RemoteStoreTests extends OpenSearchTestCase {

    public void testANodeWithNoAttributesHasNoRemoteStore() {
        RemoteStore store = RemoteStore.of(TestFixtures.localNode("pv1sDMfZRxSTMiW5VrbN9w"));

        assertFalse(store.configured());
        assertNull(store.repositories());
        assertFalse(store.backs(TestFixtures.segmentReplicatedIndexConfig("uzKxLWZu4nPSrTknEcDpoA")));
    }

    public void testANodeWithNoRemoteStoreAddsNoIndexSettings() {
        Settings.Builder settings = Settings.builder();

        RemoteStore.NONE.addIndexSettings(settings);

        assertTrue(settings.build().isEmpty());
    }

    public void testTheRepositoryComesFromTheNodesAttributes() {
        RemoteStore store = RemoteStore.of(TestFixtures.remoteStoreNode("pv1sDMfZRxSTMiW5VrbN9w", "segments"));

        assertTrue(store.configured());
        assertNotNull(store.repositories());
        assertEquals(1, store.repositories().repositories().size());
        assertEquals("segments", store.repositories().repositories().get(0).name());
        assertEquals("fs", store.repositories().repositories().get(0).type());
    }

    public void testOnlyASegmentReplicationIndexIsBacked() {
        RemoteStore store = RemoteStore.of(TestFixtures.remoteStoreNode("pv1sDMfZRxSTMiW5VrbN9w", "segments"));

        assertTrue(store.backs(TestFixtures.segmentReplicatedIndexConfig("uzKxLWZu4nPSrTknEcDpoA")));
        assertFalse(store.backs(TestFixtures.indexConfig("uzKxLWZu4nPSrTknEcDpoA")));
    }

    public void testTheReplicationTypeIsReadCaseInsensitivelyAndFromTheIndexAlone() {
        // The control plane writes settings as strings, and "segment" and "SEGMENT" are the same intent. Read from
        // the index document rather than from any node-local default, because a default is the one thing that could
        // let two nodes disagree about whether the same index is backed.
        assertTrue(RemoteStore.segmentReplication(withReplicationType("segment")));
        assertTrue(RemoteStore.segmentReplication(withReplicationType(" SEGMENT ")));
        assertFalse(RemoteStore.segmentReplication(withReplicationType("DOCUMENT")));
        assertFalse("an index that says nothing is not segment replicated", RemoteStore.segmentReplication(withReplicationType(null)));
    }

    public void testAHalfConfiguredNodeFailsLoudlyRatherThanSilently() {
        // A node naming a segment repository but no cluster-state one is not a remote-store node as far as
        // DiscoveryNode is concerned, so it reads as having no remote store at all -- which would decline every
        // search replica with a reason saying to configure the attributes that are, in fact, half configured. Not
        // ideal, and better than the alternative of building a copy against a repository that will not resolve.
        Map<String, String> attributes = new HashMap<>();
        attributes.put("remote_store.segment.repository", "segments");
        attributes.put("remote_store.repository.segments.type", "fs");

        RemoteStore store = RemoteStore.of(nodeWith(attributes));

        assertFalse(store.configured());
    }

    private static IndexConfig withReplicationType(String replicationType) {
        Map<String, String> settings = new HashMap<>();
        settings.put("index.number_of_shards", "1");
        if (replicationType != null) {
            settings.put(IndexMetadata.SETTING_REPLICATION_TYPE, replicationType);
        }
        return new IndexConfig("my-index", "uzKxLWZu4nPSrTknEcDpoA", 1788969977933L, true, Map.copyOf(settings), Map.of(), 3041);
    }

    private static DiscoveryNode nodeWith(Map<String, String> attributes) {
        return new DiscoveryNode(
            "data-3",
            "pv1sDMfZRxSTMiW5VrbN9w",
            "ephemeral",
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Map.copyOf(attributes),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }
}
