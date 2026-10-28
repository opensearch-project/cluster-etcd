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
        // A node naming a segment repository and nothing else, in the default mode, is genuinely misconfigured:
        // RemoteStoreNodeAttribute requires a translog and a cluster-state repository alongside it. Failing here
        // -- at startup, naming the attribute -- is the point. Reading it as "no remote store" instead would let
        // the node run and decline every search replica with a reason telling an operator to set attributes they
        // had in fact already set.
        Map<String, String> attributes = new HashMap<>();
        attributes.put("remote_store.segment.repository", "segments");
        attributes.put("remote_store.repository.segments.type", "fs");

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> RemoteStore.of(nodeWith(attributes)));

        assertTrue(e.getMessage(), e.getMessage().contains("remote_store.translog.repository"));
    }

    public void testSegmentsOnlyModeNeedsNoTranslogRepository() {
        // The upstream attribute added for this architecture. It makes RemoteStoreNodeAttribute require the segment
        // repository alone, which is what lets a deployment keep segments in a repository and skip the transaction
        // log -- reasonable when the ingestion source holds every operation.
        Map<String, String> attributes = new HashMap<>();
        attributes.put("remote_store.mode", "segments_only");
        attributes.put("remote_store.segment.repository", "segments");
        attributes.put("remote_store.repository.segments.type", "fs");
        attributes.put("remote_store.repository.segments.settings.location", "/tmp/clusterless-remote-store");

        RemoteStore store = RemoteStore.of(nodeWith(attributes));

        assertTrue("segments still go to a repository", store.configured());
        assertFalse("and the translog does not", store.translogConfigured());
        assertEquals("segments", store.repositories().repositories().get(0).name());
    }

    public void testSuchANodeIsNotWhatTheServerCallsARemoteStoreNode() {
        // The trap this class is keyed against. DiscoveryNode.isRemoteStoreNode() demands a cluster-state
        // repository, which segments_only does not have -- while IndexSettings.assignedOnRemoteNode is computed
        // from isRemoteDataAttributePresent, which such a node satisfies. Gating on the first would leave the
        // server routing replicas to RemoteStoreReplicationSource with no repository registered at all.
        Map<String, String> attributes = new HashMap<>();
        attributes.put("remote_store.mode", "segments_only");
        attributes.put("remote_store.segment.repository", "segments");
        attributes.put("remote_store.repository.segments.type", "fs");
        attributes.put("remote_store.repository.segments.settings.location", "/tmp/clusterless-remote-store");
        DiscoveryNode node = nodeWith(attributes);

        assertFalse("the server would not call this a remote store node", node.isRemoteStoreNode());
        assertTrue("and this plugin must still back its indices", RemoteStore.of(node).configured());
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
