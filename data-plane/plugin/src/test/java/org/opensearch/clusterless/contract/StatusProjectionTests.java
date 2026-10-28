/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Reading the two projections a coordinator routes from.
 *
 * <p>The fixtures in the first two tests are **verbatim from a real store**, written by a real supervisor during
 * the run that verified milestone 7 — including field order and the mixture of cases, which is the part a
 * hand-written fixture gets wrong. Enum values arrive upper-case here and lower-case in node-written documents,
 * and rather than depend on that this parses both.
 */
public class StatusProjectionTests extends OpenSearchTestCase {

    /** Copied from `tenant/default/status/indices/<uuid>` after the supervisor placed a primary. */
    private static final String INDEX_STATUS = """
        {"index":"my-index","index_uuid":"JCZOYbN_9b8hclL3g_8nZQ","config_revision":4,"status":"green",
         "replication":"SEGMENT","number_of_shards":1,"number_of_replicas":0,
         "shards":{"0":[{"node_id":"UntRkFd4QBihsxOaWA7Ijg","role":"PRIMARY","state":"STARTED","serving":true}]},
         "observed_at":1789490023832,"observed_pass":871,"active_shards":1,"active_primary_shards":1,
         "unassigned_shards":0,"initializing_shards":0}""";

    public void testAnIndexStatusFromARealSupervisorIsRead() {
        IndexStatus status = IndexStatus.read(INDEX_STATUS.getBytes(StandardCharsets.UTF_8));

        assertEquals("my-index", status.name());
        assertEquals("JCZOYbN_9b8hclL3g_8nZQ", status.uuid());
        assertEquals(4L, status.configRevision());
        assertEquals(1, status.numberOfShards());
        assertEquals(IndexStatus.Replication.SEGMENT, status.replication());

        List<IndexStatus.Copy> copies = status.copiesOf(0);
        assertEquals(1, copies.size());
        assertEquals(NodeId.of("UntRkFd4QBihsxOaWA7Ijg"), copies.get(0).node());
        assertEquals(ShardRole.PRIMARY, copies.get(0).role());
        assertEquals(ShardState.STARTED, copies.get(0).state());
        assertTrue(copies.get(0).serving());
    }

    public void testTheFieldsThisPluginDoesNotUseAreIgnoredRatherThanRefused() {
        // Health, timestamps and pass counters are for other readers. A document that carried only those and the
        // identity would still be read, because a supervisor is allowed to add fields.
        IndexStatus status = IndexStatus.read(
            "{\"index\":\"my-index\",\"index_uuid\":\"u\",\"something_new\":{\"nested\":1}}".getBytes(StandardCharsets.UTF_8)
        );

        assertEquals("my-index", status.name());
        assertTrue(status.shards().isEmpty());
    }

    public void testACopyThatIsNotStartedIsStillReported() {
        // Load-bearing rather than incidental: a projection filtered to STARTED would deadlock a replica placed in
        // the same pass as its primary, because the replica could never see the primary it waits for.
        IndexStatus status = IndexStatus.read("""
            {"index":"my-index","index_uuid":"u","shards":{"0":[
              {"node_id":"AAAAAAAAAAAAAAAAAAAAAA","role":"PRIMARY","state":"INITIALIZING","serving":false}]}}
            """.getBytes(StandardCharsets.UTF_8));

        IndexStatus.Copy copy = status.copiesOf(0).get(0);
        assertEquals(ShardState.INITIALIZING, copy.state());
        assertFalse(copy.serving());
    }

    public void testAnUnknownRoleOrStateBecomesTheSentinelRatherThanFailingTheDocument() {
        // A supervisor newer than this node is the ordinary case during an upgrade, and losing the whole index's
        // placement over one unrecognised string would take a coordinator's routing with it.
        IndexStatus status = IndexStatus.read("""
            {"index":"my-index","index_uuid":"u","replication":"QUANTUM","shards":{"0":[
              {"node_id":"AAAAAAAAAAAAAAAAAAAAAA","role":"WITNESS","state":"PONDERING"}]}}
            """.getBytes(StandardCharsets.UTF_8));

        assertEquals(IndexStatus.Replication.UNKNOWN, status.replication());
        assertEquals(ShardRole.UNKNOWN, status.copiesOf(0).get(0).role());
        assertEquals(ShardState.UNKNOWN, status.copiesOf(0).get(0).state());
    }

    public void testAUuidMismatchIsDetectableByTheReader() {
        // The same check the resolver makes on an index document: a name is reused, a uuid is not.
        IndexStatus status = IndexStatus.read(INDEX_STATUS.getBytes(StandardCharsets.UTF_8));

        assertTrue(status.matches("JCZOYbN_9b8hclL3g_8nZQ"));
        assertFalse(status.matches("aDifferentUuid1234567"));
    }

    public void testAShardKeyThatIsNotANumberIsAMalformedDocument() {
        // Not tolerated, unlike an unknown enum: a shard number that cannot be parsed is not a newer supervisor
        // saying something this build does not understand, it is a document that cannot mean anything.
        expectThrows(
            MalformedDocumentException.class,
            () -> IndexStatus.read("{\"index\":\"i\",\"index_uuid\":\"u\",\"shards\":{\"first\":[]}}".getBytes(StandardCharsets.UTF_8))
        );
    }

    // ---------------------------------------------------------------- the peer projection

    public void testANodeStatusCarriesEnoughToBuildAPeer() {
        NodeStatus status = NodeStatus.read("""
            {"node_id":"UntRkFd4QBihsxOaWA7Ijg","ephemeral_id":"z25m5RGTRK2icv46eD2sCA","name":"runTask-1",
             "roles":["data"],"address":"127.0.0.1","port":9301,"version":"3.9.0",
             "attributes":{"remote_store.segment.repository":"segments"},"liveness":"live",
             "observed_at":1789490198591}""".getBytes(StandardCharsets.UTF_8));

        assertEquals(NodeId.of("UntRkFd4QBihsxOaWA7Ijg"), status.nodeId());
        assertEquals("z25m5RGTRK2icv46eD2sCA", status.ephemeralId());
        assertEquals("runTask-1", status.name());
        assertEquals("127.0.0.1", status.address());
        assertEquals(9301, status.port());
        assertEquals("3.9.0", status.version());
        assertEquals("segments", status.attributes().get("remote_store.segment.repository"));
        assertTrue(status.holdsData());
        assertTrue(status.isReachable());
        assertTrue(status.isAddressable());
    }

    public void testANodeStatusWithoutAnEphemeralIdIsNotAddressable() {
        // The case that made this field exist. DiscoveryNode.equals compares only the ephemeral id, and the
        // transport's connection validator refuses a peer that does not equal what the remote reports -- so a
        // projection written by an older supervisor gives an address that cannot be used, and saying so is the only
        // honest option available to a reader.
        NodeStatus status = NodeStatus.read(
            "{\"node_id\":\"UntRkFd4QBihsxOaWA7Ijg\",\"address\":\"127.0.0.1\",\"port\":9301,\"liveness\":\"live\"}"
                .getBytes(StandardCharsets.UTF_8)
        );

        assertTrue(status.isReachable());
        assertFalse("an address alone is not enough to connect", status.isAddressable());
    }

    public void testALostNodeIsNotWorthRoutingToButAStaleOneIs() {
        // A late heartbeat usually means a slow store rather than a dead node, and refusing to route to it would
        // turn one into an outage. LOST is the supervisor having given up, and its copies are being placed
        // elsewhere.
        assertTrue(nodeWithLiveness("stale").isReachable());
        assertFalse(nodeWithLiveness("lost").isReachable());
        assertTrue("a liveness a newer supervisor invented must not read as dead", nodeWithLiveness("dozing").isReachable());
    }

    public void testARoleThisBuildDoesNotKnowDoesNotMakeTheNodeUseless() {
        NodeStatus status = NodeStatus.read(
            "{\"node_id\":\"UntRkFd4QBihsxOaWA7Ijg\",\"roles\":[\"data\",\"archivist\"],\"liveness\":\"live\"}"
                .getBytes(StandardCharsets.UTF_8)
        );

        assertTrue(status.holdsData());
        assertTrue(status.roles().contains(NodeRole.UNKNOWN));
    }

    private static NodeStatus nodeWithLiveness(String liveness) {
        return NodeStatus.read(
            ("{\"node_id\":\"UntRkFd4QBihsxOaWA7Ijg\",\"liveness\":\"" + liveness + "\"}").getBytes(StandardCharsets.UTF_8)
        );
    }
}
