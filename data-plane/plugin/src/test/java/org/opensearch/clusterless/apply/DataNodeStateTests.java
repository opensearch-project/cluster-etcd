/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.ClusterlessIndexSettings;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The fabrication, as a pure function.
 *
 * <p>Built and tested at this milestone rather than at the one that applies it, which is the advice
 * {@code MILESTONES.md} gives and the reason is worth restating: this is a function of (previous state, desired
 * state), and every property that matters about it is a property of that pair. Waiting for a running node to
 * check them would mean checking them by observing shard recoveries, which is slow, indirect, and hopeless as a
 * regression test.
 *
 * <p>The assertions that matter most are the ones about the <em>second</em> call. A fabricator that produces a
 * plausible state from an empty previous one and then quietly restarts every shard on the pass after is the
 * failure this design is most exposed to, because it looks like it works.
 */
public class DataNodeStateTests extends OpenSearchTestCase {

    private static final String LOCAL = "pv1sDMfZRxSTMiW5VrbN9w";
    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    // ---------------------------------------------------------------- the shape of a first application

    public void testAFirstApplicationDescribesTheAssignedCopies() {
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY, 1, ShardRole.PRIMARY)));

        assertEquals(1, state.metadata().indices().size());
        IndexMetadata index = onlyIndex(state);
        assertEquals(UUID, index.getIndexUUID());
        assertEquals(2, state.routingTable().index(index.getIndex()).shards().size());

        for (int shard = 0; shard < 2; shard++) {
            ShardRouting routing = state.routingTable().index(index.getIndex()).shard(shard).shards().get(0);
            assertTrue("a new copy starts as initializing", routing.initializing());
            assertTrue(routing.primary());
            assertEquals(LOCAL, routing.currentNodeId());
            // The in-sync set has to believe in the allocation id the routing entry carries.
            assertTrue(index.inSyncAllocationIds(shard).contains(routing.allocationId().getId()));
        }
    }

    public void testTheStateCarriesOnlyTheLocalNode() {
        // There is no cluster to enumerate, and a data node has no business knowing the fleet's routing table.
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));

        assertEquals(1, state.nodes().getSize());
        assertEquals(LOCAL, state.nodes().getLocalNodeId());
    }

    public void testEachApplicationLooksLikeANewerPublication() {
        // The applier ignores a state that is not newer than the one it has.
        ClusterState first = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));
        ClusterState second = build(first, desired(Map.of(0, ShardRole.PRIMARY)));

        assertEquals(ClusterState.EMPTY_STATE.version() + 1, first.version());
        assertEquals(first.version() + 1, second.version());
    }

    public void testIdentityTravelsInSettingsRatherThanInTheName() {
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));
        IndexMetadata index = onlyIndex(state);

        assertEquals(ACME.value(), ClusterlessIndexSettings.TENANT.get(index.getSettings()));
        assertEquals("my-index", ClusterlessIndexSettings.TENANT_INDEX_NAME.get(index.getSettings()));
        assertEquals(Long.valueOf(3041L), ClusterlessIndexSettings.CONFIG_REVISION.get(index.getSettings()));
        // The uuid is the server's own index uuid, so Index equality is already identity in the contract's sense.
        assertEquals(UUID, index.getIndexUUID());
    }

    public void testTheControlPlanesSettingsAndMappingsSurvive() {
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));
        IndexMetadata index = onlyIndex(state);

        assertEquals(2, index.getNumberOfShards());
        assertEquals(0, index.getNumberOfReplicas());
        assertNotNull(index.mapping());
        assertTrue(index.mapping().source().string(), index.mapping().source().string().contains("keyword"));
    }

    // ---------------------------------------------------------------- the second application

    public void testACopyThatIsAlreadyServingStaysServing() {
        // D2.2, and the most expensive thing in this class to get wrong. A routing table rebuilt from scratch
        // presents a started shard as a brand new one, so the server recovers it -- on every pass, for ever.
        ClusterState first = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));
        ClusterState serving = withShardStarted(first);
        ShardRouting before = onlyCopy(serving, 0);

        ClusterState second = build(serving, desired(Map.of(0, ShardRole.PRIMARY)));

        ShardRouting after = onlyCopy(second, 0);
        assertTrue("the copy must still be started", after.started());
        assertEquals("the allocation id must not change", before.allocationId().getId(), after.allocationId().getId());
        assertEquals(before, after);
    }

    public void testApplyingTheSameDesiredStateRepeatedlyChangesNothingButTheVersion() {
        ClusterState state = withShardStarted(build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY))));
        ShardRouting first = onlyCopy(state, 0);
        long metadataVersion = onlyIndex(state).getVersion();

        for (int pass = 0; pass < 5; pass++) {
            state = build(state, desired(Map.of(0, ShardRole.PRIMARY)));
        }

        assertEquals(first, onlyCopy(state, 0));
        assertEquals("metadata version must not climb while nothing changes", metadataVersion, onlyIndex(state).getVersion());
    }

    public void testMetadataVersionsAreCarriedForwardRatherThanReset() {
        // The server uses these to decide what to reload. Resetting them says everything changed; the effect is
        // an index that reparses its mappings on every pass.
        ClusterState first = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));
        ClusterState bumped = ClusterState.builder(first)
            .metadata(
                org.opensearch.cluster.metadata.Metadata.builder()
                    .put(IndexMetadata.builder(onlyIndex(first)).version(17).settingsVersion(5).mappingVersion(3).build(), false)
            )
            .build();

        ClusterState second = build(bumped, desired(Map.of(0, ShardRole.PRIMARY)));

        IndexMetadata index = onlyIndex(second);
        assertEquals(17, index.getVersion());
        assertEquals(5, index.getSettingsVersion());
        assertEquals(3, index.getMappingVersion());
    }

    public void testARealSettingsChangeBumpsTheSettingsVersion() {
        ClusterState first = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));
        long settingsVersion = onlyIndex(first).getSettingsVersion();
        long mappingVersion = onlyIndex(first).getMappingVersion();

        // A new revision of the index document, with a setting changed. This is what the supervisor re-issuing
        // an assignment after a put_settings looks like from here.
        IndexConfig changed = new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "2", "index.number_of_replicas", "0", "index.refresh_interval", "30s"),
            mappings(),
            3042
        );
        ClusterState second = build(first, desiredWith(changed, Map.of(0, ShardRole.PRIMARY)));

        IndexMetadata index = onlyIndex(second);
        assertEquals(settingsVersion + 1, index.getSettingsVersion());
        assertEquals("the mapping did not change, so its version must not move", mappingVersion, index.getMappingVersion());
        assertEquals("30s", index.getSettings().get("index.refresh_interval"));
    }

    // ---------------------------------------------------------------- recovery, and not losing data

    public void testANodeWithNoDataOnDiskRecoversFromEmpty() {
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)), LocalShardStore.EMPTY);
        assertEquals(RecoverySource.Type.EMPTY_STORE, onlyCopy(state, 0).recoverySource().getType());
    }

    public void testANodeWithDataOnDiskRecoversFromIt() {
        // The store cannot tell a restarting node that it used to hold a shard -- allocation ids are
        // deliberately not published -- so only the disk knows. Getting this backwards discards a shard.
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)), shardId -> true);
        assertEquals(RecoverySource.Type.EXISTING_STORE, onlyCopy(state, 0).recoverySource().getType());
    }

    public void testTheShardStoreIsAskedAboutTheShardBeingBuilt() {
        // Keyed by index uuid rather than by the fabricated name, which is what lets a returning node find its
        // own data even if this plugin's naming scheme changed under it.
        List<ShardId> asked = new java.util.ArrayList<>();
        build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY, 1, ShardRole.PRIMARY)), shardId -> {
            asked.add(shardId);
            return false;
        });

        assertEquals(2, asked.size());
        for (ShardId shardId : asked) {
            assertEquals(UUID, shardId.getIndex().getUUID());
        }
    }

    // ---------------------------------------------------------------- search replicas

    public void testAnIndexHeldOnlyAsASearchReplicaIsMarkedSearchOnly() {
        // RoutingNodes refuses a shard with no primary unless the index says serving reads is all it does, and
        // this node has no primary to name: the primary is on another node and a segment-replication copy is
        // never told where. cluster-etcd reached the same conclusion via the same assertion.
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.SEARCH_REPLICA)));

        assertTrue(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.get(onlyIndex(state).getSettings()));
        assertTrue(onlyCopy(state, 0).isSearchOnly());
        assertFalse(onlyCopy(state, 0).primary());
        // The assertion that would fire if this were wrong.
        assertNotNull(state.getRoutingNodes().node(LOCAL));
    }

    public void testAnIndexWithAPrimaryHereIsNotMarkedSearchOnly() {
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY, 1, ShardRole.SEARCH_REPLICA)));
        assertFalse(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.get(onlyIndex(state).getSettings()));
    }

    // ---------------------------------------------------------------- primary terms and creation dates

    public void testEveryPrimaryTermIsOneAndNeverGoesBackwards() {
        // A known gap rather than a simplification: nothing in the contract carries a primary term, so there is
        // nothing to fabricate one from, and D8.2 records what that costs.
        ClusterState first = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));
        assertEquals(1L, onlyIndex(first).primaryTerm(0));

        ClusterState second = build(first, desired(Map.of(0, ShardRole.PRIMARY)));
        assertEquals(1L, onlyIndex(second).primaryTerm(0));
    }

    public void testTheCreationDateComesFromTheDocumentRatherThanTheClock() {
        // A date from the clock would differ on every node and every restart, which the server reads as a
        // metadata change -- so every node would disagree about an index nobody had touched.
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));
        assertEquals(1788969977933L, onlyIndex(state).getCreationDate());
    }

    public void testAnIndexWithNoCreationDateGetsADeterministicOne() {
        IndexConfig undated = new IndexConfig("my-index", UUID, 0, true, settings(), mappings(), 3041);

        long first = onlyIndex(build(ClusterState.EMPTY_STATE, desiredWith(undated, Map.of(0, ShardRole.PRIMARY)))).getCreationDate();
        long again = onlyIndex(build(ClusterState.EMPTY_STATE, desiredWith(undated, Map.of(0, ShardRole.PRIMARY)))).getCreationDate();

        assertEquals("the same index must get the same date every time", first, again);
        assertTrue(first > 0);
    }

    // ---------------------------------------------------------------- empty desired state

    public void testAnEmptyDesiredStateIsAValidStateWithNothingInIt() {
        // What applying "hold nothing" looks like. Not an error: an assignment that names nothing is the one
        // document that means drop everything.
        ClusterState state = build(ClusterState.EMPTY_STATE, new DesiredState(9, Map.of(), List.of(), List.of()));

        assertTrue(state.metadata().indices().isEmpty());
        assertEquals(1, state.nodes().getSize());
    }

    // ---------------------------------------------------------------- fixtures

    private static ClusterState build(ClusterState previous, DesiredState desired) {
        return build(previous, desired, LocalShardStore.EMPTY);
    }

    private static ClusterState build(ClusterState previous, DesiredState desired, LocalShardStore shardStore) {
        return new DataNodeState(localNode(), desired, MappingSource.WRAPPED, shardStore).buildClusterState(previous);
    }

    private static Map<String, String> settings() {
        return Map.of("index.number_of_shards", "2", "index.number_of_replicas", "0");
    }

    private static Map<String, Object> mappings() {
        return Map.of("properties", Map.of("host", Map.of("type", "keyword")));
    }

    private static DesiredState desired(Map<Integer, ShardRole> shards) {
        return desiredWith(new IndexConfig("my-index", UUID, 1788969977933L, true, settings(), mappings(), 3041), shards);
    }

    private static DesiredState desiredWith(IndexConfig config, Map<Integer, ShardRole> shards) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        return new DesiredState(42, Map.of(key, new DesiredState.DesiredIndex(key, config, shards)), List.of(), List.of());
    }

    private static IndexMetadata onlyIndex(ClusterState state) {
        return state.metadata().iterator().next();
    }

    private static ShardRouting onlyCopy(ClusterState state, int shard) {
        Index index = onlyIndex(state).getIndex();
        return state.routingTable().index(index).shard(shard).shards().get(0);
    }

    /** What the server would have done to the state once the shard finished recovering. */
    private static ClusterState withShardStarted(ClusterState state) {
        Index index = onlyIndex(state).getIndex();
        org.opensearch.cluster.routing.IndexRoutingTable.Builder routing = org.opensearch.cluster.routing.IndexRoutingTable.builder(
            index
        );
        for (org.opensearch.cluster.routing.IndexShardRoutingTable shard : state.routingTable().index(index)) {
            for (ShardRouting copy : shard) {
                routing.addShard(copy.initializing() ? copy.moveToStarted() : copy);
            }
        }
        return ClusterState.builder(state)
            .routingTable(org.opensearch.cluster.routing.RoutingTable.builder().add(routing).build())
            .build();
    }

    private static DiscoveryNode localNode() {
        return new DiscoveryNode(
            "data-3",
            LOCAL,
            "ephemeral",
            "localhost",
            "127.0.0.1",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }
}
