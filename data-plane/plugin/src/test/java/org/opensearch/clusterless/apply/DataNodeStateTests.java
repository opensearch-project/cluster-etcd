/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionStatus;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.ClusterlessIndexSettings;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.common.settings.Settings;
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

    private static final ClusterName CLUSTER = new ClusterName("clusterless");
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

    public void testTheStateCarriesThisNodesClusterNameRatherThanInheritingOne() {
        // Found in a real node's GET /_cluster/state, which said "opensearch" -- ClusterName.DEFAULT -- while GET
        // /_cluster/health said "runTask". The state's name used to be copied from the previous state, so whatever
        // the first fabrication was handed propagated for ever, and two of this plugin's own answers disagreed
        // about which cluster they were describing.
        ClusterState first = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));
        assertEquals(CLUSTER, first.getClusterName());

        // And it stays right even when the previous state's name is wrong, which is the case that was broken.
        ClusterState second = build(first, desired(Map.of(0, ShardRole.PRIMARY)));
        assertEquals(CLUSTER, second.getClusterName());
        assertNotEquals("EMPTY_STATE carries the default name, which is where the wrong one came from",
            ClusterState.EMPTY_STATE.getClusterName(), second.getClusterName());
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

    public void testAnIndexWithAPrimaryHereIsStillMarkedSearchOnlyIfItAlsoHoldsASearchReplica() {
        // The mixed case, which is the natural placement rather than an edge: two nodes and a two-shard index with
        // one replica each gives each node a primary of one shard and a search replica of the other. Shard 1 has no
        // primary in this node's routing table, so the marker has to be on -- and the next test is why that does
        // not cost shard 0 its writes.
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY, 1, ShardRole.SEARCH_REPLICA)));

        assertTrue(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.get(onlyIndex(state).getSettings()));
        assertTrue(onlyCopy(state, 0).primary());
        assertTrue(onlyCopy(state, 1).isSearchOnly());
        // The assertion that would fire on shard 1 if the marker were off. Constructing RoutingNodes is what
        // IndicesClusterStateService does on every applied state, and these tests run with assertions enabled.
        assertNotNull(state.getRoutingNodes().node(LOCAL));
    }

    public void testAFabricatedStateCarriesNoWriteBlock() {
        // Pins an absence this design depends on. index.blocks.search_only exists to silence an assertion about a
        // shard with no primary, and its cluster block -- INDEX_SEARCH_ONLY_BLOCK, ClusterBlockLevel.WRITE, 403
        // "index scaled down" -- would stop writes to any primary of the same index on this node. It never applies,
        // because a block is only ever derived from that setting by cluster-manager code (MetadataCreateIndexService,
        // RestoreService, the gateway's ClusterStateUpdaters), none of which runs here: this state is built directly
        // and sets no blocks. If that ever changes, writes would start failing with 403 on a node that also holds a
        // search replica, and the cause would be nowhere near the symptom -- so it fails here instead.
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY, 1, ShardRole.SEARCH_REPLICA)));
        Index index = onlyIndex(state).getIndex();

        assertTrue(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.get(onlyIndex(state).getSettings()));
        assertFalse(
            "the search-only setting must stay a marker rather than becoming a write block",
            state.blocks().indexBlocked(ClusterBlockLevel.WRITE, index.getName())
        );
        assertTrue(state.blocks().global().isEmpty());
    }

    public void testTheSearchReplicaCountDescribesThisNodeRatherThanTheFleet() {
        // cluster.routing.search_replica.strict defaults to true, and with it on OperationRouting injects
        // Preference.SEARCH_REPLICA into every search that named no preference -- for any index whose metadata says
        // it has search replicas. Copying the control plane's count onto a node holding the primary would therefore
        // route searches to a set of shards that node does not have, and a healthy primary would answer nothing.
        IndexConfig wantsTwo = new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "2", "index.number_of_replicas", "0", "index.number_of_search_replicas", "2"),
            mappings(),
            3041
        );

        ClusterState holdingThePrimary = build(ClusterState.EMPTY_STATE, desiredWith(wantsTwo, Map.of(0, ShardRole.PRIMARY)));
        assertEquals(0, onlyIndex(holdingThePrimary).getNumberOfSearchOnlyReplicas());

        ClusterState holdingASearchReplica = build(
            ClusterState.EMPTY_STATE,
            desiredWith(wantsTwo, Map.of(0, ShardRole.SEARCH_REPLICA))
        );
        assertEquals(1, onlyIndex(holdingASearchReplica).getNumberOfSearchOnlyReplicas());

        // And zero in the mixed case, unlike the marker, because shard 0's searches would otherwise be routed to
        // search-only copies this node does not have. Shard 1's search replica is still reached: a started
        // search-only shard is in IndexShardRoutingTable.activeShards, so the default iterator includes it.
        ClusterState both = build(
            ClusterState.EMPTY_STATE,
            desiredWith(wantsTwo, Map.of(0, ShardRole.PRIMARY, 1, ShardRole.SEARCH_REPLICA))
        );
        assertEquals(0, onlyIndex(both).getNumberOfSearchOnlyReplicas());
    }

    // ---------------------------------------------------------------- the remote store

    public void testAnIndexUsingSegmentReplicationIsBackedByThisNodesRemoteStore() {
        ClusterState state = buildOnARemoteStoreNode(segmentReplicated());

        Settings settings = onlyIndex(state).getSettings();
        assertTrue(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(settings));
        assertEquals("segments", settings.get(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY));
        assertEquals("segments", settings.get(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY));
    }

    public void testTheRepositoryIsRegisteredThroughTheFabricatedMetadata() {
        // There is no cluster manager to write RepositoriesMetadata on node join, and RepositoriesService builds its
        // repositories by applying cluster state -- so without this the index settings would name a repository that
        // does not exist on the node.
        ClusterState state = buildOnARemoteStoreNode(segmentReplicated());

        RepositoriesMetadata repositories = state.metadata().custom(RepositoriesMetadata.TYPE);
        assertNotNull("the repository has to come from somewhere", repositories);
        assertTrue(repositories.repositories().stream().anyMatch(repository -> repository.name().equals("segments")));
    }

    public void testADocumentReplicationIndexIsNotBackedByTheRemoteStore() {
        // index.remote_store.enabled is validated against index.replication.type when the index service is
        // created, so enabling it here would fail the index on the apply path rather than at some later boundary.
        ClusterState state = buildOnARemoteStoreNode(
            new IndexConfig("my-index", UUID, 1788969977933L, true, settings(), mappings(), 3041)
        );

        assertFalse(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(onlyIndex(state).getSettings()));
    }

    public void testNoRemoteStorePathCustomDataIsStamped() {
        // The subtlest requirement in the milestone. RemoteStoreUtils.determineRemoteStorePathStrategy reads the
        // index metadata's REMOTE_STORE_CUSTOM_KEY and falls back to PathType.FIXED when there is none. One cluster
        // manager stamping that once is fine; every node stamping its own copy is not, because a primary and its
        // replica would then be free to disagree about where in the repository their segments live -- and the
        // symptom of disagreeing is a replica that reads an empty store, not an error.
        ClusterState state = buildOnARemoteStoreNode(segmentReplicated());

        assertNull(onlyIndex(state).getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY));
    }

    public void testANodeWithNoRemoteStoreRegistersNothingAndBacksNothing() {
        ClusterState state = build(ClusterState.EMPTY_STATE, desiredWith(segmentReplicated(), Map.of(0, ShardRole.PRIMARY)));

        assertNull(state.metadata().custom(RepositoriesMetadata.TYPE));
        assertFalse(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.get(onlyIndex(state).getSettings()));
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

    // ---------------------------------------------------------------- the mapping is not rebuilt every pass

    public void testTheMappingIsCanonicalisedOnceAndThenReused() {
        // Canonicalising asks IndicesService for a MapperService, which builds an IndexModule, calls
        // onIndexModule on every installed plugin, and holds a lock real index operations queue behind. Doing that
        // once per pass produced two log lines a second from unrelated components and read exactly like a shard
        // restarting in a loop.
        java.util.concurrent.atomic.AtomicInteger canonicalisations = new java.util.concurrent.atomic.AtomicInteger();
        MappingSource counting = (index, mapping) -> {
            canonicalisations.incrementAndGet();
            return MappingSource.WRAPPED.mappingFor(index, mapping);
        };

        ClusterState state = new DataNodeState(CLUSTER, localNode(), desired(Map.of(0, ShardRole.PRIMARY)), counting, LocalShardStore.EMPTY)
            .buildClusterState(ClusterState.EMPTY_STATE);
        assertEquals("the first application has nothing to reuse", 1, canonicalisations.get());

        for (int pass = 0; pass < 10; pass++) {
            state = new DataNodeState(CLUSTER, localNode(), desired(Map.of(0, ShardRole.PRIMARY)), counting, LocalShardStore.EMPTY)
                .buildClusterState(state);
        }

        assertEquals("the same index document must not be canonicalised again", 1, canonicalisations.get());
        assertNotNull(onlyIndex(state).mapping());
        assertEquals("and the mapping version must not climb", 1, onlyIndex(state).getMappingVersion());
    }

    public void testANewConfigRevisionDoesCanonicaliseAgain() {
        // The other half: the skip is keyed on the store revision of the index document, so a real change to it is
        // still picked up. Revisions are compared for equality only, which is all the contract permits.
        java.util.concurrent.atomic.AtomicInteger canonicalisations = new java.util.concurrent.atomic.AtomicInteger();
        MappingSource counting = (index, mapping) -> {
            canonicalisations.incrementAndGet();
            return MappingSource.WRAPPED.mappingFor(index, mapping);
        };

        ClusterState first = new DataNodeState(CLUSTER, localNode(), desired(Map.of(0, ShardRole.PRIMARY)), counting, LocalShardStore.EMPTY)
            .buildClusterState(ClusterState.EMPTY_STATE);

        IndexConfig newRevision = new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            settings(),
            Map.of("properties", Map.of("host", Map.of("type", "keyword"), "level", Map.of("type", "integer"))),
            3042
        );
        ClusterState second = new DataNodeState(
            CLUSTER,
            localNode(),
            desiredWith(newRevision, Map.of(0, ShardRole.PRIMARY)),
            counting,
            LocalShardStore.EMPTY
        ).buildClusterState(first);

        assertEquals(2, canonicalisations.get());
        assertTrue(onlyIndex(second).mapping().source().string(), onlyIndex(second).mapping().source().string().contains("level"));
        assertEquals("a real mapping change moves the mapping version", 2, onlyIndex(second).getMappingVersion());
    }

    public void testAnIndexWithNoMappingStaysThatWayWithoutCanonicalising() {
        // What indices.create with no body produces. Reusing a null mapping is a legitimate reuse, not a reason to
        // go and ask again on every pass.
        java.util.concurrent.atomic.AtomicInteger canonicalisations = new java.util.concurrent.atomic.AtomicInteger();
        MappingSource counting = (index, mapping) -> {
            canonicalisations.incrementAndGet();
            return MappingSource.WRAPPED.mappingFor(index, mapping);
        };
        IndexConfig unmapped = new IndexConfig("my-index", UUID, 1788969977933L, true, settings(), Map.of(), 3041);

        ClusterState state = new DataNodeState(CLUSTER, localNode(), desiredWith(unmapped, Map.of(0, ShardRole.PRIMARY)), counting,
            LocalShardStore.EMPTY).buildClusterState(ClusterState.EMPTY_STATE);
        state = new DataNodeState(CLUSTER, localNode(), desiredWith(unmapped, Map.of(0, ShardRole.PRIMARY)), counting,
            LocalShardStore.EMPTY).buildClusterState(state);

        assertEquals(1, canonicalisations.get());
        assertNull(onlyIndex(state).mapping());
    }

    // ---------------------------------------------------------------- one index's failure is not the node's

    public void testAnIndexThatCannotBeBuiltDoesNotTakeTheRestWithIt() {
        // D2.4, in the place it is easiest to violate. Fabricating the whole state is one operation, so an
        // exception anywhere in it aborts all of it -- and the node then applies nothing, holds nothing new, and
        // retries the same failing fabrication for ever. That is what a NullPointerException in the mapping merge
        // actually did: one index's problem became every index's problem.
        DesiredState.IndexKey good = new DesiredState.IndexKey(ACME, UUID);
        DesiredState.IndexKey bad = new DesiredState.IndexKey(ACME, "aSecondIndexUuid1234");
        IndexConfig config = new IndexConfig("my-index", UUID, 1788969977933L, true, settings(), mappings(), 3041);
        IndexConfig other = new IndexConfig("other-index", "aSecondIndexUuid1234", 1788969977933L, true, settings(), mappings(), 3041);
        DesiredState desired = new DesiredState(
            42,
            Map.of(
                good,
                new DesiredState.DesiredIndex(good, config, Map.of(0, ShardRole.PRIMARY)),
                bad,
                new DesiredState.DesiredIndex(bad, other, Map.of(0, ShardRole.PRIMARY))
            ),
            List.of(),
            List.of()
        );

        MappingSource explodesForOne = (index, mapping) -> {
            if (index.getUUID().equals("aSecondIndexUuid1234")) {
                throw new NullPointerException("as thrown from inside the mapping merge");
            }
            return MappingSource.WRAPPED.mappingFor(index, mapping);
        };
        ClusterState state = new DataNodeState(CLUSTER, localNode(), desired, explodesForOne, LocalShardStore.EMPTY).buildClusterState(
            ClusterState.EMPTY_STATE
        );

        assertEquals("the healthy index must still be described", 1, state.metadata().indices().size());
        assertNotNull(state.metadata().index(state.metadata().iterator().next().getIndex()));
        assertEquals(UUID, state.metadata().iterator().next().getIndexUUID());
    }

    public void testAnIndexThatCannotBeBuiltKeepsWhatWasAlreadyApplied() {
        // And the copy already serving keeps serving, rather than being dropped -- which would both stop the shard
        // and, because the metadata would vanish, delete its files.
        ClusterState serving = withShardStarted(build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY))));
        ShardRouting before = onlyCopy(serving, 0);

        MappingSource explodes = (index, mapping) -> {
            throw new IllegalStateException("no mapping for you");
        };
        ClusterState after = new DataNodeState(
            CLUSTER,
            localNode(),
            desired(Map.of(0, ShardRole.PRIMARY)),
            explodes,
            LocalShardStore.EMPTY
        ).buildClusterState(serving);

        assertEquals(1, after.metadata().indices().size());
        assertEquals("the serving copy must be untouched", before, onlyCopy(after, 0));
        assertTrue(onlyCopy(after, 0).started());
    }

    // ---------------------------------------------------------------- dropping a copy

    public void testAnIndexNoLongerAssignedKeepsItsMetadataAndLosesItsRouting() {
        // The difference between "stop serving this" and "destroy this", and the server picks between them by
        // whether the metadata is still there: absent means DELETED, which removes the shard files, while present
        // with no shards means NO_LONGER_ASSIGNED, which keeps them. D5.4 wants the second.
        ClusterState serving = withShardStarted(build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY))));
        Index index = onlyIndex(serving).getIndex();

        ClusterState afterDrop = build(serving, new DesiredState(43, Map.of(), List.of(), List.of()));

        assertNotNull("the metadata must survive, or the data is deleted", afterDrop.metadata().index(index));
        assertEquals(1, afterDrop.metadata().indices().size());
        assertTrue(
            "this node must hold no copy of it any more",
            afterDrop.routingTable().index(index) == null || afterDrop.routingTable().index(index).shards().isEmpty()
        );
    }

    public void testDroppingOneCopyOfAnIndexLeavesTheOther() {
        ClusterState serving = withShardStarted(
            build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY, 1, ShardRole.PRIMARY)))
        );

        ClusterState afterDrop = build(serving, desired(Map.of(0, ShardRole.PRIMARY)));

        Index index = onlyIndex(afterDrop).getIndex();
        assertEquals(1, afterDrop.routingTable().index(index).shards().size());
        assertNotNull(afterDrop.routingTable().index(index).shard(0));
        assertNull(afterDrop.routingTable().index(index).shard(1));
        assertTrue("the copy that stayed must not have been restarted", onlyCopy(afterDrop, 0).started());
    }

    public void testAClosedIndexKeepsItsMetadataToo() {
        // The bug this test exists for: a closed index used to be dropped from the desired state entirely, which
        // the server reads as a deletion. Closing an index would have deleted its data, leaving nothing to reopen.
        ClusterState serving = withShardStarted(build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY))));
        Index index = onlyIndex(serving).getIndex();

        // What the resolver produces for a closed index: no copies at all.
        ClusterState afterClose = build(serving, new DesiredState(43, Map.of(), List.of(), List.of()));

        assertNotNull(afterClose.metadata().index(index));
    }

    public void testAnIndexThisNodeNeverHeldIsNotInvented() {
        // The carry-forward is from the previous state, so it cannot resurrect anything: a fresh process starts
        // with no metadata, and absence at that point is not a transition and deletes nothing.
        ClusterState fresh = build(ClusterState.EMPTY_STATE, new DesiredState(43, Map.of(), List.of(), List.of()));
        assertTrue(fresh.metadata().indices().isEmpty());
    }

    // ---------------------------------------------------------------- empty desired state

    public void testAnEmptyDesiredStateIsAValidStateWithNothingInIt() {
        // What applying "hold nothing" looks like. Not an error: an assignment that names nothing is the one
        // document that means drop everything.
        ClusterState state = build(ClusterState.EMPTY_STATE, new DesiredState(9, Map.of(), List.of(), List.of()));

        assertTrue(state.metadata().indices().isEmpty());
        assertEquals(1, state.nodes().getSize());
    }

    // ---------------------------------------------------------------- the in-sync set

    public void testTheInSyncSetDescribesThisPassAndDoesNotAccumulate() {
        // This killed a node. The set used to carry the previous pass's contents forward and add the current
        // allocation id, so every id ever used for a shard stayed in it -- and routingFor mints a fresh one
        // whenever the previous entry cannot be reused, which a role change does.
        //
        // What it costs: ReplicationTracker tracks a CheckpointState per in-sync id and marks it replicated only
        // when the id is the primary's, because a remote-store-backed index makes assignedToRemoteStoreNode true.
        // With no remote translog -- segments_only -- and DiscoveryNode.isRemoteStoreNode() false without a
        // cluster-state repository, every escape in calculateReplicationGroup's assertion fails and the node exits
        // with "In absence of remote translog store, all tracked shards must have replication mode as
        // LOGICAL_REPLICATION".
        ClusterState first = withShardStarted(build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY))));
        String firstId = onlyCopy(first, 0).allocationId().getId();
        assertEquals(Set.of(firstId), onlyIndex(first).inSyncAllocationIds(0));

        // A role change is the cheapest way to make routingFor mint a new id: it reuses an entry only when the
        // node, liveness and role all match.
        ClusterState second = build(first, desired(Map.of(0, ShardRole.SEARCH_REPLICA)));
        String secondId = onlyCopy(second, 0).allocationId().getId();

        assertNotEquals("the premise of this test: a role change mints a new allocation id", firstId, secondId);
        assertEquals(
            "the in-sync set must describe this pass's copies, not every copy there has ever been",
            Set.of(secondId),
            onlyIndex(second).inSyncAllocationIds(0)
        );
    }

    public void testTheInSyncSetIsStableWhileNothingChanges() {
        // The other half, and the reason the carry-forward looked reasonable: the id must not churn either, or the
        // server has a routing entry whose allocation id its metadata does not believe in.
        ClusterState first = withShardStarted(build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY))));
        ClusterState second = build(first, desired(Map.of(0, ShardRole.PRIMARY)));

        assertEquals(onlyIndex(first).inSyncAllocationIds(0), onlyIndex(second).inSyncAllocationIds(0));
        assertEquals(Set.of(onlyCopy(second, 0).allocationId().getId()), onlyIndex(second).inSyncAllocationIds(0));
        assertTrue("and the pass must still be a fixed point", StateEquivalence.sameContent(first, second));
    }

    // ---------------------------------------------------------------- the ingestion pause

    public void testAPausedIndexFabricatesAPausedIngestionStatus() {
        ClusterState state = build(ClusterState.EMPTY_STATE, desiredWith(pullBased(true), Map.of(0, ShardRole.PRIMARY)));

        assertTrue("the pause is desired state and must reach the metadata", onlyIndex(state).getIngestionStatus().isPaused());
    }

    public void testAnOrdinaryIndexIsNotPausedAndFabricatesWhatTheBuilderWouldHaveDefaulted() {
        // Set unconditionally rather than only for an index with a source, which is only safe because
        // IngestionStatus(false) is exactly the default. If it were not, every index in every deployment would
        // have gained a metadata change on the pass this shipped.
        ClusterState state = build(ClusterState.EMPTY_STATE, desired(Map.of(0, ShardRole.PRIMARY)));

        assertFalse(onlyIndex(state).getIngestionStatus().isPaused());
        assertEquals(IngestionStatus.getDefaultValue(), onlyIndex(state).getIngestionStatus());
    }

    public void testPausingIsSeenByStateEquivalenceOrTheChangeReachesNoPoller() {
        // The load-bearing assertion in this section, and the one that connects this fabrication to the running
        // node. A poller learns about the pause only through an applied cluster state
        // (IndicesClusterStateService.updateShard -> IndexShard.updateShardIngestionState), and this plugin
        // publishes only when sameContent is false. So a pause that this comparison cannot see is a pause that
        // is written, read, fabricated -- and never applied to anything.
        ClusterState running = withShardStarted(build(ClusterState.EMPTY_STATE, desiredWith(pullBased(false), Map.of(0, ShardRole.PRIMARY))));
        ClusterState paused = build(running, desiredWith(pullBased(true), Map.of(0, ShardRole.PRIMARY)));

        assertFalse("flipping the pause must be a change", StateEquivalence.sameContent(running, paused));
        assertTrue(onlyIndex(paused).getIngestionStatus().isPaused());
        // And the copy is left alone: a pause is not a shard movement, so the shard must not be rebuilt into
        // initializing by it. This is D2.2, and getting it wrong would restart ingestion rather than pause it.
        assertTrue(onlyCopy(paused, 0).started());
    }

    public void testAPausedIndexIsAFixedPointAcrossPasses() {
        // The second pass, which is where every defect that reached a running node has been. A pause that
        // fabricated differently on the pass after would publish a state every poll interval for ever.
        ClusterState first = build(ClusterState.EMPTY_STATE, desiredWith(pullBased(true), Map.of(0, ShardRole.PRIMARY)));
        ClusterState second = build(first, desiredWith(pullBased(true), Map.of(0, ShardRole.PRIMARY)));

        // Asserted first, and it is not decoration: without it this test passes on a fabricator that has never
        // heard of the pause, because two unpaused states are a fixed point too. Checking that it failed without
        // the fix is what found that, and it is the second time that step has caught a vacuous test here.
        assertTrue("the fixed point has to be the paused one", onlyIndex(first).getIngestionStatus().isPaused());
        assertTrue("a paused index must converge like any other", StateEquivalence.sameContent(first, second));
        assertEquals(onlyIndex(first).getIngestionStatus(), onlyIndex(second).getIngestionStatus());
        assertEquals("and its metadata version must not move", onlyIndex(first).getVersion(), onlyIndex(second).getVersion());
    }

    public void testResumingIsAsOrdinaryAsPausing() {
        // Nobody should add a latch. The document is the authority in both directions, and the pass after a
        // resume must say so -- one pass, not two, and no memory of having been paused.
        ClusterState paused = withShardStarted(build(ClusterState.EMPTY_STATE, desiredWith(pullBased(true), Map.of(0, ShardRole.PRIMARY))));
        ClusterState resumed = build(paused, desiredWith(pullBased(false), Map.of(0, ShardRole.PRIMARY)));

        assertFalse(onlyIndex(resumed).getIngestionStatus().isPaused());
        assertFalse("and it must be a change, or the resume reaches no poller either", StateEquivalence.sameContent(paused, resumed));
        assertTrue(onlyCopy(resumed, 0).started());
    }

    public void testAPauseOnAnIndexWithNoSourceIsInertRatherThanRefused() {
        // A contradiction the control plane can write, since it flips the flag without consulting the settings.
        // Harmless: the server's own path early-returns on useIngestionSource() == false, so the honest
        // behaviour is to carry the field and let nothing happen -- rather than refuse the document and turn a
        // control-plane mistake into an index that cannot be built at all.
        ClusterState state = build(ClusterState.EMPTY_STATE, desiredWith(
            new IndexConfig("my-index", UUID, 1788969977933L, true, true, settings(), mappings(), 3041),
            Map.of(0, ShardRole.PRIMARY)
        ));

        IndexMetadata index = onlyIndex(state);
        assertTrue(index.getIngestionStatus().isPaused());
        assertFalse("no ingestion source, so nothing will consult it", index.useIngestionSource());
        assertTrue(onlyCopy(state, 0).primary());
    }

    // ---------------------------------------------------------------- fixtures

    /** An index fed by a pull-based source, paused or not. The source is what makes the pause mean anything. */
    private static IndexConfig pullBased(boolean paused) {
        Map<String, String> withSource = new java.util.LinkedHashMap<>(settings());
        withSource.put("index.ingestion_source.type", "FILE");
        withSource.put("index.number_of_shards", "1");
        return new IndexConfig("my-index", UUID, 1788969977933L, true, paused, withSource, mappings(), 3041);
    }

    private static ClusterState build(ClusterState previous, DesiredState desired) {
        return build(previous, desired, LocalShardStore.EMPTY);
    }

    private static ClusterState build(ClusterState previous, DesiredState desired, LocalShardStore shardStore) {
        return new DataNodeState(CLUSTER, localNode(), desired, MappingSource.WRAPPED, shardStore).buildClusterState(previous);
    }

    /**
     * A first application on a node that has a remote store, built from the node attributes an operator would set.
     *
     * <p>The primary is the copy used, because that is the one whose settings the remote store has to reach: a
     * search replica would also carry the search-only block, and the point of these tests is the store rather
     * than the block.
     */
    private static ClusterState buildOnARemoteStoreNode(IndexConfig config) {
        DiscoveryNode node = TestFixtures.remoteStoreNode(LOCAL, "segments");
        return new DataNodeState(
            CLUSTER,
            node,
            desiredWith(config, Map.of(0, ShardRole.PRIMARY)),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY,
            RemoteStore.of(node),
            Set.of()
        ).buildClusterState(ClusterState.EMPTY_STATE);
    }

    private static IndexConfig segmentReplicated() {
        return new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "2", "index.number_of_replicas", "0", "index.replication.type", "SEGMENT"),
            mappings(),
            3041
        );
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
