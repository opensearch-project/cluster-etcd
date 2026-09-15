/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterState;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.MalformedDocumentException;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * When an index this node no longer holds may finally have its files deleted.
 *
 * <p>Every assertion here is about which way to be wrong. The decision is irreversible, so the cases worth the
 * most attention are the ones where the answer is unclear: a store that will not answer, a document that will not
 * parse, an index recreated under the same name.
 */
public class RetainedIndexReaperTests extends OpenSearchTestCase {

    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    /** A state holding one retained index: metadata present, no copy of it wanted. */
    private ClusterState retaining() {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        IndexConfig config = TestFixtures.indexConfig(UUID);
        DesiredState held = new DesiredState(
            1,
            Map.of(key, new DesiredState.DesiredIndex(key, config, Map.of(0, ShardRole.PRIMARY))),
            List.of(),
            List.of()
        );
        ClusterState serving = new DataNodeState(
            new org.opensearch.cluster.ClusterName("clusterless"),
            TestFixtures.localNode("pv1sDMfZRxSTMiW5VrbN9w"),
            held,
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(ClusterState.EMPTY_STATE);

        // And then nothing is wanted, so the index is retained rather than described.
        return new DataNodeState(
            new org.opensearch.cluster.ClusterName("clusterless"),
            TestFixtures.localNode("pv1sDMfZRxSTMiW5VrbN9w"),
            DesiredState.nothing(),
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(serving);
    }

    private static Set<Index> reapAfter(RetainedIndexReaper reaper, ClusterState state, int passes) {
        Set<Index> reapable = Set.of();
        for (int pass = 0; pass < passes; pass++) {
            reapable = reaper.reapable(state, DesiredState.nothing());
        }
        return reapable;
    }

    public void testAnIndexStillInTheStoreIsNeverReaped() {
        // The copy moved elsewhere, or the index was closed. Either way the index exists and its data is worth
        // keeping -- this is the case where deleting would lose something somebody still wants.
        RetainedIndexReaper reaper = new RetainedIndexReaper((tenant, name) -> Optional.of(TestFixtures.indexConfig(UUID)), 3);

        assertTrue(reapAfter(reaper, retaining(), 50).isEmpty());
    }

    public void testAnIndexAbsentFromTheStoreIsReapedAfterTheThreshold() {
        RetainedIndexReaper reaper = new RetainedIndexReaper((tenant, name) -> Optional.empty(), 3);
        ClusterState state = retaining();

        assertTrue("not on the first look", reaper.reapable(state, DesiredState.nothing()).isEmpty());
        assertTrue("nor the second", reaper.reapable(state, DesiredState.nothing()).isEmpty());
        assertEquals("but on the third", 1, reaper.reapable(state, DesiredState.nothing()).size());
    }

    public void testAnIndexRecreatedUnderTheSameNameCountsAsGone() {
        // A document present under the same name may describe a different index, and if it does then the one being
        // retained really is deleted -- the same torn read the resolver checks for, from the other direction.
        IndexConfig different = new IndexConfig(
            "my-index",
            "aDifferentIndexUuid1",
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "1", "index.number_of_replicas", "0"),
            Map.of(),
            9
        );
        RetainedIndexReaper reaper = new RetainedIndexReaper((tenant, name) -> Optional.of(different), 2);

        assertEquals(1, reapAfter(reaper, retaining(), 2).size());
    }

    public void testAStoreThatCannotBeReadIsNotEvidenceOfDeletion() {
        // Treating silence as deletion is how a network blip becomes data loss.
        RetainedIndexReaper reaper = new RetainedIndexReaper((tenant, name) -> {
            throw new IllegalStateException("the store is unreachable");
        }, 2);

        assertTrue(reapAfter(reaper, retaining(), 50).isEmpty());
    }

    public void testAnUnparseableIndexDocumentIsNotEvidenceEither() {
        RetainedIndexReaper reaper = new RetainedIndexReaper((tenant, name) -> {
            throw new MalformedDocumentException("index configuration", "it is not json");
        }, 2);

        assertTrue(reapAfter(reaper, retaining(), 50).isEmpty());
    }

    public void testOneClearLookResetsTheCount() {
        // Which is what stops strikes accumulating across a copy that comes and goes.
        java.util.concurrent.atomic.AtomicBoolean exists = new java.util.concurrent.atomic.AtomicBoolean(false);
        RetainedIndexReaper reaper = new RetainedIndexReaper(
            (tenant, name) -> exists.get() ? Optional.of(TestFixtures.indexConfig(UUID)) : Optional.empty(),
            3
        );
        ClusterState state = retaining();

        reaper.reapable(state, DesiredState.nothing());
        reaper.reapable(state, DesiredState.nothing());
        exists.set(true);
        reaper.reapable(state, DesiredState.nothing());
        exists.set(false);

        assertTrue("the count must have started again", reaper.reapable(state, DesiredState.nothing()).isEmpty());
        assertTrue(reaper.reapable(state, DesiredState.nothing()).isEmpty());
        assertEquals(1, reaper.reapable(state, DesiredState.nothing()).size());
    }

    public void testAnIndexThisPluginDidNotFabricateIsLeftAlone() {
        // No tenant setting means no way to look it up, and no business deleting it.
        org.opensearch.cluster.metadata.IndexMetadata anonymous = org.opensearch.cluster.metadata.IndexMetadata.builder("anonymous")
            .settings(
                org.opensearch.common.settings.Settings.builder()
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                    .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID, "someOtherUuid12345678")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ClusterState state = ClusterState.builder(new org.opensearch.cluster.ClusterName("clusterless"))
            .metadata(org.opensearch.cluster.metadata.Metadata.builder().put(anonymous, false))
            .build();
        RetainedIndexReaper reaper = new RetainedIndexReaper((tenant, name) -> Optional.empty(), 1);

        assertTrue(reaper.reapable(state, DesiredState.nothing()).isEmpty());
    }

    public void testAThresholdBelowOneIsRefused() {
        expectThrows(IllegalArgumentException.class, () -> new RetainedIndexReaper((tenant, name) -> Optional.empty(), 0));
    }
}
