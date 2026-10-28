/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The comparison that decides whether a pass publishes anything, on the part of the state that is not an index.
 *
 * <p>Most of what {@code sameContent} is for is covered where the fabrication is tested — a pass that rebuilds a
 * routing entry, or bumps a version for a change that netted to nothing, is caught in {@code DataNodeStateTests}
 * and {@code CoordinatorStateTests}. What is here is the one thing neither of those would notice: a state that
 * differs only outside its indices.
 */
public class StateEquivalenceTests extends OpenSearchTestCase {

    private static final ClusterName CLUSTER = new ClusterName("clusterless");

    public void testAClusterSettingChangeIsAChange() {
        // Left out, this is an instruction that vanishes. ClusterApplierService applies settings from an applied
        // state and only when metadataChanged() -- which only happens on a pass that publishes -- so a state
        // differing only by a cluster setting would compare equal, never be published, and the setting would never
        // reach its update consumer. The node would hold the value in the state it fabricated while the running
        // components did not have it, which is the worst of the three possible outcomes.
        ClusterState cleanupOn = withSettings(Settings.EMPTY);
        ClusterState cleanupOff = withSettings(
            Settings.builder().put("cluster.remote_store.index.segment_metadata.retention.max_count", -1).build()
        );

        assertFalse(StateEquivalence.sameContent(cleanupOn, cleanupOff));
        assertFalse("and in the other direction, since turning it back on commits a deployment", StateEquivalence.sameContent(cleanupOff, cleanupOn));
    }

    public void testTheSameSettingsAreStillAFixedPoint() {
        // The guard on the above: comparing settings must not make every pass differ, which would publish a state
        // per poll interval for ever -- the defect StateEquivalence exists to prevent, reintroduced by its own fix.
        Settings settings = Settings.builder()
            .put("cluster.remote_store.index.segment_metadata.retention.max_count", -1)
            .build();

        assertTrue(StateEquivalence.sameContent(withSettings(settings), withSettings(settings)));
        assertTrue(StateEquivalence.sameContent(withSettings(Settings.EMPTY), withSettings(Settings.EMPTY)));
    }

    public void testAnIndexDifferenceIsStillSeenWhenTheSettingsMatch() {
        // Ordering check, not a new rule: the settings comparison returns early, so it must not shadow the index
        // comparisons that were the original point of this class.
        ClusterState withIndex = ClusterState.builder(CLUSTER)
            .metadata(Metadata.builder().put(index("my-index").build(), false))
            .build();

        assertFalse(StateEquivalence.sameContent(withSettings(Settings.EMPTY), withIndex));
    }

    private static ClusterState withSettings(Settings settings) {
        return ClusterState.builder(CLUSTER).metadata(Metadata.builder().persistentSettings(settings)).build();
    }

    private static IndexMetadata.Builder index(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "uzKxLWZu4nPSrTknEcDpoA")
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            );
    }
}
