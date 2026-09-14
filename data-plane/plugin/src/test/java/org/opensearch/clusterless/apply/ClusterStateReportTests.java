/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterState;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * This milestone's stated deliverable, as an assertion.
 *
 * <p>{@code MILESTONES.md}: <em>"a supervisor in --write mode assigns a shard, and the node logs a complete
 * intended state including index settings, mappings, roles and primary terms — without touching the node."</em>
 * Each of those four is checked below, because "complete" is otherwise a matter of opinion, and the whole point
 * of a read-only milestone is that somebody reads the output and can trust that what is missing from it is
 * missing from the intent.
 */
public class ClusterStateReportTests extends OpenSearchTestCase {

    private static final TenantId ACME = TenantId.of("acme");
    private static final String UUID = "uzKxLWZu4nPSrTknEcDpoA";

    private String report(Map<Integer, ShardRole> shards, List<String> deferred) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(ACME, UUID);
        IndexConfig config = new IndexConfig(
            "my-index",
            UUID,
            1788969977933L,
            true,
            Map.of("index.number_of_shards", "2", "index.number_of_replicas", "0", "index.refresh_interval", "30s"),
            Map.of("properties", Map.of("host", Map.of("type", "keyword"))),
            3041
        );
        DesiredState desired = new DesiredState(
            42,
            Map.of(key, new DesiredState.DesiredIndex(key, config, shards)),
            List.of(TenantId.of("globex")),
            deferred
        );
        ClusterState intended = new DataNodeState(
            TestFixtures.localNode("pv1sDMfZRxSTMiW5VrbN9w"),
            desired,
            MappingSource.WRAPPED,
            LocalShardStore.EMPTY
        ).buildClusterState(ClusterState.EMPTY_STATE);
        return ClusterStateReport.describe(intended, desired);
    }

    public void testTheReportNamesTheAssignmentItCameFrom() {
        String report = report(Map.of(0, ShardRole.PRIMARY), List.of());
        assertTrue(report, report.contains("from assignment version 42"));
        assertTrue(report, report.contains("1 index(es), 1 copy(ies)"));
    }

    public void testTheReportShowsIndexSettings() {
        String report = report(Map.of(0, ShardRole.PRIMARY), List.of());
        assertTrue(report, report.contains("index.refresh_interval=30s"));
        assertTrue(report, report.contains("index.number_of_shards=2"));
    }

    public void testTheReportShowsMappings() {
        String report = report(Map.of(0, ShardRole.PRIMARY), List.of());
        assertTrue(report, report.contains("mappings:"));
        assertTrue(report, report.contains("keyword"));
    }

    public void testTheReportShowsRolesAndStates() {
        String report = report(Map.of(0, ShardRole.PRIMARY, 1, ShardRole.SEARCH_REPLICA), List.of());
        assertTrue(report, report.contains("PRIMARY INITIALIZING"));
        assertTrue(report, report.contains("SEARCH_REPLICA INITIALIZING"));
    }

    public void testTheReportShowsPrimaryTerms() {
        String report = report(Map.of(0, ShardRole.PRIMARY), List.of());
        assertTrue(report, report.contains("primary_term=1"));
    }

    public void testTheReportShowsTheRecoverySourceThatWouldBeUsed() {
        // The field that decides whether applying this would resume a shard or rebuild it from nothing, which is
        // exactly the kind of thing a read-only milestone exists to let somebody check.
        String report = report(Map.of(0, ShardRole.PRIMARY), List.of());
        assertTrue(report, report.contains("recovery=EMPTY_STORE"));
    }

    public void testTheReportShowsTheContractsIdentityRatherThanOnlyTheFabricatedName() {
        // The fabricated index name is not the identity, so a report that showed only the name would be
        // unreadable against the store: nothing over there is keyed on it.
        String report = report(Map.of(0, ShardRole.PRIMARY), List.of());
        assertTrue(report, report.contains("tenant=acme"));
        assertTrue(report, report.contains("name=my-index"));
        assertTrue(report, report.contains("uuid=" + UUID));
        assertTrue(report, report.contains("config_revision=3041"));
    }

    public void testTheReportListsWhatWasNotBuiltAndWhy() {
        String report = report(Map.of(0, ShardRole.PRIMARY), List.of("acme/other: no index document at [other] yet"));
        assertTrue(report, report.contains("not built (1)"));
        assertTrue(report, report.contains("no index document"));
    }

    public void testTheReportNamesTheTenantsItWouldCoordinateFor() {
        String report = report(Map.of(0, ShardRole.PRIMARY), List.of());
        assertTrue(report, report.contains("coordinating for: [globex]"));
    }

    public void testTheIdentitySettingsAreNotRepeatedAmongTheConfiguredOnes() {
        // They are on the identity line already, and repeating them pushes the settings somebody actually chose
        // off the end of a reader's attention.
        String report = report(Map.of(0, ShardRole.PRIMARY), List.of());
        int settingsLine = report.indexOf("settings: ");
        String settings = report.substring(settingsLine, report.indexOf('\n', settingsLine));
        assertFalse(settings, settings.contains("index.clusterless."));
    }
}
