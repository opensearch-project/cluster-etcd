/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.clusterless.ClusterlessIndexSettings;
import org.opensearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Renders a fabricated {@link ClusterState} as something an operator can read.
 *
 * <p>This is the whole deliverable of the read-only milestone, so it is worth more than a {@code toString}.
 * The point of introducing a service that can move data by having it explain itself first is that somebody
 * reads the explanation and checks it — which means the output has to show the things that would be wrong in
 * an interesting way, not the things that are easy to print. Hence settings, mappings, roles, primary terms
 * and recovery sources, all of which decide whether an applied state would recover a shard or rebuild it.
 *
 * <p>Deliberately not JSON. It goes to a log at a few hundred bytes per index, where indentation reads better
 * than punctuation, and the machine-readable half of this question is answered by the store's own documents.
 */
public final class ClusterStateReport {

    private ClusterStateReport() {}

    public static String describe(ClusterState state, DesiredState desired) {
        StringBuilder report = new StringBuilder();
        report.append("intended cluster state: version ")
            .append(state.version())
            .append(", from assignment version ")
            .append(desired.assignmentVersion())
            .append(", ")
            .append(desired.indices().size())
            .append(" index(es), ")
            .append(desired.copyCount())
            .append(" copy(ies)");

        if (desired.coordinatorTenants().isEmpty() == false) {
            report.append("\n  coordinating for: ").append(desired.coordinatorTenants());
        }

        for (IndexMetadata index : state.metadata()) {
            describeIndex(report, state, index);
        }

        if (desired.deferred().isEmpty() == false) {
            report.append("\n  not built (").append(desired.deferred().size()).append("):");
            for (String reason : desired.deferred()) {
                report.append("\n    - ").append(reason);
            }
        }
        return report.toString();
    }

    private static void describeIndex(StringBuilder report, ClusterState state, IndexMetadata index) {
        Settings settings = index.getSettings();
        report.append("\n  index ").append(index.getIndex().getName());
        report.append("\n    identity: tenant=")
            .append(ClusterlessIndexSettings.TENANT.get(settings))
            .append(" name=")
            .append(ClusterlessIndexSettings.TENANT_INDEX_NAME.get(settings))
            .append(" uuid=")
            .append(index.getIndexUUID())
            .append(" config_revision=")
            .append(ClusterlessIndexSettings.CONFIG_REVISION.get(settings));
        report.append("\n    metadata: version=")
            .append(index.getVersion())
            .append(" settings_version=")
            .append(index.getSettingsVersion())
            .append(" mapping_version=")
            .append(index.getMappingVersion())
            .append(" shards=")
            .append(index.getNumberOfShards())
            .append(" replicas=")
            .append(index.getNumberOfReplicas());

        // Sorted, and with the identity settings left out: they are on the line above, and repeating them
        // pushes the settings that were actually configured off the end of a reader's attention.
        Map<String, String> configured = new TreeMap<>();
        for (String key : settings.keySet()) {
            if (key.startsWith(ClusterlessIndexSettings.PREFIX) == false) {
                configured.put(key, settings.get(key));
            }
        }
        report.append("\n    settings: ").append(configured);

        report.append("\n    mappings: ").append(index.mapping() == null ? "(none)" : index.mapping().source().string());

        IndexRoutingTable routing = state.routingTable().index(index.getIndex());
        if (routing == null) {
            report.append("\n    routing: (none)");
            return;
        }
        for (IndexShardRoutingTable shard : routing) {
            List<String> copies = new ArrayList<>();
            for (ShardRouting copy : shard) {
                copies.add(describeCopy(copy));
            }
            report.append("\n    shard ")
                .append(shard.shardId().id())
                .append(": primary_term=")
                .append(index.primaryTerm(shard.shardId().id()))
                .append(" in_sync=")
                .append(index.inSyncAllocationIds(shard.shardId().id()))
                .append(" ")
                .append(copies);
        }
    }

    private static String describeCopy(ShardRouting copy) {
        StringBuilder described = new StringBuilder();
        described.append(copy.primary() ? "PRIMARY" : copy.isSearchOnly() ? "SEARCH_REPLICA" : "REPLICA")
            .append(' ')
            .append(copy.state())
            .append(" on ")
            .append(copy.currentNodeId())
            .append(" allocation=")
            .append(copy.allocationId() == null ? "(none)" : copy.allocationId().getId());
        if (copy.unassignedInfo() != null && copy.recoverySource() != null) {
            // The field that decides whether applying this would resume a shard or rebuild it from nothing.
            described.append(" recovery=").append(copy.recoverySource().getType());
        }
        return described.toString();
    }
}
