/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import org.opensearch.cluster.node.DiscoveryNode;

/**
 * Utility class for constructing ETCD paths used by the cluster-etcd plugin.
 * These paths align with the standardized control plane and data plane path structure.
 */
public class ETCDPathUtils {
    private static final String DEFAULT_SEARCH_UNIT_GROUP = "search-unit";
    private static final String SEARCH_UNIT_GROUP_ATTRIBUTE = "search_unit_group";
    private static final String SEARCH_UNIT_NAME_ATTRIBUTE = "search_unit";

    public static String buildSearchUnitGoalStatePath(DiscoveryNode discoveryNode, String clusterName) {
        String searchUnitGroup = getSearchUnitGroup(discoveryNode, clusterName);
        String searchUnit = getSearchUnit(discoveryNode, clusterName);
        return String.join("/", "", clusterName, searchUnitGroup, searchUnit, "goal-state");
    }

    public static String buildSearchUnitActualStatePath(DiscoveryNode discoveryNode, String clusterName) {
        String searchUnitGroup = getSearchUnitGroup(discoveryNode, clusterName);
        return String.join("/", "", clusterName, searchUnitGroup, discoveryNode.getName(), "actual-state");
    }

    public static String buildSearchUnitActualStatePath(String clusterName, String nodeName) {
        // TODO - Decide ActualStatePath for coordinators
        return String.join("/", "", clusterName, DEFAULT_SEARCH_UNIT_GROUP, nodeName, "actual-state");
    }

    /**
     * @deprecated Use buildIndexSettingsPath, buildIndexMappingsPath, or buildIndexOtherPath instead.
     * This method returns the legacy path for complete index metadata blob.
     */
    @Deprecated
    public static String buildIndexConfigPath(String clusterName, String indexName) {
        return "/" + clusterName + "/indices/" + indexName + "/conf";
    }

    /**
     * Builds the etcd path for index settings.
     * Index settings are needed by both data nodes and coordinator nodes.
     *
     * @param clusterName the cluster name
     * @param indexName the index name
     * @return the etcd path for index settings
     */
    public static String buildIndexSettingsPath(String clusterName, String indexName) {
        return "/" + clusterName + "/indices/" + indexName + "/settings";
    }

    /**
     * Builds the etcd path for index mappings.
     * Index mappings are needed by data nodes only.
     *
     * @param clusterName the cluster name
     * @param indexName the index name
     * @return the etcd path for index mappings
     */
    public static String buildIndexMappingsPath(String clusterName, String indexName) {
        return "/" + clusterName + "/indices/" + indexName + "/mappings";
    }

    /**
     * Builds the etcd path for index restore metadata.
     * This metadata is used to trigger snapshot-based recovery in cluster-etcd.
     *
     * @param clusterName the cluster name
     * @param indexName the index name
     * @return the etcd path for index restore metadata
     */
    public static String buildIndexRestorePath(String clusterName, String indexName) {
        return "/" + clusterName + "/indices/" + indexName + "/restore";
    }

    /**
     * Builds the etcd path for repository metadata.
     * This metadata is used to register snapshot repositories in clusterless mode.
     *
     * @param clusterName the cluster name
     * @param repositoryName the repository name
     * @return the etcd path for repository metadata
     */
    public static String buildRepositoryPath(String clusterName, String repositoryName) {
        return "/" + clusterName + "/repositories/" + repositoryName;
    }

    public static String buildShardPlannedAllocationPath(String clusterName, String indexName, int shardId) {
        return "/" + clusterName + "/indices/" + indexName + "/shard/" + shardId + "/planned-allocation";
    }

    public static String buildShardActualAllocationPath(String clusterName, String indexName, int shardId) {
        return "/" + clusterName + "/indices/" + indexName + "/shard/" + shardId + "/actual-allocation";
    }

    private static String getSearchUnitGroup(DiscoveryNode discoveryNode, String clusterName) {
        return discoveryNode.getAttributes()
            .getOrDefault(
                discoveryNode.getName() + "." + SEARCH_UNIT_GROUP_ATTRIBUTE, // Key: Try node-specific first
                discoveryNode.getAttributes().getOrDefault(SEARCH_UNIT_GROUP_ATTRIBUTE, DEFAULT_SEARCH_UNIT_GROUP)// DefaultValue: Fall back
                                                                                                                  // to generic key
            );
    }

    private static String getSearchUnit(DiscoveryNode discoveryNode, String clusterName) {
        return discoveryNode.getAttributes()
            .getOrDefault(
                discoveryNode.getName() + "." + SEARCH_UNIT_NAME_ATTRIBUTE, // Key: Try node-specific first
                discoveryNode.getAttributes().getOrDefault(SEARCH_UNIT_NAME_ATTRIBUTE, discoveryNode.getName())// DefaultValue: Fall back to
                                                                                                               // generic key
            );
    }
}
