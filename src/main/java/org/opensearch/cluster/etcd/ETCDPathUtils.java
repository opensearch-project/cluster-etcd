/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

/**
 * Utility class for constructing ETCD paths used by the cluster-etcd plugin.
 * These paths align with the standardized control plane and data plane path structure.
 */
public class ETCDPathUtils {

    public static String buildSearchUnitConfigPath(String clusterName, String searchName) {
        return clusterName + "/search-unit/" + searchName + "/conf";
    }

    public static String buildSearchUnitGoalStatePath(String clusterName, String searchName) {
        return clusterName + "/search-unit/" + searchName + "/goal-state";
    }

    public static String buildSearchUnitActualStatePath(String clusterName, String searchName) {
        return clusterName + "/search-unit/" + searchName + "/actual-state";
    }

    public static String buildNodeActualStatePath(String clusterName, String nodeName) {
        return buildSearchUnitActualStatePath(clusterName, nodeName);
    }

    /**
     * @deprecated Use buildIndexSettingsPath, buildIndexMappingsPath, or buildIndexOtherPath instead.
     * This method returns the legacy path for complete index metadata blob.
     */
    @Deprecated
    public static String buildIndexConfigPath(String clusterName, String indexName) {
        return clusterName + "/indices/" + indexName + "/conf";
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
        return clusterName + "/indices/" + indexName + "/settings";
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
        return clusterName + "/indices/" + indexName + "/mappings";
    }

    public static String buildShardPlannedAllocationPath(String clusterName, String indexName, int shardId) {
        return clusterName + "/indices/" + indexName + "/shard/" + shardId + "/planned-allocation";
    }

    public static String buildShardActualAllocationPath(String clusterName, String indexName, int shardId) {
        return clusterName + "/indices/" + indexName + "/shard/" + shardId + "/actual-allocation";
    }

}
