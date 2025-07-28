/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd;

/**
 * Utility class for constructing ETCD paths used by the cluster-etcd plugin.
 * These paths align with the standardized control plane and data plane path structure.
 */
public class ETCDPathUtils {
    
    /**
     * Builds the etcd path for search unit configuration.
     *
     * @param clusterName the cluster name
     * @param searchName  the search unit name
     * @return the etcd path for search unit configuration
     */
    public static String buildSearchUnitConfigPath(String clusterName, String searchName) {
        return clusterName + "/search-unit/" + searchName + "/conf";
    }
    
    /**
     * Builds the etcd path for search unit goal state.
     *
     * @param clusterName the cluster name
     * @param searchName  the search unit name
     * @return the etcd path for search unit goal state
     */
    public static String buildSearchUnitGoalStatePath(String clusterName, String searchName) {
        return clusterName + "/search-unit/" + searchName + "/goal-state";
    }
    
    /**
     * Builds the etcd path for search unit actual state.
     *
     * @param clusterName the cluster name
     * @param searchName  the search unit name
     * @return the etcd path for search unit actual state
     */
    public static String buildSearchUnitActualStatePath(String clusterName, String searchName) {
        return clusterName + "/search-unit/" + searchName + "/actual-state";
    }
    
    /**
     * Builds the etcd path for node actual state (heartbeat information).
     *
     * @param clusterName the cluster name
     * @param nodeName    the node name
     * @return the etcd path for node actual state
     */
    public static String buildNodeActualStatePath(String clusterName, String nodeName) {
        return buildSearchUnitActualStatePath(clusterName, nodeName);
    }
    
    /**
     * @deprecated Use buildIndexSettingsPath, buildIndexMappingsPath, or buildIndexOtherPath instead.
     * This method returns the legacy path for complete index metadata blob.
     *
     * @param clusterName the cluster name
     * @param indexName   the index name
     * @return the etcd path for complete index configuration (deprecated)
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
    
    /**
     * Builds the etcd path for planned shard allocation.
     *
     * @param clusterName the cluster name
     * @param indexName   the index name
     * @param shardId     the shard ID
     * @return the etcd path for planned shard allocation
     */
    public static String buildShardPlannedAllocationPath(String clusterName, String indexName, int shardId) {
        return clusterName + "/indices/" + indexName + "/shard/" + shardId + "/planned-allocation";
    }
    
    /**
     * Builds the etcd path for actual shard allocation.
     *
     * @param clusterName the cluster name
     * @param indexName   the index name
     * @param shardId     the shard ID
     * @return the etcd path for actual shard allocation
     */
    public static String buildShardActualAllocationPath(String clusterName, String indexName, int shardId) {
        return clusterName + "/indices/" + indexName + "/shard/" + shardId + "/actual-allocation";
    }
    
} 