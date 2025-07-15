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
    
    public static String buildIndexConfigPath(String clusterName, String indexName) {
        return clusterName + "/indices/" + indexName + "/conf";
    }
    
    public static String buildShardPlannedAllocationPath(String clusterName, String indexName, int shardId) {
        return clusterName + "/indices/" + indexName + "/shard/" + shardId + "/planned-allocation";
    }
    
   
    public static String buildShardActualAllocationPath(String clusterName, String indexName, int shardId) {
        return clusterName + "/indices/" + indexName + "/shard/" + shardId + "/actual-allocation";
    }
    
} 