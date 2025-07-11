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
 */
public class ETCDPathUtils {
    
    /**
     * Builds the path for a node's actual state in ETCD.
     * The path pattern is: {cluster_name}/search-unit/{node_name}/actual-state
     * 
     * @param clusterName the cluster name
     * @param nodeName the node name
     * @return the constructed path as a string
     */
    public static String buildNodeActualStatePath(String clusterName, String nodeName) {
        return clusterName + "/search-unit/" + nodeName + "/actual-state";
    }
} 