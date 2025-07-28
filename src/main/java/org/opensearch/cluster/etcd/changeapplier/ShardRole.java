/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd.changeapplier;

/**
 * Defines the role that a shard can play on a node in the cluster.
 */
public enum ShardRole {
    /** The primary shard that handles indexing and search operations */
    PRIMARY,
    /** A replica shard that handles search operations and provides redundancy */
    REPLICA,
    /** A search-only replica that handles search operations but not indexing */
    SEARCH_REPLICA
}
