/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

public enum ShardRole {
    PRIMARY,
    REPLICA,
    SEARCH_REPLICA
}
