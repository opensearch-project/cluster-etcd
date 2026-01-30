/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

/**
 * Snapshot restore metadata used to build SnapshotRecoverySource.
 */
public record SnapshotRestoreInfo(
    String repository,
    String snapshotName,
    String snapshotUuid,
    String indexUuid,
    Integer shardPathType
) {
}
