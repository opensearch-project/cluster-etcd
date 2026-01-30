/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import java.util.Map;

public record SnapshotRepositoryInfo(String name, String type, Map<String, Object> settings) {
}
