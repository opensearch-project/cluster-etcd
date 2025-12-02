/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import java.util.Map;

public record IndexMetadataComponents(Map<String, Object> settings, Map<String, Object> mappings, Map<String, Object> additionalMetadata) {
}
