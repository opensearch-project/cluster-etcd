/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;

import java.util.Map;

class ClusterStateUtils {
    static Settings.Builder initializeSettingsBuilder(String indexName, Map<String, Object> settingsMap) {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.loadFromMap(settingsMap);
        settingsBuilder.put(IndexMetadata.SETTING_INDEX_UUID, indexName);
        settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        settingsBuilder.put(IndexMetadata.SETTING_CREATION_DATE, generateDeterministicCreationDate(indexName));
        return settingsBuilder;
    }

    /**
     * Generates a deterministic creation date based on the index name.
     * This ensures all nodes generate the same creation date for the same index.
     */
    private static long generateDeterministicCreationDate(String indexName) {
        // Generate a deterministic timestamp based on index name
        // This ensures all nodes generate the same creation date for the same index
        // Use a fixed epoch time (e.g., 2024-01-01) plus a hash of the index name
        long baseEpoch = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Generate a deterministic offset based on index name hash
        int hashOffset = (indexName.hashCode() & Integer.MAX_VALUE) % (24 * 60 * 60 * 1000);

        return baseEpoch + hashOffset;
    }
}
