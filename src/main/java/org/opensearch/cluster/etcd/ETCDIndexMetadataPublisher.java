/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Utility class for publishing index metadata to etcd in split format.
 * This splits index metadata into separate etcd entries for settings, mappings, and other fields.
 *
 * <p>The control plane can use this to publish index metadata that will be consumed by OpenSearch nodes.
 * Data nodes will fetch both settings and mappings, while coordinator nodes may only need settings.
 */
public class ETCDIndexMetadataPublisher {
    private static final Logger LOGGER = LogManager.getLogger(ETCDIndexMetadataPublisher.class);

    private final Client etcdClient;
    private final String clusterName;

    public ETCDIndexMetadataPublisher(Client etcdClient, String clusterName) {
        this.etcdClient = etcdClient;
        this.clusterName = clusterName;
    }

    /**
     * Publishes complete index metadata to etcd by splitting it into settings, mappings, and other components.
     * This is the main method for control plane to publish index metadata.
     *
     * @param indexMetadata the complete index metadata to publish
     * @throws IOException if serialization fails
     * @throws RuntimeException if etcd operations fail
     */
    public void publishIndexMetadata(IndexMetadata indexMetadata) throws IOException {
        String indexName = indexMetadata.getIndex().getName();

        LOGGER.debug("Publishing split index metadata for index [{}]", indexName);

        // Extract and publish settings (needed by both data nodes and coordinators)
        Settings indexSettings = extractRelevantSettings(indexMetadata.getSettings());
        publishIndexSettings(indexName, indexSettings);

        // Extract and publish mappings (needed by data nodes only)
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata != null) {
            publishIndexMappings(indexName, mappingMetadata);
        }

        // TODO: Extract and publish other metadata fields if needed (aliases for example)
        // publishIndexOther(indexName, otherMetadata);

        LOGGER.debug("Successfully published split index metadata for index [{}]", indexName);
    }

    /**
     * Publishes only index settings to etcd.
     *
     * @param indexName the name of the index
     * @param settings the index settings to publish
     *
     */
    public void publishIndexSettings(String indexName, Settings settings) throws IOException {
        String settingsPath = ETCDPathUtils.buildIndexSettingsPath(clusterName, indexName);

        // Serialize settings to JSON
        ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
        try (XContentBuilder jsonBuilder = XContentType.JSON.contentBuilder(jsonStream)) {
            settings.toXContent(jsonBuilder, Settings.EMPTY_PARAMS);
        }
        byte[] settingsJson = jsonStream.toByteArray();

        // Write to etcd
        publishToEtcd(settingsPath, settingsJson, "settings for index " + indexName);
    }

    /**
     * Publishes only index mappings to etcd.
     *
     * @param indexName the name of the index
     * @param mappingMetadata the mapping metadata to publish
     * @throws IOException if serialization fails
     * @throws RuntimeException if etcd operations fail
     */
    public void publishIndexMappings(String indexName, MappingMetadata mappingMetadata) throws IOException {
        String mappingsPath = ETCDPathUtils.buildIndexMappingsPath(clusterName, indexName);

        // Serialize mappings to JSON
        ByteArrayOutputStream jsonStream = new ByteArrayOutputStream();
        try (XContentBuilder jsonBuilder = XContentType.JSON.contentBuilder(jsonStream)) {
            jsonBuilder.map(mappingMetadata.sourceAsMap());
        }
        byte[] mappingsJson = jsonStream.toByteArray();

        // Write to etcd
        publishToEtcd(mappingsPath, mappingsJson, "mappings for index " + indexName);
    }

    /**
     * Removes index metadata from etcd by deleting all related entries.
     * This should be called when an index is deleted.
     *
     * @param indexName the name of the index to remove
     */
    public void removeIndexMetadata(String indexName) {
        LOGGER.debug("Removing split index metadata for index [{}]", indexName);

        try (KV kvClient = etcdClient.getKVClient()) {
            String settingsPath = ETCDPathUtils.buildIndexSettingsPath(clusterName, indexName);
            String mappingsPath = ETCDPathUtils.buildIndexMappingsPath(clusterName, indexName);

            // Delete all entries concurrently
            CompletableFuture<Void> settingsDelete = kvClient.delete(ByteSequence.from(settingsPath, StandardCharsets.UTF_8))
                .thenApply(response -> null);
            CompletableFuture<Void> mappingsDelete = kvClient.delete(ByteSequence.from(mappingsPath, StandardCharsets.UTF_8))
                .thenApply(response -> null);

            // Wait for all deletions to complete
            CompletableFuture.allOf(settingsDelete, mappingsDelete).get();

            LOGGER.debug("Successfully removed split index metadata for index [{}]", indexName);
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Failed to remove index metadata from etcd for index " + indexName, e);
        }
    }

    /**
     * Extracts relevant settings from the full index settings, filtering out fields that should
     * be populated by the plugin rather than stored in etcd.
     */
    private Settings extractRelevantSettings(Settings originalSettings) {
        Settings.Builder filteredSettings = Settings.builder();

        // Filter out settings that are populated as constants in the plugin
        for (String key : originalSettings.keySet()) {
            // Skip settings that the plugin populates as constants
            if (shouldFilterOutSetting(key)) {
                continue;
            }
            filteredSettings.put(key, originalSettings.get(key));
        }

        return filteredSettings.build();
    }

    /**
     * Determines if a setting should be filtered out (not stored in etcd) because
     * it's populated as a constant in the plugin.
     */
    private boolean shouldFilterOutSetting(String settingKey) {
        // Filter out settings that might be constants in the future
        if (settingKey.startsWith("constant")) { // This is a placeholder for future constants
            return true;
        }
        return false;
    }

    /**
     * Helper method to publish data to etcd.
     */
    private void publishToEtcd(String path, byte[] data, String description) {
        try (KV kvClient = etcdClient.getKVClient()) {
            ByteSequence key = ByteSequence.from(path, StandardCharsets.UTF_8);
            ByteSequence value = ByteSequence.from(data);

            kvClient.put(key, value).get();
            LOGGER.trace("Successfully published {} to etcd path [{}]", description, path);
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Failed to publish " + description + " to etcd", e);
        }
    }
}
