/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Each rule against the message the server gives, verified against OpenSearch 3.10.0-SNAPSHOT. */
class IngestionSourceValidationTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static Settings settings(String json) throws Exception {
        return Settings.forIndex(MAPPER.readTree(json));
    }

    private static Optional<String> create(String json) throws Exception {
        return IngestionSourceValidation.checkCreate(settings(json), "DOCUMENT");
    }

    private static String kafka(String extra) {
        return "{\"index.replication.type\":\"SEGMENT\",\"ingestion_source\":{\"type\":\"kafka\""
            + (extra.isEmpty() ? "" : "," + extra) + ",\"param\":{\"topic\":\"t\",\"bootstrap_servers\":\"b:9092\"}}}";
    }

    @Test
    void anIndexWithoutAnIngestionSourceIsNotChecked() throws Exception {
        assertTrue(create("{\"index.number_of_shards\":\"1\"}").isEmpty());
        assertTrue(create("{\"ingestion_source\":{\"type\":\"none\",\"pointer.init.reset\":\"nonsense\"}}").isEmpty());
    }

    @Test
    void aValidSegmentReplicationIndexPasses() throws Exception {
        assertTrue(create(kafka("\"pointer.init.reset\":\"EARLIEST\",\"error_strategy\":\"block\""
            + ",\"mapper_type\":\"field_mapping\"")).isEmpty());
    }

    @Test
    void anUnknownResetStateIsRefused() throws Exception {
        assertEquals("Invalid value for index.ingestion_source.pointer.init.reset [beginning]",
            create(kafka("\"pointer.init.reset\":\"beginning\"")).orElseThrow());
    }

    @Test
    void aResetByOffsetNeedsItsValue() throws Exception {
        assertEquals("Setting index.ingestion_source.pointer.init.reset.value should be set when resetting consumer"
            + " position", create(kafka("\"pointer.init.reset\":\"reset_by_offset\"")).orElseThrow());
        assertTrue(create(kafka("\"pointer.init.reset\":\"reset_by_offset\",\"pointer.init.reset.value\":\"5\""))
            .isEmpty());
    }

    @Test
    void segmentReplicationRefusesAllActive() throws Exception {
        assertEquals("Replication type SEGMENT is not supported in pull-based ingestion when"
            + " index.ingestion_source.all_active is enabled", create(kafka("\"all_active\":true")).orElseThrow());
    }

    @Test
    void documentReplicationRequiresAllActiveAndIsTheDefault() throws Exception {
        String expected = "Replication type DOCUMENT is not supported in pull-based ingestion when"
            + " index.ingestion_source.all_active is not enabled";
        assertEquals(expected, create("{\"ingestion_source\":{\"type\":\"kafka\"}}").orElseThrow(),
            "an index that names no replication type is document-replicated");
        assertEquals(expected,
            create("{\"index.replication.type\":\"DOCUMENT\",\"ingestion_source\":{\"type\":\"kafka\"}}").orElseThrow());
        assertTrue(create("{\"ingestion_source\":{\"type\":\"kafka\",\"all_active\":\"true\"}}").isEmpty());
    }

    @Test
    void allActiveMustBeABoolean() throws Exception {
        assertEquals("Failed to parse value [yes] as only [true] or [false] are allowed.",
            create(kafka("\"all_active\":\"yes\"")).orElseThrow());
    }

    @Test
    void anUnknownErrorStrategyIsRefusedOnCreateAndUpdate() throws Exception {
        assertEquals("Invalid ingestion errorStrategy: skip", create(kafka("\"error_strategy\":\"skip\"")).orElseThrow());
        assertEquals("Invalid ingestion errorStrategy: skip", IngestionSourceValidation.checkUpdate(
            settings("{\"index.ingestion_source.error_strategy\":\"skip\"}")).orElseThrow());
        assertTrue(IngestionSourceValidation.checkUpdate(
            settings("{\"index.ingestion_source.error_strategy\":\"DROP\"}")).isEmpty());
    }

    @Test
    void anUnknownMapperTypeIsRefused() throws Exception {
        assertEquals("Unknown ingestion mapper type: json. Valid values are: default, raw_payload, field_mapping",
            create(kafka("\"mapper_type\":\"json\"")).orElseThrow());
    }
}
