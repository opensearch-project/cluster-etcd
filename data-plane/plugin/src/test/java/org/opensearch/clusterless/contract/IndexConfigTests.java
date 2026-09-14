/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

/**
 * Reading an index document, whose shape has three traps in it.
 *
 * <p>All three are stated in {@code KEY_LAYOUT.md} and all three are silent when got wrong: flat string-valued
 * settings that read as zero, a name-keyed document whose uuid may not match, and a closed index that still has
 * everything an open one does.
 */
public class IndexConfigTests extends OpenSearchTestCase {

    /** The layout document's own example, which is a closed index — deliberately, since that is the odd case. */
    private static final String EXAMPLE = """
        {
          "index": "my-index",
          "uuid": "uzKxLWZu4nPSrTknEcDpoA",
          "creation_date": "1788969977933",
          "state": "close",
          "settings": {
            "index.number_of_replicas": "1",
            "index.number_of_shards": "3",
            "index.replication.type": "DOCUMENT",
            "index.verified_before_close": "true"
          },
          "mappings": { "properties": { "host": { "type": "keyword" } } }
        }""";

    private static IndexConfig read(String json, long revision) {
        return IndexConfig.read(json.getBytes(StandardCharsets.UTF_8), revision);
    }

    public void testTheLayoutsExampleReadsAsItLooks() {
        IndexConfig config = read(EXAMPLE, 3041);

        assertEquals("my-index", config.name());
        assertEquals("uzKxLWZu4nPSrTknEcDpoA", config.uuid());
        // Epoch millis in a string, matching what OpenSearch stores.
        assertEquals(1788969977933L, config.creationDate());
        assertFalse("state: close means not open", config.open());
        assertEquals(3, config.numberOfShards());
        assertEquals(1, config.numberOfReplicas());
        assertEquals("DOCUMENT", config.replication());
        // The revision comes from the store rather than the document, and travels to the inventory.
        assertEquals(3041L, config.revision());
        assertEquals("keyword", ((java.util.Map<?, ?>) ((java.util.Map<?, ?>) config.mappings().get("properties")).get("host")).get("type"));
    }

    public void testSettingsAreStringsAndAreReadAsSuch() {
        // The silent-zero bug: "3" read with something expecting a number gives zero, and an index with zero
        // shards is an index that cannot exist. Every value here is a string, always.
        IndexConfig config = read(EXAMPLE, 1);
        assertEquals("3", config.settings().get("index.number_of_shards"));
        assertEquals("true", config.settings().get("index.verified_before_close"));
        assertEquals(3, config.numberOfShards());
    }

    public void testAMissingShardCountIsRefusedRatherThanGuessed() {
        IndexConfig config = read("{\"index\": \"i\", \"uuid\": \"u\", \"settings\": {}}", 1);
        MalformedDocumentException e = expectThrows(MalformedDocumentException.class, config::numberOfShards);
        assertTrue(e.getMessage(), e.getMessage().contains("index.number_of_shards"));
    }

    public void testAShardCountThatIsNotANumberIsRefused() {
        IndexConfig config = read("{\"index\": \"i\", \"uuid\": \"u\", \"settings\": {\"index.number_of_shards\": \"lots\"}}", 1);
        MalformedDocumentException e = expectThrows(MalformedDocumentException.class, config::numberOfShards);
        assertTrue(e.getMessage(), e.getMessage().contains("not a number"));
    }

    public void testTheUuidIsCheckedAgainstTheAssignmentsRatherThanTrusted() {
        // The one torn read a later pass would not correct. This document is keyed by name; the assignment
        // identifies the index by uuid. An index deleted and recreated under the same name gives a perfectly
        // valid document describing a different index, and building against it would produce a shard reported
        // under one uuid with another index's mappings inside it.
        IndexConfig config = read(EXAMPLE, 1);
        assertTrue(config.matches("uzKxLWZu4nPSrTknEcDpoA"));
        assertFalse(config.matches("aDifferentIndexUuid12"));
    }

    public void testAnOpenIndexIsTheDefaultWhenStateIsAbsent() {
        // Absent state is not a third state. An index the control plane wrote without one is open, which is
        // what every index is until somebody closes it.
        assertTrue(read("{\"index\": \"i\", \"uuid\": \"u\", \"settings\": {}}", 1).open());
        assertTrue(read("{\"index\": \"i\", \"uuid\": \"u\", \"state\": \"open\", \"settings\": {}}", 1).open());
    }

    public void testReplicationDefaultsToSegmentWhenUnset() {
        // The deployment this plugin supports first. A copy that pulls from the remote store resolves no peers,
        // which is what makes it the version that can exist before peer resolution does.
        assertEquals("SEGMENT", read("{\"index\": \"i\", \"uuid\": \"u\", \"settings\": {}}", 1).replication());
    }

    public void testAMissingCreationDateIsSurvivable() {
        // The fabricator derives one from the uuid rather than from the clock. A date from the clock would
        // differ on every node and every restart, which the server reads as a metadata change.
        assertEquals(0L, read("{\"index\": \"i\", \"uuid\": \"u\", \"settings\": {}}", 1).creationDate());
    }

    public void testADocumentWithoutIdentityIsRefused() {
        // Name and uuid are the two things nothing downstream can proceed without.
        expectThrows(MalformedDocumentException.class, () -> read("{\"uuid\": \"u\", \"settings\": {}}", 1));
        expectThrows(MalformedDocumentException.class, () -> read("{\"index\": \"i\", \"settings\": {}}", 1));
        expectThrows(MalformedDocumentException.class, () -> read("{ not json", 1));
    }
}
