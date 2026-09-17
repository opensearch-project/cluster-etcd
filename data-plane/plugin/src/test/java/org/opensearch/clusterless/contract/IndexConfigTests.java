/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

/**
 * Reading an index document, whose shape has four traps in it.
 *
 * <p>All four are stated in {@code KEY_LAYOUT.md} and all four are silent when got wrong: flat string-valued
 * settings that read as zero, a name-keyed document whose uuid may not match, a closed index that still has
 * everything an open one does, and an ingestion pause that is not in the settings block where one would look
 * for it.
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

    // ---------------------------------------------------------------- the ingestion pause

    public void testTheIngestionPauseIsReadFromTheFirstClassField() {
        // A sibling of state, not a setting, because that is what it is upstream. The control plane writes a
        // real JSON boolean here -- only the settings block is all-strings.
        String paused = """
            {
              "index": "logs", "uuid": "u", "state": "open",
              "ingestion_status": { "is_paused": true },
              "settings": { "index.ingestion_source.type": "kafka" }
            }""";
        assertTrue(read(paused, 1).ingestionPaused());
    }

    public void testAnAbsentIngestionStatusMeansNotPaused() {
        // What makes every index document written before the pause existed still legal, and the reason the
        // control plane needs no migration.
        assertFalse(read("{\"index\": \"i\", \"uuid\": \"u\", \"settings\": {}}", 1).ingestionPaused());
        assertFalse(read(EXAMPLE, 1).ingestionPaused());
    }

    public void testResumingIsWrittenAsAnExplicitFalseRatherThanByRemovingTheField() {
        // What the control plane actually does: it sets the field on both operations, so a resumed index carries
        // ingestion_status with is_paused false rather than losing the field. Both must read as not paused, and
        // this is the case that would be missed by testing only the absent one.
        String resumed = """
            {"index": "logs", "uuid": "u", "ingestion_status": { "is_paused": false }, "settings": {}}""";
        assertFalse(read(resumed, 1).ingestionPaused());
    }

    public void testThePauseIsNotLookedForInTheSettingsBlock() {
        // The trap, asserted so that nobody "fixes" the reader by making the obvious guess work. A setting
        // named this way is not the pause, and treating it as one would let a tenant pause their own ingestion
        // by writing a setting -- desired state arriving through a channel the control plane does not own.
        String misplaced = """
            {"index": "logs", "uuid": "u", "settings": { "index.ingestion.paused": "true" }}""";
        assertFalse(read(misplaced, 1).ingestionPaused());
    }

    public void testAStringValuedPauseIsStillRead() {
        // Not what the control plane writes, and cheap to accept: a hand-edited document during an incident is
        // the likely source, and "true" meaning false would be a confusing way to discover the difference.
        String stringly = """
            {"index": "logs", "uuid": "u", "ingestion_status": { "is_paused": "true" }, "settings": {}}""";
        assertTrue(read(stringly, 1).ingestionPaused());
    }

    public void testAnUnreadablePauseLeavesTheStreamAsItWas() {
        // The opposite choice from the shard count, and deliberate: there is no unparseable boolean worse than
        // the default. A shard count of zero breaks an index; a pause that reads as "not paused" leaves the
        // stream doing exactly what it was already doing.
        String nonsense = """
            {"index": "logs", "uuid": "u", "ingestion_status": { "is_paused": "sometimes" }, "settings": {}}""";
        assertFalse(read(nonsense, 1).ingestionPaused());
        String wrongType = """
            {"index": "logs", "uuid": "u", "ingestion_status": "paused", "settings": {}}""";
        assertFalse(read(wrongType, 1).ingestionPaused());
    }
}
