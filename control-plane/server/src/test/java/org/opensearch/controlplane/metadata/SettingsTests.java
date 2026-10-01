/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The canonical settings representation.
 *
 * <p>Every rule asserted here was established by probing OpenSearch 3.9.0 rather than read out of the
 * spec, which declares none of it. The probes were run against index creation, all three template
 * kinds and {@code _simulate_index}, and agreed.
 */
class SettingsTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Settings parse(String json) throws Exception {
        return Settings.forIndex(MAPPER.readTree(json));
    }

    private String flat(String json) throws Exception {
        return MAPPER.writeValueAsString(parse(json).toJson(true));
    }

    private String nested(String json) throws Exception {
        return MAPPER.writeValueAsString(parse(json).toJson(false));
    }

    /** All three accepted shapes reach the same canonical form. */
    @Test
    void everyAcceptedShapeNormalisesToTheSameThing() throws Exception {
        String canonical = "{\"index.number_of_shards\":\"2\"}";

        assertEquals(canonical, flat("{\"number_of_shards\":2}"));
        assertEquals(canonical, flat("{\"index.number_of_shards\":2}"));
        assertEquals(canonical, flat("{\"index\":{\"number_of_shards\":2}}"));
    }

    /** Values are text on the wire and in the store, whatever JSON type they arrived as. */
    @Test
    void scalarsAreStringified() throws Exception {
        assertEquals("{\"index.blocks.read_only\":\"true\"}", flat("{\"blocks\":{\"read_only\":true}}"));
        assertEquals("{\"index.refresh_interval\":\"5s\"}", flat("{\"refresh_interval\":\"5s\"}"));
        assertEquals("{\"index.priority\":\"1.5\"}", flat("{\"priority\":1.5}"));
    }

    /** A list stays a list of strings in both renderings, so a value is not always a scalar. */
    @Test
    void listsStayLists() throws Exception {
        String json = "{\"analysis\":{\"analyzer\":{\"my\":{\"filter\":[\"lowercase\",\"stop\"]}}}}";

        assertEquals("{\"index.analysis.analyzer.my.filter\":[\"lowercase\",\"stop\"]}", flat(json));
        assertEquals(
            "{\"index\":{\"analysis\":{\"analyzer\":{\"my\":{\"filter\":[\"lowercase\",\"stop\"]}}}}}",
            nested(json)
        );
    }

    /** Numbers inside a list are stringified like any other value. */
    @Test
    void listElementsAreStringifiedToo() throws Exception {
        assertEquals("{\"index.some.list\":[\"1\",\"2\"]}", flat("{\"some\":{\"list\":[1,2]}}"));
    }

    /** Verified: an explicit null stays a null rather than becoming the string "null". */
    @Test
    void nullStaysNull() throws Exception {
        assertEquals("{\"index.refresh_interval\":null}", flat("{\"refresh_interval\":null}"));
        assertTrue(parse("{\"refresh_interval\":null}").get("index.refresh_interval").isEmpty(),
            "a null reads as unset, which is how a cluster treats it");
    }

    /**
     * Verified in both document orders against 3.9.0: the dotted key wins either way, so this is not
     * last-writer-wins. Objects are descended into before leaves are applied, which is what produces it.
     */
    @Test
    void aDottedKeyBeatsTheNestedSpellingInEitherOrder() throws Exception {
        assertEquals("{\"index.number_of_shards\":\"2\"}",
            flat("{\"index.number_of_shards\":2,\"index\":{\"number_of_shards\":3}}"));
        assertEquals("{\"index.number_of_shards\":\"2\"}",
            flat("{\"index\":{\"number_of_shards\":3},\"index.number_of_shards\":2}"));
    }

    @Test
    void nestedRenderingRebuildsTheObjectTree() throws Exception {
        assertEquals(
            "{\"index\":{\"number_of_shards\":\"2\",\"version\":{\"created\":\"137307827\"}}}",
            nested("{\"number_of_shards\":2,\"version.created\":137307827}")
        );
    }

    /**
     * Re-reading stored metadata goes through {@code forIndex} again, so it has to be a no-op the
     * second time rather than prefixing twice.
     */
    @Test
    void normalisingIsIdempotent() throws Exception {
        Settings once = parse("{\"number_of_shards\":2}");
        Settings twice = Settings.forIndex(once.toJson(true));
        Settings viaNested = Settings.forIndex(once.toJson(false));

        assertEquals(once, twice);
        assertEquals(once, viaNested, "and round-trips through either rendering");
    }

    /** Overriding is key by key on the flat form, so it lands across differing input shapes. */
    @Test
    void mergingOverridesKeyByKeyAcrossShapes() throws Exception {
        Settings base = parse("{\"index\":{\"number_of_replicas\":9},\"number_of_shards\":3}");
        Settings merged = base.mergedWith(parse("{\"number_of_replicas\":5}"));

        assertEquals("5", merged.get("index.number_of_replicas").orElseThrow());
        assertEquals("3", merged.get("index.number_of_shards").orElseThrow(), "untouched keys survive");
        assertEquals(
            "{\"index.number_of_replicas\":\"5\",\"index.number_of_shards\":\"3\"}",
            MAPPER.writeValueAsString(merged.toJson(true)),
            "one key per setting, not one per spelling"
        );
    }

    @Test
    void defaultsFillOnlyWhatIsAbsent() throws Exception {
        Settings settings = parse("{\"number_of_shards\":2}")
            .withDefault("index.number_of_shards", "1")
            .withDefault("index.number_of_replicas", "1");

        assertEquals("2", settings.get("index.number_of_shards").orElseThrow());
        assertEquals("1", settings.get("index.number_of_replicas").orElseThrow());
    }

    /** Values are text, so a reader parses; {@code JsonNode.asBoolean} would read "true" as false. */
    @Test
    void typedReadsParseTheStoredText() throws Exception {
        assertTrue(parse("{\"hidden\":true}").getAsBoolean("index.hidden", false));
        assertFalse(parse("{}").getAsBoolean("index.hidden", false), "absent falls back to the default");
        assertTrue(parse("{\"hidden\":null}").getAsBoolean("index.hidden", true), "so does an explicit null");
    }

    @Test
    void anythingThatIsNotAnObjectIsEmpty() throws Exception {
        for (String json : List.of("null", "[]", "\"x\"", "{}")) {
            assertTrue(parse(json).isEmpty(), json);
        }
    }

    /**
     * Sorted, deliberately. OpenSearch's nested rendering goes through a {@code HashMap} and so is
     * neither sorted nor input-ordered; see the divergence noted on {@link Settings}.
     */
    @Test
    void keysAreRenderedInSortedOrder() throws Exception {
        JsonNode flat = parse("{\"number_of_shards\":1,\"creation_date\":2,\"blocks\":{\"read_only\":true}}")
            .toJson(true);

        assertEquals(
            List.of("index.blocks.read_only", "index.creation_date", "index.number_of_shards"),
            flat.propertyStream().map(entry -> entry.getKey()).toList()
        );
    }
}
