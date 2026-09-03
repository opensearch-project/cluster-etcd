/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code indices.get}, {@code get_mapping}, {@code get_field_mapping} and {@code resolve_index}.
 *
 * <p>All four are reads over the same resolved index expression, so they are tested together — the
 * interesting parts are where their response shapes and their notion of "nothing to report" differ,
 * which is easier to see side by side. Every rule was verified against OpenSearch 3.9.0.
 */
class IndexReadHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Router router;

    @BeforeEach
    void setUp() {
        router = Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(),
            TestContexts.factory(new InMemoryKeyValueStore())
        );
    }

    private void create(String index, String body) {
        String path = "/" + index;
        RestResponse response = router.dispatch(
            "PUT", path, path, Map.of(), Map.of(),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8)
        );
        assertEquals(RestStatus.OK, response.status(), index);
    }

    private RestResponse response(String path, Map<String, List<String>> params) {
        return router.dispatch("GET", path, path, params, Map.of(), new byte[0]);
    }

    private JsonNode get(String path) throws Exception {
        return get(path, Map.of());
    }

    private JsonNode get(String path, Map<String, List<String>> params) throws Exception {
        RestResponse response = response(path, params);
        assertEquals(RestStatus.OK, response.status(), path);
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private List<String> keys(JsonNode node) {
        return node.propertyStream().map(Map.Entry::getKey).toList();
    }

    // ------------------------------------------------------------------
    // indices.get
    // ------------------------------------------------------------------

    @Test
    void getAnswersAliasesMappingsAndSettingsInThatOrder() throws Exception {
        create("my-index", "{\"settings\":{\"number_of_shards\":2},"
            + "\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"}}},"
            + "\"aliases\":{\"my-alias\":{}}}");

        JsonNode entry = get("/my-index").get("my-index");
        assertEquals(List.of("aliases", "mappings", "settings"), keys(entry));
        assertTrue(entry.get("aliases").has("my-alias"));
        assertEquals("text", entry.get("mappings").get("properties").get("title").get("type").asText());
        assertEquals("2", entry.get("settings").get("index").get("number_of_shards").textValue());
    }

    /** Every block is present even when empty — the opposite of get_settings' omit-when-empty rule. */
    @Test
    void getAlwaysEmitsAllThreeBlocks() throws Exception {
        create("bare", null);

        JsonNode entry = get("/bare").get("bare");
        assertEquals(List.of("aliases", "mappings", "settings"), keys(entry));
        assertEquals(0, entry.get("aliases").size());
        assertEquals(0, entry.get("mappings").size());
    }

    @Test
    void getHonoursFlatSettings() throws Exception {
        create("my-index", "{\"settings\":{\"number_of_shards\":2}}");

        JsonNode settings = get("/my-index", Map.of("flat_settings", List.of("true")))
            .get("my-index").get("settings");
        assertEquals("2", settings.get("index.number_of_shards").textValue());
    }

    @Test
    void getReportsAMissingIndexAsIndexNotFound() throws Exception {
        RestResponse response = response("/nope", Map.of());

        assertEquals(RestStatus.NOT_FOUND, response.status());
        JsonNode error = MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8)).get("error");
        assertEquals("index_not_found_exception", error.get("type").asText());
    }

    // ------------------------------------------------------------------
    // indices.get_mapping
    // ------------------------------------------------------------------

    @Test
    void getMappingIsIndexKeyedAndKeepsEmptyMappings() throws Exception {
        create("mapped", "{\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}");
        create("bare", null);

        JsonNode body = get("/_mapping");
        assertEquals(List.of("bare", "mapped"), keys(body));
        assertEquals(0, body.get("bare").get("mappings").size(), "still reported, just empty");
        assertEquals("text",
            body.get("mapped").get("mappings").get("properties").get("title").get("type").asText());
    }

    // ------------------------------------------------------------------
    // indices.get_field_mapping
    // ------------------------------------------------------------------

    @Test
    void fieldMappingKeysByFullNameAndNestsByLeafName() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{\"user\":{\"properties\":"
            + "{\"name\":{\"type\":\"text\"}}}}}}");

        JsonNode field = get("/my-index/_mapping/field/user.name").get("my-index").get("mappings").get("user.name");
        assertEquals("user.name", field.get("full_name").asText());
        // The two names differ: dotted outside, leaf inside.
        assertEquals("text", field.get("mapping").get("name").get("type").asText());
        assertFalse(field.get("mapping").has("user.name"));
    }

    /** Multi-fields are leaves in their own right; object fields are not fields at all. */
    @Test
    void multiFieldsResolveAndObjectFieldsDoNot() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{"
            + "\"title\":{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}},"
            + "\"user\":{\"properties\":{\"name\":{\"type\":\"text\"}}}}}}");

        JsonNode raw = get("/my-index/_mapping/field/title.raw").get("my-index").get("mappings").get("title.raw");
        assertEquals("title.raw", raw.get("full_name").asText());
        assertEquals("keyword", raw.get("mapping").get("raw").get("type").asText());

        assertEquals(0, get("/my-index/_mapping/field/user").get("my-index").get("mappings").size(),
            "an object field matches nothing");
    }

    @Test
    void fieldMappingTakesWildcardsAndCommas() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{"
            + "\"count\":{\"type\":\"long\"},\"title\":{\"type\":\"text\"},\"other\":{\"type\":\"long\"}}}}");

        assertEquals(List.of("count", "title"),
            keys(get("/my-index/_mapping/field/count,title").get("my-index").get("mappings")).stream().sorted().toList());
        assertEquals(3, get("/my-index/_mapping/field/*").get("my-index").get("mappings").size(),
            "declared fields only; metadata fields are a documented gap");
    }

    /** A field matching nothing is a 200 with the index present and an empty mappings block. */
    @Test
    void anUnmatchedFieldStillReportsTheIndex() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}");

        JsonNode body = get("/my-index/_mapping/field/nope");
        assertEquals(List.of("my-index"), keys(body));
        assertEquals(0, body.get("my-index").get("mappings").size());
    }

    // ------------------------------------------------------------------
    // indices.resolve_index
    // ------------------------------------------------------------------

    @Test
    void resolveSortsNamesIntoIndicesAndAliases() throws Exception {
        create("my-index", "{\"aliases\":{\"my-alias\":{}}}");

        JsonNode byIndex = get("/_resolve/index/my-index");
        assertEquals(List.of("indices", "aliases", "data_streams"), keys(byIndex));
        JsonNode entry = byIndex.get("indices").get(0);
        assertEquals(List.of("name", "aliases", "attributes"), keys(entry));
        assertEquals("my-index", entry.get("name").asText());
        assertEquals("my-alias", entry.get("aliases").get(0).asText());
        assertEquals(List.of("open"), List.of(entry.get("attributes").get(0).asText()));
        assertEquals(0, byIndex.get("aliases").size());

        // An alias stays an alias here, where every other endpoint flattens it to its indices.
        JsonNode byAlias = get("/_resolve/index/my-alias");
        assertEquals(0, byAlias.get("indices").size());
        assertEquals("my-alias", byAlias.get("aliases").get(0).get("name").asText());
        assertEquals("my-index", byAlias.get("aliases").get(0).get("indices").get(0).asText());
    }

    @Test
    void anIndexWithNoAliasesOmitsTheAliasesKey() throws Exception {
        create("bare", null);

        JsonNode entry = get("/_resolve/index/bare").get("indices").get(0);
        assertEquals(List.of("name", "attributes"), keys(entry), "omitted, not empty");
    }

    /**
     * Three ways this differs from every other index expression, all verified: {@code _all} is a
     * literal name, nothing is ever an error, and a hidden index needs expansion or a dotted pattern.
     */
    @Test
    void resolveTreatsAllAsALiteralAndNeverErrors() throws Exception {
        create("my-index", null);

        for (String expression : List.of("_all", "zzz-nope", "_foo")) {
            JsonNode body = get("/_resolve/index/" + expression);
            assertEquals(0, body.get("indices").size(), expression);
            assertEquals(0, body.get("aliases").size(), expression);
            assertEquals(0, body.get("data_streams").size(), expression);
        }
    }

    @Test
    void resolveReportsHiddenAttributesAndHonoursExpansion() throws Exception {
        create("visible", null);
        create(".hidden", "{\"settings\":{\"index.hidden\":true}}");

        assertEquals(1, get("/_resolve/index/*").get("indices").size(), "hidden left out");
        assertEquals(2, get("/_resolve/index/*", Map.of("expand_wildcards", List.of("all")))
            .get("indices").size());

        JsonNode hidden = get("/_resolve/index/.hidden").get("indices").get(0);
        assertEquals(List.of("hidden", "open"),
            List.of(hidden.get("attributes").get(0).asText(), hidden.get("attributes").get(1).asText()));
    }
}
