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
 * {@code PUT /{index}/_mapping}, with the conflict rules carried from OpenSearch's own
 * {@code updateable} flags. Both conflict messages were verified against 3.9.0.
 */
class PutMappingHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = Router.fromGeneratedRoutes(ControlPlaneHandlers.build(), TestContexts.factory(store));
    }

    private RestResponse send(String method, String path, String body) {
        return router.dispatch(method, path, path, Map.of(), Map.of(),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8));
    }

    private void create(String index, String body) {
        assertEquals(RestStatus.OK, send("PUT", "/" + index, body).status(), index);
    }

    private RestResponse putMapping(String index, String body) {
        return send("PUT", "/" + index + "/_mapping", body);
    }

    private JsonNode mappings(String index) throws Exception {
        return MAPPER.readTree(store.getAsString("tenant/default/indices/" + index).orElseThrow())
            .get("mappings");
    }

    private String reasonOf(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8))
            .get("error").get("reason").asText();
    }

    // ------------------------------------------------------------------
    // additive changes
    // ------------------------------------------------------------------

    @Test
    void addsANewFieldAndAcknowledges() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}");

        RestResponse response = putMapping("my-index", "{\"properties\":{\"count\":{\"type\":\"long\"}}}");
        assertEquals(RestStatus.OK, response.status());
        assertTrue(MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8))
            .get("acknowledged").asBoolean());

        JsonNode properties = mappings("my-index").get("properties");
        assertEquals("text", properties.get("title").get("type").asText(), "the existing field survives");
        assertEquals("long", properties.get("count").get("type").asText());
    }

    /** Adding under an existing object or an existing field's `fields` is additive, so allowed. */
    @Test
    void addsSubFieldsToExistingFields() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{"
            + "\"title\":{\"type\":\"text\"},\"user\":{\"properties\":{\"name\":{\"type\":\"text\"}}}}}}");

        assertEquals(RestStatus.OK, putMapping("my-index",
            "{\"properties\":{\"title\":{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}}}}").status());
        assertEquals(RestStatus.OK, putMapping("my-index",
            "{\"properties\":{\"user\":{\"properties\":{\"age\":{\"type\":\"integer\"}}}}}").status());

        JsonNode properties = mappings("my-index").get("properties");
        assertEquals("keyword", properties.get("title").get("fields").get("raw").get("type").asText());
        assertEquals("text", properties.get("user").get("properties").get("name").get("type").asText());
        assertEquals("integer", properties.get("user").get("properties").get("age").get("type").asText());
    }

    /** The parameters OpenSearch marks updateable, because they cannot invalidate indexed data. */
    @Test
    void allowsTheUpdateableParameters() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{"
            + "\"k\":{\"type\":\"keyword\",\"ignore_above\":10},\"t\":{\"type\":\"text\"}}}}");

        assertEquals(RestStatus.OK,
            putMapping("my-index", "{\"properties\":{\"k\":{\"type\":\"keyword\",\"ignore_above\":50}}}").status());
        assertEquals(RestStatus.OK,
            putMapping("my-index", "{\"properties\":{\"t\":{\"type\":\"text\",\"meta\":{\"x\":\"1\"}}}}").status());
        assertEquals(RestStatus.OK,
            putMapping("my-index", "{\"properties\":{\"t\":{\"type\":\"text\",\"search_analyzer\":\"simple\"}}}").status());

        JsonNode properties = mappings("my-index").get("properties");
        assertEquals(50, properties.get("k").get("ignore_above").asInt());
        assertEquals("simple", properties.get("t").get("search_analyzer").asText());
    }

    // ------------------------------------------------------------------
    // conflicts
    // ------------------------------------------------------------------

    /** Byte-exact against 3.9.0. */
    @Test
    void rejectsATypeChange() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{\"t\":{\"type\":\"text\"}}}}");

        RestResponse response = putMapping("my-index", "{\"properties\":{\"t\":{\"type\":\"long\"}}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("mapper [t] cannot be changed from type [text] to [long]", reasonOf(response));
        assertEquals("text", mappings("my-index").get("properties").get("t").get("type").asText(),
            "and nothing was written");
    }

    /** Byte-exact against 3.9.0, including the newline and tab in the middle. */
    @Test
    void rejectsAnImmutableParameterChange() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{\"t\":{\"type\":\"text\",\"index\":true}}}}");

        RestResponse response = putMapping("my-index",
            "{\"properties\":{\"t\":{\"type\":\"text\",\"index\":false}}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals(
            "Mapper for [t] conflicts with existing mapper:\n\tCannot update parameter [index] from [true] to [false]",
            reasonOf(response)
        );
    }

    /** A conflict deep in the tree names the field by its full dotted path. */
    @Test
    void reportsNestedConflictsByFullPath() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{\"user\":{\"properties\":"
            + "{\"name\":{\"type\":\"text\"}}}}}}");

        RestResponse response = putMapping("my-index",
            "{\"properties\":{\"user\":{\"properties\":{\"name\":{\"type\":\"long\"}}}}}");
        assertEquals("mapper [user.name] cannot be changed from type [text] to [long]", reasonOf(response));
    }

    /** An unrecognised parameter is treated as immutable — the answer that cannot corrupt an index. */
    @Test
    void treatsUnknownParametersAsImmutable() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{\"t\":{\"type\":\"text\",\"from_a_plugin\":\"a\"}}}}");

        RestResponse response = putMapping("my-index",
            "{\"properties\":{\"t\":{\"type\":\"text\",\"from_a_plugin\":\"b\"}}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertTrue(reasonOf(response).contains("Cannot update parameter [from_a_plugin]"));
    }

    /**
     * Rejects an immutable parameter the stored mapping never declared, because without the mapper's
     * defaults there is no way to tell a real change from a restatement. This is the one place stricter
     * than a real cluster — see {@link org.opensearch.controlplane.metadata.MappingMerge}. It matters
     * because these are exactly the parameters that decide how bytes were written.
     */
    @Test
    void rejectsAnUndeclaredImmutableParameter() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{\"t\":{\"type\":\"text\"},\"k\":{\"type\":\"keyword\"}}}}");

        for (String update : List.of(
            "{\"properties\":{\"t\":{\"type\":\"text\",\"index\":false}}}",
            "{\"properties\":{\"k\":{\"type\":\"keyword\",\"doc_values\":false}}}",
            "{\"properties\":{\"k\":{\"type\":\"keyword\",\"store\":true}}}",
            "{\"properties\":{\"t\":{\"type\":\"text\",\"similarity\":\"boolean\"}}}")) {
            assertEquals(RestStatus.BAD_REQUEST, putMapping("my-index", update).status(), update);
        }
        // No "from [x]" clause, because there is no previous value to name.
        String reason = reasonOf(putMapping("my-index", "{\"properties\":{\"t\":{\"type\":\"text\",\"index\":false}}}"));
        assertEquals("Mapper for [t] conflicts with existing mapper:\n\tCannot update parameter [index] to [false]",
            reason);
    }

    /** Restating a field's declaration unchanged stays a no-op, which is what keeps retries safe. */
    @Test
    void restatingAFieldUnchangedIsAccepted() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{\"t\":{\"type\":\"text\",\"index\":true}}}}");

        assertEquals(RestStatus.OK,
            putMapping("my-index", "{\"properties\":{\"t\":{\"type\":\"text\",\"index\":true}}}").status());
        assertEquals(RestStatus.OK,
            putMapping("my-index", "{\"properties\":{\"t\":{\"type\":\"text\"}}}").status());
    }

    /** norms is updateable on text and fixed on match_only_text — resolved by the field's own type. */
    @Test
    void resolvesPerTypeParametersByFieldType() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{"
            + "\"t\":{\"type\":\"text\",\"norms\":true},"
            + "\"m\":{\"type\":\"match_only_text\",\"norms\":true}}}}");

        assertEquals(RestStatus.OK,
            putMapping("my-index", "{\"properties\":{\"t\":{\"type\":\"text\",\"norms\":false}}}").status());
        assertEquals(RestStatus.BAD_REQUEST, putMapping("my-index",
            "{\"properties\":{\"m\":{\"type\":\"match_only_text\",\"norms\":false}}}").status());
    }

    // ------------------------------------------------------------------
    // multi-index and plumbing
    // ------------------------------------------------------------------

    /** All or nothing: a conflict on the second index must leave the first untouched. */
    @Test
    void aConflictOnAnyIndexLeavesThemAllUnchanged() throws Exception {
        create("idx-a", "{\"mappings\":{\"properties\":{\"t\":{\"type\":\"text\"}}}}");
        create("idx-b", "{\"mappings\":{\"properties\":{\"t\":{\"type\":\"long\"}}}}");

        RestResponse response = send("PUT", "/idx-*/_mapping", "{\"properties\":{\"t\":{\"type\":\"text\"}}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status(), "idx-b conflicts");

        assertFalse(mappings("idx-a").get("properties").has("added"), "idx-a untouched");
        assertEquals("long", mappings("idx-b").get("properties").get("t").get("type").asText());
        assertEquals(0, store.scanPrefix("tenant/default/" + IndexManagementTasks.KEY_PREFIX).size()
            - 2, "only the two create tasks; no update task was registered");
    }

    @Test
    void appliesToEveryResolvedIndex() throws Exception {
        create("idx-a", null);
        create("idx-b", null);

        assertEquals(RestStatus.OK,
            send("PUT", "/idx-*/_mapping", "{\"properties\":{\"shared\":{\"type\":\"long\"}}}").status());
        for (String index : List.of("idx-a", "idx-b")) {
            assertTrue(mappings(index).get("properties").has("shared"), index);
        }
    }

    @Test
    void registersAnUpdateMappingTaskForTheShardManager() throws Exception {
        create("my-index", null);
        putMapping("my-index", "{\"properties\":{\"t\":{\"type\":\"text\"}}}");

        boolean found = store.scanPrefix("tenant/default/" + IndexManagementTasks.KEY_PREFIX).values().stream()
            .map(versioned -> new String(versioned.value(), StandardCharsets.UTF_8))
            .anyMatch(task -> task.contains(IndexManagementTasks.TYPE_UPDATE_MAPPING));
        assertTrue(found, "the data plane has to learn the mapping changed");
    }

    @Test
    void postIsTheSameOperation() throws Exception {
        create("my-index", null);

        assertEquals(RestStatus.OK,
            send("POST", "/my-index/_mapping", "{\"properties\":{\"t\":{\"type\":\"text\"}}}").status());
        assertTrue(mappings("my-index").get("properties").has("t"));
    }

    @Test
    void rejectsAMissingIndexAndAMalformedBody() throws Exception {
        create("my-index", null);

        assertEquals(RestStatus.NOT_FOUND, putMapping("nope", "{\"properties\":{}}").status());
        assertEquals(RestStatus.BAD_REQUEST, putMapping("my-index", "{not json").status());
        assertEquals(RestStatus.BAD_REQUEST, putMapping("my-index", null).status());
    }
}
