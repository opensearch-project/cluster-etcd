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
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code GET /{index}/_settings}, all verified against OpenSearch 3.9.0.
 *
 * <p>The interesting behaviour is the setting-name filter — see {@link GetSettingsHandler} — and the
 * fact that an index filtered down to nothing disappears from the response rather than appearing empty.
 */
class GetSettingsHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long FIXED_MILLIS = 1_760_000_000_000L;

    private Router router;

    @BeforeEach
    void setUp() {
        router = Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(),
            TestContexts.factory(new InMemoryKeyValueStore(), Clock.fixed(Instant.ofEpochMilli(FIXED_MILLIS), ZoneOffset.UTC))
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

    private RestResponse get(String path, Map<String, List<String>> params) {
        return router.dispatch("GET", path, path, params, Map.of(), new byte[0]);
    }

    private JsonNode settings(String path) throws Exception {
        return settings(path, Map.of());
    }

    private JsonNode settings(String path, Map<String, List<String>> params) throws Exception {
        RestResponse response = get(path, params);
        assertEquals(RestStatus.OK, response.status(), path);
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------
    // shape
    // ------------------------------------------------------------------

    @Test
    void answersIndexKeyedSettingsNestedUnderIndex() throws Exception {
        create("my-index", "{\"settings\":{\"number_of_shards\":2,\"refresh_interval\":\"5s\"}}");

        JsonNode index = settings("/my-index/_settings").get("my-index").get("settings").get("index");
        assertEquals("2", index.get("number_of_shards").textValue());
        assertEquals("1", index.get("number_of_replicas").textValue(), "the default is reported too");
        assertEquals("5s", index.get("refresh_interval").textValue());
    }

    /**
     * A real cluster keeps uuid, creation_date and provided_name in an index's settings. Here they are
     * top-level metadata fields, so {@code get_settings} has to render them back in.
     */
    @Test
    void reportsTheServerSuppliedSettings() throws Exception {
        create("my-index", null);

        JsonNode index = settings("/my-index/_settings").get("my-index").get("settings").get("index");
        assertEquals("my-index", index.get("provided_name").textValue());
        assertEquals(Long.toString(FIXED_MILLIS), index.get("creation_date").textValue());
        assertEquals(22, index.get("uuid").textValue().length(), "128-bit base64url, unpadded");
    }

    @Test
    void flatSettingsRendersDottedKeys() throws Exception {
        create("my-index", "{\"settings\":{\"number_of_shards\":2}}");

        JsonNode settings = settings("/my-index/_settings", Map.of("flat_settings", List.of("true")))
            .get("my-index").get("settings");
        assertEquals("2", settings.get("index.number_of_shards").textValue());
        assertFalse(settings.has("index"), "flat means flat, not both");
    }

    // ------------------------------------------------------------------
    // the setting-name filter
    // ------------------------------------------------------------------

    @Test
    void aNameFilterSelectsByExactDottedKey() throws Exception {
        create("my-index", "{\"settings\":{\"number_of_shards\":2}}");

        JsonNode index = settings("/my-index/_settings/index.number_of_shards")
            .get("my-index").get("settings").get("index");
        assertEquals(1, index.size(), "only the requested setting");
        assertEquals("2", index.get("number_of_shards").textValue());
    }

    @Test
    void aNameFilterAcceptsWildcardsAndCommas() throws Exception {
        create("my-index", "{\"settings\":{\"number_of_shards\":2,\"refresh_interval\":\"5s\"}}");

        JsonNode wildcard = settings("/my-index/_settings/index.number_*")
            .get("my-index").get("settings").get("index");
        assertEquals(2, wildcard.size(), "shards and replicas");

        JsonNode list = settings("/my-index/_settings/index.refresh_interval,index.number_of_shards")
            .get("my-index").get("settings").get("index");
        assertEquals(2, list.size());
        assertTrue(list.has("refresh_interval") && list.has("number_of_shards"));
    }

    /**
     * The asymmetry worth knowing: a write may omit the {@code index.} prefix, a filter may not.
     * Verified — the unprefixed form answers a bare {@code {}}.
     */
    @Test
    void aNameFilterDoesNotGetTheImplicitIndexPrefix() throws Exception {
        create("my-index", "{\"settings\":{\"number_of_shards\":2}}");

        assertEquals(0, settings("/my-index/_settings/number_of_shards").size());
    }

    /** An index filtered down to nothing is omitted entirely, and the empty body is still a 200. */
    @Test
    void anIndexWithNoMatchingSettingsIsOmitted() throws Exception {
        create("has-it", "{\"settings\":{\"refresh_interval\":\"5s\"}}");
        create("lacks-it", null);

        JsonNode body = settings("/_settings/index.refresh_interval");
        assertEquals(List.of("has-it"), body.propertyStream().map(Map.Entry::getKey).toList());

        assertEquals(0, settings("/has-it/_settings/index.nope").size(), "a bare {}, not settings:{}");
        assertEquals(RestStatus.OK, get("/has-it/_settings/index.nope", Map.of()).status(),
            "nothing matched, but the index resolved");
    }

    @Test
    void flatSettingsAndANameFilterCombine() throws Exception {
        create("my-index", "{\"settings\":{\"number_of_shards\":2}}");

        JsonNode settings = settings(
            "/my-index/_settings/index.number_of_shards",
            Map.of("flat_settings", List.of("true"))
        ).get("my-index").get("settings");
        assertEquals("{\"index.number_of_shards\":\"2\"}", settings.toString());
    }

    // ------------------------------------------------------------------
    // index expression resolution
    // ------------------------------------------------------------------

    @Test
    void withNoIndexEveryIndexIsReported() throws Exception {
        create("a", null);
        create("b", null);

        assertEquals(List.of("a", "b"), settings("/_settings").propertyStream().map(Map.Entry::getKey).toList());
    }

    @Test
    void aMissingIndexIsIndexNotFound() throws Exception {
        RestResponse response = get("/nope/_settings", Map.of());

        assertEquals(RestStatus.NOT_FOUND, response.status());
        JsonNode error = MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8)).get("error");
        assertEquals("index_not_found_exception", error.get("type").asText());
        assertEquals("no such index [nope]", error.get("reason").asText());
    }

    /** Asymmetric, as everywhere else: a wildcard may match nothing, a concrete name may not. */
    @Test
    void aWildcardMatchingNothingIsAnEmptyTwoHundred() throws Exception {
        create("a", null);

        assertEquals(0, settings("/zzz*/_settings").size());
    }

    @Test
    void hiddenIndicesNeedExpandWildcards() throws Exception {
        create("visible", null);
        create(".hidden", "{\"settings\":{\"index.hidden\":true}}");

        assertEquals(List.of("visible"), settings("/_settings").propertyStream().map(Map.Entry::getKey).toList());
        assertEquals(
            List.of(".hidden", "visible"),
            settings("/_settings", Map.of("expand_wildcards", List.of("all")))
                .propertyStream().map(Map.Entry::getKey).toList()
        );
        assertTrue(settings("/.hidden/_settings").has(".hidden"), "naming it outright always works");
    }

    @Test
    void anAliasResolvesToItsIndices() throws Exception {
        create("my-index", "{\"aliases\":{\"my-alias\":{}}}");

        assertTrue(settings("/my-alias/_settings").has("my-index"), "keyed by index, not by alias");
    }

    @Test
    void anExpressionThatCouldNeverBeAnIndexNameIsABadRequest() {
        assertEquals(RestStatus.BAD_REQUEST, get("/_foo,bar/_settings", Map.of()).status());
    }
}
