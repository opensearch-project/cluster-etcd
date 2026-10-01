/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.tenant.HeaderTenantResolver;
import org.opensearch.controlplane.tenant.SingleTenantResolver;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store's key and value limits, surfaced as client errors and checked before anything is written.
 *
 * <p>{@link InMemoryKeyValueStore} enforces the same limits every backend does, so a write that slipped
 * past these checks would fail here as it would in production.
 */
class StoreLimitsTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TENANT = SingleTenantResolver.DEFAULT_TENANT.keyPrefix();
    private static final String TOO_LONG = "a".repeat(256);

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

    private JsonNode error(RestResponse response) throws Exception {
        assertEquals(RestStatus.BAD_REQUEST, response.status(), () -> new String(response.body(), StandardCharsets.UTF_8));
        return MAPPER.readTree(response.body()).get("error");
    }

    private boolean stored(String key) {
        return store.get(TENANT + key).isPresent();
    }

    private int storedSize(String key) {
        return store.get(TENANT + key).orElseThrow().value().length;
    }

    // ------------------------------------------------------------------
    // names that become key segments
    // ------------------------------------------------------------------

    /** Upstream's own rule: an alias name is held to the index-name rules, length included. */
    @Test
    void anAliasNameOverTheLimitIsRejectedAsUpstreamDoes() throws Exception {
        send("PUT", "/logs", null);

        JsonNode error = error(send("POST", "/_aliases",
            "{\"actions\":[{\"add\":{\"index\":\"logs\",\"alias\":\"" + TOO_LONG + "\"}}]}"));
        assertEquals("invalid_alias_name_exception", error.get("type").asText());
        assertEquals("Invalid alias name [" + TOO_LONG + "]: index name is too long, (256 > 255)",
            error.get("reason").asText());
    }

    /** Bytes, not characters: 128 two-byte characters are 256 bytes. */
    @Test
    void theLimitCountsUtf8Bytes() throws Exception {
        send("PUT", "/logs", null);

        JsonNode error = error(send("PUT", "/logs/_alias/" + "é".repeat(128), null));
        assertTrue(error.get("reason").asText().endsWith("index name is too long, (256 > 255)"));
    }

    /** The other shared rules come with it; case does not, being an index-only rule. */
    @Test
    void aliasNamesFollowTheSharedRulesButMayBeUpperCase() throws Exception {
        send("PUT", "/logs", null);

        assertEquals("Invalid alias name [a*]: must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?]",
            error(send("PUT", "/logs/_alias/a*", null)).get("reason").asText());
        assertEquals(RestStatus.OK, send("PUT", "/logs/_alias/Logs", null).status());
    }

    @Test
    void createRejectsAnAliasNameOverTheLimitAndWritesNothing() throws Exception {
        JsonNode error = error(send("PUT", "/logs", "{\"aliases\":{\"" + TOO_LONG + "\":{}}}"));
        assertEquals("invalid_alias_name_exception", error.get("type").asText());
        assertFalse(stored("indices/logs"));
    }

    /** A template's aliases become alias keys when an index is created from it, so they are checked at put. */
    @Test
    void aTemplateDeclaringAnInvalidAliasIsRejected() throws Exception {
        JsonNode error = error(send("PUT", "/_index_template/t",
            "{\"index_patterns\":[\"x-*\"],\"template\":{\"aliases\":{\"" + TOO_LONG + "\":{}}}}"));
        assertEquals("invalid_alias_name_exception", error.get("type").asText());
        assertFalse(stored("templates/index/t"));
    }

    @Test
    void aTemplateNameOverTheLimitIsRejectedForEveryKind() throws Exception {
        for (String path : List.of("/_index_template/", "/_component_template/", "/_template/")) {
            String body = path.equals("/_component_template/") ? "{\"template\":{}}" : "{\"index_patterns\":[\"x-*\"]}";
            JsonNode error = error(send("PUT", path + TOO_LONG, body));
            assertEquals("invalid_index_template_exception", error.get("type").asText(), path);
            assertEquals("index_template [" + TOO_LONG + "] invalid, cause [Validation Failed: 1: name is too long, "
                + "(256 > 255);]", error.get("reason").asText(), path);
        }
    }

    /** Upstream's message for its own 512-byte cap, with this service's 255 in it. */
    @Test
    void aPipelineIdOverTheLimitIsRejectedInUpstreamsWords() throws Exception {
        assertEquals("Pipeline id [" + TOO_LONG + "] exceeds maximum length of 255 UTF-8 bytes (actual: 256 bytes)",
            error(send("PUT", "/_ingest/pipeline/" + TOO_LONG, "{\"processors\":[]}")).get("reason").asText());
        assertEquals("Search Pipeline id [" + TOO_LONG + "] exceeds maximum length of 255 UTF-8 bytes (actual: 256 bytes)",
            error(send("PUT", "/_search/pipeline/" + TOO_LONG, "{}")).get("reason").asText());
    }

    @Test
    void aStoredScriptIdOverTheLimitIsRejected() throws Exception {
        JsonNode error = error(send("PUT", "/_scripts/" + TOO_LONG,
            "{\"script\":{\"lang\":\"painless\",\"source\":\"1\"}}"));
        assertEquals("action_request_validation_exception", error.get("type").asText());
        assertEquals("Validation Failed: 1: id is too long, (256 > 255) for stored script;", error.get("reason").asText());
    }

    @Test
    void aRepositoryNameOverTheLimitIsRejected() throws Exception {
        JsonNode error = error(send("PUT", "/_snapshot/" + TOO_LONG, "{\"type\":\"fs\"}"));
        assertEquals("repository_exception", error.get("type").asText());
        assertEquals("[" + TOO_LONG + "] is too long, (256 > 255)", error.get("reason").asText());
    }

    /** The backing index is an index, so a stream name within 11 bytes of the limit fails on its .ds- name. */
    @Test
    void aDataStreamWhoseBackingIndexNameIsTooLongIsRejected() throws Exception {
        send("PUT", "/_index_template/ds", "{\"index_patterns\":[\"s*\"],\"data_stream\":{}}");
        String name = "s".repeat(250);

        JsonNode error = error(send("PUT", "/_data_stream/" + name, null));
        assertEquals("invalid_index_name_exception", error.get("type").asText());
        assertEquals("Invalid index name [.ds-" + name + "-000001], index name is too long, (261 > 255)",
            error.get("reason").asText());
        assertFalse(stored("data_streams/" + name));
    }

    /**
     * The point of 255: every resource named at the limit, under the longest tenant id, still has a key the
     * store accepts. The store would refuse the write otherwise, so this needs no knowledge of the layout.
     */
    @Test
    void everyNameAtTheLimitFitsUnderTheLongestTenant() {
        Router multiTenant = Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(), TestContexts.factory(store, new HeaderTenantResolver()));
        Map<String, String> tenant = Map.of(HeaderTenantResolver.DEFAULT_HEADER, "t".repeat(128));
        String name = "a".repeat(255);
        Map<String, String> puts = new java.util.LinkedHashMap<>();
        puts.put("/" + name, "{}");
        puts.put("/" + name + "/_alias/" + "b".repeat(255), "");
        puts.put("/_index_template/" + name, "{\"index_patterns\":[\"zz-*\"]}");
        puts.put("/_component_template/" + name, "{\"template\":{}}");
        puts.put("/_template/" + name, "{\"index_patterns\":[\"yy-*\"]}");
        puts.put("/_ingest/pipeline/" + name, "{\"processors\":[]}");
        puts.put("/_search/pipeline/" + name, "{}");
        puts.put("/_scripts/" + name, "{\"script\":{\"lang\":\"painless\",\"source\":\"1\"}}");
        puts.put("/_snapshot/" + name, "{\"type\":\"fs\"}");

        puts.forEach((path, body) -> {
            RestResponse response = multiTenant.dispatch("PUT", path, path, Map.of(), tenant,
                body.getBytes(StandardCharsets.UTF_8));
            assertEquals(RestStatus.OK, response.status(),
                () -> path.substring(0, 20) + ": " + new String(response.body(), StandardCharsets.UTF_8));
        });
    }

    // ------------------------------------------------------------------
    // document sizes, checked before any write
    // ------------------------------------------------------------------

    /** A string of the given length, padded into a mapping's _meta to grow an index document. */
    private static String metaPad(int length) {
        return "{\"_meta\":{\"pad\":\"" + "x".repeat(length) + "\"}}";
    }

    @Test
    void anIndexOverTheLimitIsRejectedAndNothingIsWritten() throws Exception {
        JsonNode error = error(send("PUT", "/logs",
            "{\"mappings\":" + metaPad(KeyValueStore.MAX_VALUE_BYTES) + ",\"aliases\":{\"all\":{}}}"));
        assertEquals("illegal_argument_exception", error.get("type").asText());
        assertTrue(error.get("reason").asText().startsWith("index [logs] would be "), error.get("reason").asText());
        assertTrue(error.get("reason").asText().endsWith(" bytes, over the limit of " + KeyValueStore.MAX_VALUE_BYTES
            + " bytes"));
        assertFalse(stored("indices/logs"));
        assertFalse(stored("aliases/all"));
        assertTrue(store.scanPrefix(TENANT + "tasks/").isEmpty());
    }

    /** One index growing past the limit refuses the whole put_mapping, including the index sorted first. */
    @Test
    void putMappingOverSeveralIndicesIsRefusedWhole() throws Exception {
        send("PUT", "/a", null);
        send("PUT", "/b", "{\"mappings\":" + metaPad(600 * 1024) + "}");
        int before = storedSize("indices/a");

        error(send("PUT", "/a,b/_mapping", "{\"_meta\":{\"more\":\"" + "y".repeat(400 * 1024) + "\"}}"));
        assertEquals(before, storedSize("indices/a"), "a would have fitted, and is untouched");
    }

    @Test
    void putSettingsOverSeveralIndicesIsRefusedWhole() throws Exception {
        send("PUT", "/a", null);
        send("PUT", "/b", "{\"mappings\":" + metaPad(600 * 1024) + "}");
        int before = storedSize("indices/a");

        error(send("PUT", "/a,b/_settings", "{\"index\":{\"pad\":\"" + "z".repeat(400 * 1024) + "\"}}"));
        assertEquals(before, storedSize("indices/a"));
    }

    /**
     * Close adds a setting, so an index just under the limit can go over. Close used to write each index as
     * it went; it now checks them all first.
     */
    @Test
    void closeOverSeveralIndicesIsRefusedWhole() throws Exception {
        send("PUT", "/a", null);
        send("PUT", "/b", "{\"mappings\":" + metaPad(1) + "}");
        int padded = KeyValueStore.MAX_VALUE_BYTES - 10 - storedSize("indices/b") + 1;
        assertEquals(RestStatus.OK, send("PUT", "/b/_mapping", metaPad(padded)).status());
        assertEquals(KeyValueStore.MAX_VALUE_BYTES - 10, storedSize("indices/b"));

        error(send("POST", "/a,b/_close", null));
        assertEquals(RestStatus.OK, send("GET", "/a/_settings", null).status());
        assertTrue(new String(store.get(TENANT + "indices/a").orElseThrow().value(), StandardCharsets.UTF_8)
            .contains("\"state\":\"open\""), "a is still open");
    }

    /** One alias document over the limit refuses the whole _aliases request. */
    @Test
    void aliasActionsOverSeveralAliasesAreRefusedWhole() throws Exception {
        send("PUT", "/logs", null);

        JsonNode error = error(send("POST", "/_aliases", "{\"actions\":["
            + "{\"add\":{\"index\":\"logs\",\"alias\":\"a1\"}},"
            + "{\"add\":{\"index\":\"logs\",\"alias\":\"a2\",\"filter\":{\"term\":{\"f\":\""
            + "x".repeat(KeyValueStore.MAX_VALUE_BYTES) + "\"}}}}]}"));
        assertTrue(error.get("reason").asText().startsWith("alias [a2] would be "), error.get("reason").asText());
        assertFalse(stored("aliases/a1"));
    }

    // ------------------------------------------------------------------
    // the backstop
    // ------------------------------------------------------------------

    /** A write no handler checks still answers 400 rather than 500, because the store refused it. */
    @Test
    void aWriteTheStoreRefusesIsAClientError() throws Exception {
        JsonNode error = error(send("PUT", "/_scripts/big",
            "{\"script\":{\"lang\":\"painless\",\"source\":\"" + "x".repeat(KeyValueStore.MAX_VALUE_BYTES) + "\"}}"));
        assertEquals("illegal_argument_exception", error.get("type").asText());
        assertTrue(error.get("reason").asText().contains("over the limit of " + KeyValueStore.MAX_VALUE_BYTES),
            error.get("reason").asText());
        assertFalse(stored("scripts/big"));
    }
}
