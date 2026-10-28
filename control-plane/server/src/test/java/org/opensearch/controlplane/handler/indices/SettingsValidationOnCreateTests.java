/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.capability.DeclaredSettings;
import org.opensearch.controlplane.capability.PermissiveCapabilities;
import org.opensearch.controlplane.capability.PermissiveSettings;
import org.opensearch.controlplane.capability.SettingsRegistry;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.core.ClusterIdentity;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Settings validation on the two paths that <em>create</em> configuration rather than update it:
 * {@code indices.create} and the three template puts.
 *
 * <p>Both permit static settings, because they are where static settings are set — and both reject a
 * private one, in two different shapes. All wording verified against OpenSearch 3.9.0.
 */
class SettingsValidationOnCreateTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String REGISTRY = """
        version: "3.9.0"
        index:
          - {key: "index.number_of_shards",   kind: exact,  properties: [Final, IndexScope]}
          - {key: "index.number_of_replicas", kind: exact,  properties: [Dynamic, IndexScope]}
          - {key: "index.replication.type",   kind: exact,  properties: [IndexScope]}
          - {key: "index.analysis.",          kind: prefix, properties: [IndexScope]}
          - {key: "index.uuid",               kind: exact,  properties: [IndexScope, PrivateIndex]}
          - {key: "index.history.uuid",       kind: exact,  properties: [IndexScope, PrivateIndex]}
        """;

    private final InMemoryKeyValueStore store = new InMemoryKeyValueStore();

    private Router routerWith(SettingsRegistry settings) {
        return Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(ClusterIdentity.defaults(), new PermissiveCapabilities(), settings),
            TestContexts.factory(store)
        );
    }

    private Router enforcing(Path dir) throws IOException {
        Path file = dir.resolve("settings_registry.yaml");
        Files.writeString(file, REGISTRY);
        return routerWith(DeclaredSettings.load(file).orElseThrow());
    }

    private RestResponse put(Router router, String path, String body) {
        return router.dispatch("PUT", path, path, Map.of(), Map.of(), body.getBytes(StandardCharsets.UTF_8));
    }

    private JsonNode error(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8)).get("error");
    }

    // ------------------------------------------------------------------
    // indices.create
    // ------------------------------------------------------------------

    /** The whole point of separating create from put_settings: static settings are legitimate here. */
    @Test
    void createAcceptsStaticSettings(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        assertEquals(RestStatus.OK,
            put(router, "/my-index", "{\"settings\":{\"number_of_shards\":3}}").status());
    }

    @Test
    void createRejectsAnUnknownSetting(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        RestResponse response = put(router, "/my-index", "{\"settings\":{\"totally_bogus\":1}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("settings_exception", error(response).get("type").asText());
        assertEquals(
            "unknown setting [index.totally_bogus] please check that any required plugins are installed, "
                + "or check the breaking changes documentation for removed settings",
            error(response).get("reason").asText()
        );
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/my-index").status(),
            "rejected before anything was written");
    }

    /** Create's own wording, from MetadataCreateIndexService rather than AbstractScopedSettings. */
    @Test
    void createRejectsAPrivateSetting(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        RestResponse response = put(router, "/my-index", "{\"settings\":{\"index.uuid\":\"abc\"}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("validation_exception", error(response).get("type").asText());
        assertEquals("Validation Failed: 1: private index setting [index.uuid] can not be set explicitly;",
            error(response).get("reason").asText());
    }

    /**
     * Several private settings are numbered from one, each terminated by a semicolon, and with
     * <b>no space between entries</b> — {@code ValidationException.toString} appends
     * {@code "N: <error>;"} with no separator, so the second follows the first's semicolon directly.
     * Verified byte for byte; the space is easy to add by accident and a first cut of this did.
     */
    @Test
    void createNumbersMultiplePrivateSettingsWithNoSpaceBetween(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        RestResponse response = put(router, "/my-index",
            "{\"settings\":{\"index.uuid\":\"abc\",\"index.history.uuid\":\"def\"}}");
        assertEquals(
            "Validation Failed: 1: private index setting [index.history.uuid] can not be set explicitly;"
                + "2: private index setting [index.uuid] can not be set explicitly;",
            error(response).get("reason").asText()
        );
    }

    /** Validation runs after composition, so a template's contribution is checked too. */
    @Test
    void createValidatesWhatATemplateContributed(@TempDir Path dir) throws Exception {
        Router permissive = routerWith(new PermissiveSettings());
        // Stored while nothing was enforcing, so it carries a setting the registry does not know.
        put(permissive, "/_index_template/t",
            "{\"index_patterns\":[\"x-*\"],\"template\":{\"settings\":{\"totally_bogus\":1}}}");

        Router router = enforcing(dir);
        RestResponse response = put(router, "/x-1", "{}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertTrue(error(response).get("reason").asText().startsWith("unknown setting [index.totally_bogus]"));
    }

    @Test
    void createWithoutARegistryValidatesNothing() {
        Router router = routerWith(new PermissiveSettings());

        assertEquals(RestStatus.OK, put(router, "/a", "{\"settings\":{\"totally_bogus\":1}}").status());
        assertEquals(RestStatus.OK, put(router, "/b", "{\"settings\":{\"index.uuid\":\"abc\"}}").status());
    }

    // ------------------------------------------------------------------
    // template puts
    // ------------------------------------------------------------------

    private static final Map<String, String> TEMPLATES = Map.of(
        "/_index_template/t", "{\"index_patterns\":[\"a-*\"],\"template\":{\"settings\":{%s}}}",
        "/_component_template/c", "{\"template\":{\"settings\":{%s}}}",
        "/_template/l", "{\"index_patterns\":[\"b-*\"],\"settings\":{%s}}"
    );

    @Test
    void everyTemplateKindAcceptsStaticSettings(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        for (var entry : TEMPLATES.entrySet()) {
            String body = entry.getValue().formatted("\"number_of_shards\":3");
            assertEquals(RestStatus.OK, put(router, entry.getKey(), body).status(), entry.getKey());
        }
    }

    /** Bare settings_exception, not wrapped — verified for all three kinds. */
    @Test
    void everyTemplateKindRejectsAnUnknownSettingUnwrapped(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        for (var entry : TEMPLATES.entrySet()) {
            String body = entry.getValue().formatted("\"totally_bogus\":1");
            RestResponse response = put(router, entry.getKey(), body);
            assertEquals(RestStatus.BAD_REQUEST, response.status(), entry.getKey());
            assertEquals("settings_exception", error(response).get("type").asText(), entry.getKey());
            assertTrue(error(response).get("reason").asText().startsWith("unknown setting [index.totally_bogus]"),
                entry.getKey());
        }
    }

    /**
     * A private setting <em>is</em> wrapped, and every kind says {@code index_template} — including
     * component and legacy, the same quirk as the DELETE-missing message.
     */
    @Test
    void everyTemplateKindWrapsAPrivateSetting(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        for (var entry : Map.of("/_index_template/t", "t", "/_component_template/c", "c", "/_template/l", "l")
            .entrySet()) {
            String body = TEMPLATES.get(entry.getKey()).formatted("\"index.uuid\":\"abc\"");
            RestResponse response = put(router, entry.getKey(), body);

            assertEquals(RestStatus.BAD_REQUEST, response.status(), entry.getKey());
            assertEquals("invalid_index_template_exception", error(response).get("type").asText(), entry.getKey());
            assertEquals(
                "index_template [" + entry.getValue() + "] invalid, cause [Validation Failed: 1: "
                    + "private index setting [index.uuid] can not be set explicitly;]",
                error(response).get("reason").asText(),
                entry.getKey()
            );
        }
    }

    @Test
    void aTemplateSettingCoveredByAPrefixIsAccepted(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        assertEquals(RestStatus.OK, put(router, "/_index_template/t",
            "{\"index_patterns\":[\"a-*\"],\"template\":{\"settings\":"
                + "{\"index.analysis.analyzer.my.type\":\"custom\"}}}").status());
    }

    /** A template with no settings block at all must not trip the check. */
    @Test
    void aTemplateWithNoSettingsIsFine(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        assertEquals(RestStatus.OK,
            put(router, "/_index_template/t", "{\"index_patterns\":[\"a-*\"]}").status());
        assertEquals(RestStatus.OK,
            put(router, "/_component_template/c", "{\"template\":{}}").status());
    }

    @Test
    void templatesWithoutARegistryValidateNothing() {
        Router router = routerWith(new PermissiveSettings());

        for (var entry : TEMPLATES.entrySet()) {
            String body = entry.getValue().formatted("\"totally_bogus\":1");
            assertEquals(RestStatus.OK, put(router, entry.getKey(), body).status(), entry.getKey());
        }
    }

    @Test
    void theSuggestionBranchWorksOnThesePathsToo(@TempDir Path dir) throws Exception {
        Router router = enforcing(dir);

        RestResponse response = put(router, "/my-index", "{\"settings\":{\"number_of_replica\":2}}");
        assertEquals("unknown setting [index.number_of_replica] did you mean [index.number_of_replicas]?",
            error(response).get("reason").asText());
    }
}
