/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.capability.DataPlaneCapabilities;
import org.opensearch.controlplane.capability.DeclaredCapabilities;
import org.opensearch.controlplane.capability.PermissiveCapabilities;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code GET /_script_language}, and the stored-script {@code lang} validation it unlocks.
 *
 * <p>The response shape is taken from a live 3.9.0 cluster; the fixture below is a trimmed version of
 * what it reported.
 */
class GetScriptLanguagesHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Shaped exactly as {@code tools/dump-capabilities.sh} emits it. */
    private static final String CAPABILITIES = """
        processors:
          ingest: [append, set]
        script_languages:
          types_allowed: [inline, stored]
          language_contexts:
            expression:
              - aggs
              - score
            mustache:
              - template
            painless:
              - field
              - filter
              - score
        """;

    private Router routerWith(DataPlaneCapabilities capabilities) {
        return Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(
                org.opensearch.controlplane.handler.core.ClusterIdentity.defaults(), capabilities),
            TestContexts.factory(new InMemoryKeyValueStore())
        );
    }

    private Router declaring(Path dir, String yaml) throws IOException {
        Path file = dir.resolve("capabilities.yaml");
        Files.writeString(file, yaml);
        return routerWith(DeclaredCapabilities.load(file));
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private RestResponse put(Router router, String id, String body) {
        String path = "/_scripts/" + id;
        return router.dispatch("PUT", path, path, Map.of(), Map.of(), body.getBytes(StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------
    // the read
    // ------------------------------------------------------------------

    @Test
    void reportsTheDeclaredLanguagesAndContexts(@TempDir Path dir) throws Exception {
        Router router = declaring(dir, CAPABILITIES);

        RestResponse response = router.dispatch("GET", "/_script_language");
        assertEquals(RestStatus.OK, response.status());
        assertEquals(List.of("types_allowed", "language_contexts"),
            body(response).propertyStream().map(Map.Entry::getKey).toList());

        JsonNode types = body(response).get("types_allowed");
        assertEquals(List.of("inline", "stored"),
            List.of(types.get(0).asText(), types.get(1).asText()));

        // An array of {language, contexts} objects, sorted by language -- the shape a cluster uses.
        JsonNode contexts = body(response).get("language_contexts");
        assertEquals(3, contexts.size());
        assertEquals("expression", contexts.get(0).get("language").asText());
        assertEquals("mustache", contexts.get(1).get("language").asText());
        assertEquals("painless", contexts.get(2).get("language").asText());
        assertEquals(List.of("language", "contexts"),
            contexts.get(0).propertyStream().map(Map.Entry::getKey).toList());
        assertEquals("template", contexts.get(1).get("contexts").get(0).asText());
    }

    /**
     * With nothing declared the operation keeps its 501 stub rather than answering empty lists, which
     * would tell a client it may use no language at all. See {@link GetScriptLanguagesHandler}.
     */
    @Test
    void withoutDeclaredLanguagesTheOperationStaysStubbed() {
        Router router = routerWith(new PermissiveCapabilities());

        assertEquals(RestStatus.NOT_IMPLEMENTED, router.dispatch("GET", "/_script_language").status());
        assertFalse(ControlPlaneHandlers.build().implemented().contains("get_script_languages"),
            "not implemented by default, because the answer is not knowable by default");
    }

    /** A capabilities file predating this feature still loads; the section is optional. */
    @Test
    void aCapabilitiesFileWithNoLanguagesSectionStillLoads(@TempDir Path dir) throws Exception {
        Router router = declaring(dir, "processors:\n  ingest: [set]\n");

        assertEquals(RestStatus.NOT_IMPLEMENTED, router.dispatch("GET", "/_script_language").status());
        // Processors are still enforced, so the rest of the file did its job.
        assertEquals(RestStatus.BAD_REQUEST, router.dispatch("PUT", "/_ingest/pipeline/p", "/_ingest/pipeline/p",
            Map.of(), Map.of(), "{\"processors\":[{\"nonesuch\":{}}]}".getBytes(StandardCharsets.UTF_8)).status());
    }

    // ------------------------------------------------------------------
    // the validation it unlocks
    // ------------------------------------------------------------------

    /** Byte-exact against 3.9.0. */
    @Test
    void anUnsupportedLangIsRejectedOnceLanguagesAreDeclared(@TempDir Path dir) throws Exception {
        Router router = declaring(dir, CAPABILITIES);

        RestResponse response = put(router, "x", "{\"script\":{\"lang\":\"nonesuch\",\"source\":\"1\"}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("illegal_argument_exception", body(response).get("error").get("type").asText());
        assertEquals("unable to put stored script with unsupported lang [nonesuch]",
            body(response).get("error").get("reason").asText());
    }

    @Test
    void aDeclaredLangIsAccepted(@TempDir Path dir) throws Exception {
        Router router = declaring(dir, CAPABILITIES);

        for (String lang : List.of("painless", "mustache", "expression")) {
            assertEquals(RestStatus.OK,
                put(router, lang, "{\"script\":{\"lang\":\"" + lang + "\",\"source\":\"1\"}}").status(), lang);
        }
    }

    /** Without a declaration anything goes, as with processor types before --capabilities existed. */
    @Test
    void withoutDeclaredLanguagesAnyLangIsAccepted() {
        Router router = routerWith(new PermissiveCapabilities());

        assertEquals(RestStatus.OK, put(router, "x", "{\"script\":{\"lang\":\"nonesuch\",\"source\":\"1\"}}").status());
    }

    /**
     * The deprecated empty-body path is not language-validated: it supplies {@code mustache} itself
     * rather than taking it from the client, so there is nothing to check.
     */
    @Test
    void theEmptyBodyPathIsNotLanguageValidated(@TempDir Path dir) throws Exception {
        Router router = declaring(dir, "processors:\n  ingest: []\nscript_languages:\n"
            + "  types_allowed: [inline]\n  language_contexts:\n    painless:\n      - score\n");

        // mustache is NOT declared here, and the empty-body path still succeeds.
        assertEquals(RestStatus.OK, put(router, "empty", "{}").status());
        assertTrue(ControlPlaneHandlers.build().notImplemented().isEmpty() == false);
    }
}
