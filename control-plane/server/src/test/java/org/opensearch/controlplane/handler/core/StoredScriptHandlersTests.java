/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.core;

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

/** Stored scripts, all verified against OpenSearch 3.9.0. */
class StoredScriptHandlersTests {

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

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private String reasonOf(RestResponse response) throws Exception {
        return body(response).get("error").get("reason").asText();
    }

    private String typeOf(RestResponse response) throws Exception {
        return body(response).get("error").get("type").asText();
    }

    // ------------------------------------------------------------------
    // round trip
    // ------------------------------------------------------------------

    @Test
    void storesAndReturnsAScript() throws Exception {
        assertEquals(RestStatus.OK, send("PUT", "/_scripts/calc",
            "{\"script\":{\"lang\":\"painless\",\"source\":\"doc['f'].value * 2\"}}").status());

        RestResponse response = send("GET", "/_scripts/calc", null);
        assertEquals(RestStatus.OK, response.status());
        assertEquals(List.of("_id", "found", "script"),
            body(response).propertyStream().map(Map.Entry::getKey).toList());
        assertEquals("calc", body(response).get("_id").asText());
        assertTrue(body(response).get("found").asBoolean());
        assertEquals("painless", body(response).get("script").get("lang").asText());
        assertEquals("doc['f'].value * 2", body(response).get("script").get("source").asText());
    }

    @Test
    void optionsRoundTripWhenPresent() throws Exception {
        send("PUT", "/_scripts/tmpl", "{\"script\":{\"lang\":\"mustache\",\"source\":\"{{x}}\","
            + "\"options\":{\"content_type\":\"application/json\"}}}");

        JsonNode script = body(send("GET", "/_scripts/tmpl", null)).get("script");
        assertEquals("application/json", script.get("options").get("content_type").asText());
    }

    @Test
    void optionsAreAbsentWhenNotSupplied() throws Exception {
        send("PUT", "/_scripts/calc", "{\"script\":{\"lang\":\"painless\",\"source\":\"1\"}}");

        assertFalse(body(send("GET", "/_scripts/calc", null)).get("script").has("options"));
    }

    @Test
    void putIsAnUpsert() throws Exception {
        send("PUT", "/_scripts/calc", "{\"script\":{\"lang\":\"painless\",\"source\":\"1\"}}");
        send("PUT", "/_scripts/calc", "{\"script\":{\"lang\":\"painless\",\"source\":\"2\"}}");

        assertEquals("2", body(send("GET", "/_scripts/calc", null)).get("script").get("source").asText());
    }

    @Test
    void postIsTheSameOperation() throws Exception {
        assertEquals(RestStatus.OK, send("POST", "/_scripts/calc",
            "{\"script\":{\"lang\":\"painless\",\"source\":\"1\"}}").status());
        assertTrue(body(send("GET", "/_scripts/calc", null)).get("found").asBoolean());
    }

    /** A context in the path or the query string is accepted and ignored, and never echoed back. */
    @Test
    void aContextIsAcceptedAndIgnored() throws Exception {
        assertEquals(RestStatus.OK, send("PUT", "/_scripts/calc/score",
            "{\"script\":{\"lang\":\"painless\",\"source\":\"1\"}}").status());

        JsonNode fetched = body(send("GET", "/_scripts/calc", null));
        assertTrue(fetched.get("found").asBoolean());
        assertFalse(fetched.get("script").has("context"));
    }

    @Test
    void deleteRemovesItAndAcknowledges() throws Exception {
        send("PUT", "/_scripts/calc", "{\"script\":{\"lang\":\"painless\",\"source\":\"1\"}}");

        assertTrue(body(send("DELETE", "/_scripts/calc", null)).get("acknowledged").asBoolean());
        assertFalse(body(send("GET", "/_scripts/calc", null)).get("found").asBoolean());
    }

    // ------------------------------------------------------------------
    // missing, two ways
    // ------------------------------------------------------------------

    /** GET answers an ordinary body with {@code found: false} — no error document at all. */
    @Test
    void gettingAMissingScriptIsFoundFalseNotAnError() throws Exception {
        RestResponse response = send("GET", "/_scripts/nope", null);

        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals(List.of("_id", "found"), body(response).propertyStream().map(Map.Entry::getKey).toList());
        assertEquals("nope", body(response).get("_id").asText());
        assertFalse(body(response).get("found").asBoolean());
        assertFalse(body(response).has("error"), "the read path has no error document");
    }

    /** DELETE answers a structured error for the same condition. */
    @Test
    void deletingAMissingScriptIsAStructuredError() throws Exception {
        RestResponse response = send("DELETE", "/_scripts/nope", null);

        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals("resource_not_found_exception", typeOf(response));
        assertEquals("stored script [nope] does not exist and cannot be deleted", reasonOf(response));
    }

    // ------------------------------------------------------------------
    // validation
    // ------------------------------------------------------------------

    /**
     * Lang is checked before source, which is not the obvious order: {@code {"script":{}}} answers
     * {@code must specify lang}. Verified, and the reverse would misreport the commonest bad body.
     */
    @Test
    void langIsCheckedBeforeSource() throws Exception {
        assertEquals("must specify lang for stored script",
            reasonOf(send("PUT", "/_scripts/x", "{\"script\":{}}")));
        assertEquals("must specify lang for stored script",
            reasonOf(send("PUT", "/_scripts/x", "{\"script\":{\"source\":\"1\"}}")));
        assertEquals("must specify source for stored script",
            reasonOf(send("PUT", "/_scripts/x", "{\"script\":{\"lang\":\"painless\"}}")));
    }

    @Test
    void bothValidationFailuresAreIllegalArgument() throws Exception {
        assertEquals("illegal_argument_exception", typeOf(send("PUT", "/_scripts/x", "{\"script\":{}}")));
        assertEquals(RestStatus.BAD_REQUEST, send("PUT", "/_scripts/x", "{\"script\":{}}").status());
    }

    /** A body not wrapped in {@code script} names the offending field. */
    @Test
    void anUnwrappedBodyIsAParsingException() throws Exception {
        RestResponse response = send("PUT", "/_scripts/x", "{\"lang\":\"painless\",\"source\":\"1\"}");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("parsing_exception", typeOf(response));
        assertEquals("unexpected field [lang], expected [script]", reasonOf(response));
        // line and col are deliberately absent; see ErrorResponses.scriptParsingFailure.
        assertFalse(body(response).get("error").has("line"));
    }

    @Test
    void rejectsAMissingOrMalformedBody() throws Exception {
        RestResponse noBody = send("PUT", "/_scripts/x", null);
        assertEquals("parse_exception", typeOf(noBody));
        assertEquals("request body is required", reasonOf(noBody));

        assertEquals(RestStatus.BAD_REQUEST, send("PUT", "/_scripts/x", "{not json").status());
    }

    /**
     * The deprecated empty-template path, carried forward on purpose: an empty body object stores
     * {@code mustache} with an empty source and answers 200. From {@code StoredScriptSource.parse},
     * which does this alongside a deprecation warning.
     */
    @Test
    void anEmptyBodyStoresTheDeprecatedEmptyTemplate() throws Exception {
        assertEquals(RestStatus.OK, send("PUT", "/_scripts/empty", "{}").status());

        JsonNode script = body(send("GET", "/_scripts/empty", null)).get("script");
        assertEquals("mustache", script.get("lang").asText());
        assertEquals("", script.get("source").asText());
    }

    /** No lang validation: which languages exist is a data plane fact we have no source for yet. */
    @Test
    void anUnknownLangIsAccepted() throws Exception {
        assertEquals(RestStatus.OK, send("PUT", "/_scripts/x",
            "{\"script\":{\"lang\":\"nonesuch\",\"source\":\"1\"}}").status());
    }

    @Test
    void allThreeGroupsAreRegistered() {
        var registry = ControlPlaneHandlers.build();

        for (String group : List.of("put_script", "get_script", "delete_script")) {
            assertTrue(registry.implemented().contains(group), group);
        }
        assertTrue(registry.notImplemented().contains("get_script_languages"),
            "the capability read that would let us validate lang is still stubbed");
    }
}
