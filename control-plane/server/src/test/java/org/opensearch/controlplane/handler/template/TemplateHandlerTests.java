/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.HandlerRegistry;
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
 * The three template kinds, all verified against OpenSearch 3.9.0.
 *
 * <p>Unlike the pipelines, these are genuinely different APIs rather than one API twice, so the tests
 * assert the differences as much as the commonality.
 */
class TemplateHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = Router.fromGeneratedRoutes(ControlPlaneHandlers.build(), TestContexts.factory(store));
    }

    private RestResponse put(String path, String body) {
        return router.dispatch("PUT", path, path, Map.of(), Map.of(), body.getBytes(StandardCharsets.UTF_8));
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------
    // shared: upsert, delete, exists
    // ------------------------------------------------------------------

    @Test
    void allThreeKindsRoundTrip() throws Exception {
        assertEquals(RestStatus.OK, put("/_template/lt", "{\"index_patterns\":[\"a-*\"]}").status());
        assertEquals(RestStatus.OK, put("/_index_template/it", "{\"index_patterns\":[\"b-*\"]}").status());
        assertEquals(RestStatus.OK, put("/_component_template/ct", "{\"template\":{}}").status());

        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/_template/lt").status());
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/_index_template/it").status());
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/_component_template/ct").status());

        for (String path : List.of("/_template/lt", "/_index_template/it", "/_component_template/ct")) {
            assertTrue(body(router.dispatch("DELETE", path)).get("acknowledged").asBoolean(), path);
            assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", path).status(), path);
        }
    }

    /** All three report a missing delete identically, including component templates. */
    @Test
    void deletingAMissingTemplateIsIndexTemplateMissingForEveryKind() throws Exception {
        for (String path : List.of("/_template/nope", "/_index_template/nope", "/_component_template/nope")) {
            RestResponse response = router.dispatch("DELETE", path);
            assertEquals(RestStatus.NOT_FOUND, response.status(), path);
            JsonNode error = body(response).get("error");
            assertEquals("index_template_missing_exception", error.get("type").asText(), path);
            assertEquals("index_template [nope] missing", error.get("reason").asText(), path);
        }
    }

    @Test
    void putIsAnUpsert() throws Exception {
        put("/_index_template/it", "{\"index_patterns\":[\"a-*\"]}");
        assertEquals(RestStatus.OK, put("/_index_template/it", "{\"index_patterns\":[\"b-*\"]}").status());

        JsonNode item = body(router.dispatch("GET", "/_index_template/it")).get("index_templates").get(0);
        assertEquals("b-*", item.get("index_template").get("index_patterns").get(0).asText());
    }

    // ------------------------------------------------------------------
    // the differences
    // ------------------------------------------------------------------

    /** Legacy answers a name-keyed map; the newer kinds answer an array under a wrapper key. */
    @Test
    void getShapeDiffersByKind() throws Exception {
        put("/_template/lt", "{\"index_patterns\":[\"a-*\"]}");
        put("/_index_template/it", "{\"index_patterns\":[\"b-*\"]}");
        put("/_component_template/ct", "{\"template\":{}}");

        JsonNode legacy = body(router.dispatch("GET", "/_template/lt"));
        assertEquals(List.of("lt"), legacy.propertyStream().map(Map.Entry::getKey).toList());
        assertEquals("a-*", legacy.get("lt").get("index_patterns").get(0).asText());

        JsonNode index = body(router.dispatch("GET", "/_index_template/it"));
        assertEquals("it", index.get("index_templates").get(0).get("name").asText());
        assertTrue(index.get("index_templates").get(0).has("index_template"));

        JsonNode component = body(router.dispatch("GET", "/_component_template/ct"));
        assertEquals("ct", component.get("component_templates").get(0).get("name").asText());
        assertTrue(component.get("component_templates").get(0).has("component_template"));
    }

    /** Legacy answers a bare {@code {}} with its 404; the newer kinds answer an error document. */
    @Test
    void getMissingDiffersByKind() throws Exception {
        RestResponse legacy = router.dispatch("GET", "/_template/nope");
        assertEquals(RestStatus.NOT_FOUND, legacy.status());
        assertEquals(0, body(legacy).size(), "bare empty object, as with GET /_ingest/pipeline");

        RestResponse index = router.dispatch("GET", "/_index_template/nope");
        assertEquals(RestStatus.NOT_FOUND, index.status());
        assertEquals("resource_not_found_exception", body(index).get("error").get("type").asText());
        assertEquals("index template matching [nope] not found", body(index).get("error").get("reason").asText());

        RestResponse component = router.dispatch("GET", "/_component_template/nope");
        assertEquals(
            "component template matching [nope] not found",
            body(component).get("error").get("reason").asText()
        );
    }

    /** The required property differs, and so does the error for omitting it. */
    @Test
    void requiredPropertyAndItsErrorDifferByKind() throws Exception {
        RestResponse legacy = put("/_template/lt", "{}");
        assertEquals(RestStatus.BAD_REQUEST, legacy.status());
        JsonNode legacyError = body(legacy).get("error");
        assertEquals("action_request_validation_exception", legacyError.get("type").asText());
        assertEquals("Validation Failed: 1: index patterns are missing;", legacyError.get("reason").asText());

        RestResponse index = put("/_index_template/it", "{\"template\":{}}");
        assertEquals(RestStatus.BAD_REQUEST, index.status());
        assertEquals("illegal_argument_exception", body(index).get("error").get("type").asText());
        assertEquals("Required [index_patterns]", body(index).get("error").get("reason").asText());

        // Component templates require `template`, not `index_patterns`.
        RestResponse component = put("/_component_template/ct", "{\"index_patterns\":[\"a-*\"]}");
        assertEquals(RestStatus.BAD_REQUEST, component.status());
        assertEquals("Required [template]", body(component).get("error").get("reason").asText());
    }

    // ------------------------------------------------------------------
    // name expressions
    // ------------------------------------------------------------------

    @Test
    void getSupportsWildcardsCommaListsAndListingEverything() throws Exception {
        put("/_index_template/a-one", "{\"index_patterns\":[\"x\"]}");
        put("/_index_template/a-two", "{\"index_patterns\":[\"y\"]}");
        put("/_index_template/b-one", "{\"index_patterns\":[\"z\"]}");

        assertEquals(2, body(router.dispatch("GET", "/_index_template/a-*")).get("index_templates").size());
        assertEquals(2, body(router.dispatch("GET", "/_index_template/a-one,b-one")).get("index_templates").size());
        assertEquals(3, body(router.dispatch("GET", "/_index_template")).get("index_templates").size());
    }

    @Test
    void listingEverythingIsNotA404WhenEmpty() throws Exception {
        RestResponse response = router.dispatch("GET", "/_index_template");

        assertEquals(RestStatus.OK, response.status(), "no name given, so nothing to miss");
        assertEquals(0, body(response).get("index_templates").size());
    }

    @Test
    void kindsDoNotShareAKeySpace() {
        put("/_template/same", "{\"index_patterns\":[\"a\"]}");
        put("/_index_template/same", "{\"index_patterns\":[\"a\"]}");
        put("/_component_template/same", "{\"template\":{}}");

        assertEquals(1, store.scanPrefix("tenant/default/templates/legacy/").size());
        assertEquals(1, store.scanPrefix("tenant/default/templates/index/").size());
        assertEquals(1, store.scanPrefix("tenant/default/templates/component/").size());
    }

    // ------------------------------------------------------------------
    // settings normalisation -- a real cluster normalises templates too
    // ------------------------------------------------------------------

    /**
     * Verified against 3.9.0 for all three kinds: whichever shape a template's settings were written
     * in, a GET answers them nested under {@code index} with the values as strings.
     */
    @Test
    void templateSettingsAreNormalisedForEveryKind() throws Exception {
        put("/_index_template/it", "{\"index_patterns\":[\"a-*\"],\"template\":{\"settings\":{\"number_of_shards\":2}}}");
        put("/_component_template/ct", "{\"template\":{\"settings\":{\"index.number_of_replicas\":7}}}");
        put("/_template/lt", "{\"index_patterns\":[\"b-*\"],\"settings\":{\"index\":{\"number_of_replicas\":6}}}");

        JsonNode index = body(router.dispatch("GET", "/_index_template/it"))
            .get("index_templates").get(0).get("index_template").get("template").get("settings");
        assertEquals("2", index.get("index").get("number_of_shards").textValue());

        JsonNode component = body(router.dispatch("GET", "/_component_template/ct"))
            .get("component_templates").get(0).get("component_template").get("template").get("settings");
        assertEquals("7", component.get("index").get("number_of_replicas").textValue());

        JsonNode legacy = body(router.dispatch("GET", "/_template/lt")).get("lt").get("settings");
        assertEquals("6", legacy.get("index").get("number_of_replicas").textValue());
    }

    /**
     * Verified: {@code flat_settings=true} is honoured by legacy {@code GET /_template} and ignored by
     * {@code GET /_index_template}. The parameter is declared on the one and not the other in the spec,
     * and a live cluster follows suit.
     */
    @Test
    void flatSettingsIsHonouredByLegacyAndIgnoredByTheNewerKinds() throws Exception {
        put("/_template/lt", "{\"index_patterns\":[\"b-*\"],\"settings\":{\"number_of_replicas\":6}}");
        put("/_index_template/it", "{\"index_patterns\":[\"a-*\"],\"template\":{\"settings\":{\"number_of_shards\":2}}}");

        JsonNode legacy = get("/_template/lt").get("lt").get("settings");
        assertEquals("6", legacy.get("index.number_of_replicas").textValue(), "flattened on request");
        assertFalse(legacy.has("index"), "and not also nested");

        JsonNode index = get("/_index_template/it")
            .get("index_templates").get(0).get("index_template").get("template").get("settings");
        assertEquals("2", index.get("index").get("number_of_shards").textValue(), "parameter ignored");
    }

    /** A GET with {@code flat_settings=true}. */
    private JsonNode get(String path) throws Exception {
        return body(router.dispatch("GET", path, path, Map.of("flat_settings", List.of("true")), Map.of(), new byte[0]));
    }

    // ------------------------------------------------------------------
    // composition -- precedence established by probing _simulate_index
    // ------------------------------------------------------------------

    private void component(String name, String template) {
        put("/_component_template/" + name, "{\"template\":" + template + "}");
    }

    private JsonNode simulateIndex(String indexName) throws Exception {
        RestResponse response = router.dispatch("POST", "/_index_template/_simulate_index/" + indexName);
        assertEquals(RestStatus.OK, response.status());
        return body(response).get("template");
    }

    /**
     * The composed settings, nested under {@code index} and stringified as a real cluster renders them.
     * Verified: {@code _simulate_index} answers {@code {"settings":{"index":{"number_of_shards":"2"}}}}
     * whichever of the accepted shapes the template was written in.
     */
    private JsonNode composedSettings(JsonNode composed) {
        return composed.get("settings").get("index");
    }

    /** Verified: later entries in composed_of win. Reversing the list reverses the outcome. */
    @Test
    void componentTemplatesMergeInOrderWithLaterWinning() throws Exception {
        component("c1", "{\"settings\":{\"number_of_replicas\":9}}");
        component("c2", "{\"settings\":{\"number_of_replicas\":5}}");

        put("/_index_template/t", "{\"index_patterns\":[\"x-*\"],\"composed_of\":[\"c1\",\"c2\"]}");
        assertEquals("5", composedSettings(simulateIndex("x-1")).get("number_of_replicas").textValue());

        put("/_index_template/t", "{\"index_patterns\":[\"x-*\"],\"composed_of\":[\"c2\",\"c1\"]}");
        assertEquals("9", composedSettings(simulateIndex("x-1")).get("number_of_replicas").textValue());
    }

    /**
     * Overriding is key by key on the canonical flat form, so it lands across differing shapes.
     *
     * <p>The regression this pins: merging the nested documents structurally left
     * {@code {"index":{"number_of_replicas":9},"number_of_replicas":5}} — two keys for one setting,
     * where a real cluster reports one.
     */
    @Test
    void anOverrideLandsEvenWhenTheTwoSidesSpellTheSettingDifferently() throws Exception {
        component("c1", "{\"settings\":{\"index\":{\"number_of_replicas\":9}}}");
        put("/_index_template/t", "{\"index_patterns\":[\"x-*\"],\"composed_of\":[\"c1\"],"
            + "\"template\":{\"settings\":{\"number_of_replicas\":5}}}");

        JsonNode settings = simulateIndex("x-1").get("settings");
        assertEquals("5", settings.get("index").get("number_of_replicas").textValue());
        assertEquals(1, settings.size(), "one setting, not one per spelling");
        assertEquals(1, settings.get("index").size());
    }

    /** Verified: the template's own block beats every component. */
    @Test
    void theTemplatesOwnBlockBeatsItsComponents() throws Exception {
        component("c1", "{\"settings\":{\"number_of_replicas\":9}}");
        component("c2", "{\"settings\":{\"number_of_replicas\":5}}");
        put("/_index_template/t", "{\"index_patterns\":[\"x-*\"],\"composed_of\":[\"c1\",\"c2\"],"
            + "\"template\":{\"settings\":{\"number_of_replicas\":1}}}");

        assertEquals("1", composedSettings(simulateIndex("x-1")).get("number_of_replicas").textValue());
    }

    @Test
    void mappingsAndAliasesMergeStructurallyAcrossComponents() throws Exception {
        component("c1", "{\"mappings\":{\"properties\":{\"a\":{\"type\":\"keyword\"}}},\"aliases\":{\"from-c1\":{}}}");
        component("c2", "{\"mappings\":{\"properties\":{\"b\":{\"type\":\"long\"}}},\"aliases\":{\"from-c2\":{}}}");
        put("/_index_template/t", "{\"index_patterns\":[\"x-*\"],\"composed_of\":[\"c1\",\"c2\"],"
            + "\"template\":{\"aliases\":{\"from-t\":{}}}}");

        JsonNode composed = simulateIndex("x-1");
        JsonNode properties = composed.get("mappings").get("properties");
        assertEquals("keyword", properties.get("a").get("type").asText());
        assertEquals("long", properties.get("b").get("type").asText(), "both survive rather than replacing");
        for (String alias : List.of("from-c1", "from-c2", "from-t")) {
            assertTrue(composed.get("aliases").has(alias), alias);
        }
    }

    /** Templates are selected, not merged with each other: the highest priority wins outright. */
    @Test
    void theHighestPriorityTemplateWinsOutright() throws Exception {
        put("/_index_template/low", "{\"index_patterns\":[\"x-*\"],\"priority\":1,"
            + "\"template\":{\"settings\":{\"number_of_shards\":1}}}");
        put("/_index_template/high", "{\"index_patterns\":[\"x-1*\"],\"priority\":50,"
            + "\"template\":{\"settings\":{\"number_of_replicas\":7}}}");

        JsonNode composed = simulateIndex("x-1");
        assertEquals("7", composedSettings(composed).get("number_of_replicas").textValue());
        assertFalse(composedSettings(composed).has("number_of_shards"), "the loser contributes nothing");
    }

    @Test
    void simulateIndexReportsNoMatchAsNotFound() {
        assertEquals(RestStatus.NOT_FOUND,
            router.dispatch("POST", "/_index_template/_simulate_index/unmatched").status());
    }

    @Test
    void simulateTemplateAcceptsABodyToTryBeforeSaving() throws Exception {
        component("c1", "{\"settings\":{\"number_of_replicas\":4}}");

        RestResponse response = put("/_index_template/_simulate",
            "{\"index_patterns\":[\"y-*\"],\"composed_of\":[\"c1\"]}");
        // The route is POST; a PUT of the same path is not registered, so drive it directly.
        RestResponse simulated = router.dispatch("POST", "/_index_template/_simulate",
            "/_index_template/_simulate", Map.of(), Map.of(),
            "{\"index_patterns\":[\"y-*\"],\"composed_of\":[\"c1\"]}".getBytes(StandardCharsets.UTF_8));

        assertEquals(RestStatus.OK, simulated.status());
        assertEquals("4", composedSettings(body(simulated).get("template")).get("number_of_replicas").textValue());
    }

    // ------------------------------------------------------------------
    // composition applies to index creation
    // ------------------------------------------------------------------

    @Test
    void creatingAnIndexAppliesTheMatchingTemplate() throws Exception {
        component("c1", "{\"settings\":{\"number_of_shards\":3},\"aliases\":{\"via-template\":{}}}");
        put("/_index_template/t", "{\"index_patterns\":[\"x-*\"],\"composed_of\":[\"c1\"]}");

        assertEquals(RestStatus.OK, router.dispatch("PUT", "/x-1").status());
        // The alias the template supplied became a real alias resource.
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/_alias/via-template").status());
    }

    @Test
    void theRequestBodyOverridesTheTemplate() throws Exception {
        component("c1", "{\"settings\":{\"number_of_shards\":3}}");
        put("/_index_template/t", "{\"index_patterns\":[\"x-*\"],\"composed_of\":[\"c1\"]}");

        put("/x-1", "{\"settings\":{\"number_of_shards\":7}}");
        JsonNode settings = MAPPER.readTree(
            store.getAsString("tenant/default/indices/x-1").orElseThrow()).get("settings");
        assertEquals("7", settings.get("index.number_of_shards").textValue(), "the request wins");
    }

    @Test
    void anIndexMatchingNoTemplateIsUnaffected() throws Exception {
        put("/_index_template/t", "{\"index_patterns\":[\"x-*\"],"
            + "\"template\":{\"settings\":{\"number_of_shards\":9}}}");

        router.dispatch("PUT", "/unmatched");
        JsonNode settings = MAPPER.readTree(
            store.getAsString("tenant/default/indices/unmatched").orElseThrow()).get("settings");
        assertEquals("1", settings.get("index.number_of_shards").textValue(), "the default, not the template");
    }

    // ------------------------------------------------------------------
    // put-time conflict validation
    // ------------------------------------------------------------------

    @Test
    void overlappingPatternsAtTheSamePriorityAreRejected() throws Exception {
        put("/_index_template/first", "{\"index_patterns\":[\"x-*\"],\"priority\":10}");

        RestResponse response = put("/_index_template/second", "{\"index_patterns\":[\"x-*\"],\"priority\":10}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("illegal_argument_exception", error.get("type").asText());
        assertTrue(error.get("reason").asText().contains("same priority [10]"));
    }

    @Test
    void overlappingPatternsAtDifferentPrioritiesAreAllowed() {
        put("/_index_template/first", "{\"index_patterns\":[\"x-*\"],\"priority\":10}");

        assertEquals(RestStatus.OK,
            put("/_index_template/second", "{\"index_patterns\":[\"x-*\"],\"priority\":20}").status(),
            "the higher priority simply wins, so there is no ambiguity");
    }

    @Test
    void replacingATemplateDoesNotConflictWithItself() {
        put("/_index_template/t", "{\"index_patterns\":[\"x-*\"],\"priority\":10}");

        assertEquals(RestStatus.OK, put("/_index_template/t", "{\"index_patterns\":[\"x-*\"],\"priority\":10}").status());
    }

    /** Legacy templates are stored but never applied. A real cluster would fall back to them. */
    @Test
    void legacyTemplatesAreNotAppliedToCreatedIndices() throws Exception {
        put("/_template/lt", "{\"index_patterns\":[\"x-*\"],\"settings\":{\"number_of_shards\":9}}");

        router.dispatch("PUT", "/x-1");
        JsonNode settings = MAPPER.readTree(
            store.getAsString("tenant/default/indices/x-1").orElseThrow()).get("settings");
        assertEquals("1", settings.get("index.number_of_shards").textValue(), "known gap, documented");
    }

    @Test
    void allTwelveGroupsAreRegistered() {
        HandlerRegistry registry = ControlPlaneHandlers.build();

        for (String group : List.of(
            "indices.put_template", "indices.get_template", "indices.delete_template", "indices.exists_template",
            "indices.put_index_template", "indices.get_index_template",
            "indices.delete_index_template", "indices.exists_index_template",
            "cluster.put_component_template", "cluster.get_component_template",
            "cluster.delete_component_template", "cluster.exists_component_template")) {
            assertTrue(registry.implemented().contains(group), group);
        }
    }
}
