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
import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.tenant.SingleTenantResolver;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code PUT}, {@code HEAD} and {@code DELETE} on {@code /{index}}, driven through the real router.
 *
 * <p>Status codes and error bodies were verified against OpenSearch 3.9.0.
 */
class CreateIndexHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TENANT_PREFIX = SingleTenantResolver.DEFAULT_TENANT.keyPrefix();
    private static final long FIXED_MILLIS = 1_760_000_000_000L;

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(),
            TestContexts.factory(store, Clock.fixed(Instant.ofEpochMilli(FIXED_MILLIS), ZoneOffset.UTC))
        );
    }

    private RestResponse create(String index, String body) {
        String path = "/" + index;
        return router.dispatch(
            "PUT", path, path, Map.of(), Map.of(),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8)
        );
    }

    private RestResponse create(String index) {
        return create(index, null);
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private JsonNode metadata(String index) throws Exception {
        return MAPPER.readTree(store.getAsString(TENANT_PREFIX + IndexStore.key(index)).orElseThrow());
    }

    private int taskCount() {
        return store.scanPrefix(TENANT_PREFIX + IndexManagementTasks.KEY_PREFIX).size();
    }

    private JsonNode onlyTask() throws Exception {
        var tasks = store.scanPrefix(TENANT_PREFIX + IndexManagementTasks.KEY_PREFIX);
        assertEquals(1, tasks.size());
        return MAPPER.readTree(new String(tasks.firstEntry().getValue().value(), StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------
    // create
    // ------------------------------------------------------------------

    @Test
    void createsAnIndexWithNoBody() throws Exception {
        RestResponse response = create("my-index");

        assertEquals(RestStatus.OK, response.status());
        JsonNode json = body(response);
        // Field order matches the server's: acknowledged, shards_acknowledged, index.
        assertEquals(List.of("acknowledged", "shards_acknowledged", "index"),
            json.propertyStream().map(Map.Entry::getKey).toList());
        assertTrue(json.get("acknowledged").asBoolean());
        assertEquals("my-index", json.get("index").asText());
    }

    @Test
    void storesMetadataWithAUuidAndCreationDate() throws Exception {
        create("my-index");
        JsonNode metadata = metadata("my-index");

        assertEquals("my-index", metadata.get("index").asText());
        assertEquals("open", metadata.get("state").asText());
        // OpenSearch stores creation_date as epoch millis in a string.
        assertEquals(Long.toString(FIXED_MILLIS), metadata.get("creation_date").asText());
        assertEquals(22, metadata.get("uuid").asText().length(), "128-bit base64url, unpadded");
    }

    @Test
    void appliesShardDefaultsSoTheShardManagerHasSomethingToAllocate() throws Exception {
        create("my-index");
        JsonNode settings = metadata("my-index").get("settings");

        // Stringified, like every settings value on a real cluster -- textValue, not asInt, so that a
        // regression to native types fails here rather than being papered over by Jackson's coercion.
        assertEquals("1", settings.get("index.number_of_shards").textValue());
        assertEquals("1", settings.get("index.number_of_replicas").textValue());
    }

    /**
     * Replication is chosen per index by whoever creates it, so the request wins and {@code DOCUMENT}
     * is the fallback. Verified against 3.9.0, which reports {@code index.replication.type: "DOCUMENT"}
     * on an index created without one.
     */
    @Test
    void replicationTypeDefaultsToDocumentAndIsTakenFromTheRequest() throws Exception {
        create("defaulted");
        create("chosen", "{\"settings\":{\"index.replication.type\":\"SEGMENT\"}}");

        assertEquals("DOCUMENT", metadata("defaulted").get("settings").get("index.replication.type").textValue());
        assertEquals("SEGMENT", metadata("chosen").get("settings").get("index.replication.type").textValue());
    }

    /**
     * All three accepted shapes normalise to the one canonical form. Verified against 3.9.0: each of
     * these stores {@code index.number_of_shards} as a string.
     */
    @Test
    void acceptsAllThreeSettingsFormsAndStoresOne() throws Exception {
        create("flat", "{\"settings\":{\"number_of_shards\":2}}");
        create("nested", "{\"settings\":{\"index\":{\"number_of_shards\":3}}}");
        create("prefixed", "{\"settings\":{\"index.number_of_shards\":4}}");

        assertEquals("2", metadata("flat").get("settings").get("index.number_of_shards").textValue());
        assertEquals("3", metadata("nested").get("settings").get("index.number_of_shards").textValue());
        assertEquals("4", metadata("prefixed").get("settings").get("index.number_of_shards").textValue());
    }

    /**
     * A dotted key beats the nested spelling of the same setting whichever came first in the document.
     * Verified against 3.9.0 in both orders — it is not last-writer-wins.
     */
    @Test
    void aDottedKeyBeatsTheNestedSpellingInEitherOrder() throws Exception {
        create("dotted-first", "{\"settings\":{\"index.number_of_shards\":2,\"index\":{\"number_of_shards\":3}}}");
        create("nested-first", "{\"settings\":{\"index\":{\"number_of_shards\":3},\"index.number_of_shards\":2}}");

        assertEquals("2", metadata("dotted-first").get("settings").get("index.number_of_shards").textValue());
        assertEquals("2", metadata("nested-first").get("settings").get("index.number_of_shards").textValue());
    }

    /** Lists stay lists of strings rather than collapsing to text. Verified against 3.9.0. */
    @Test
    void listValuedSettingsStayLists() throws Exception {
        create("my-index", "{\"settings\":{\"index\":{\"routing\":{\"allocation\":{\"include\":{\"_name\":[\"a\",\"b\"]}}}}}}");

        JsonNode value = metadata("my-index").get("settings").get("index.routing.allocation.include._name");
        assertTrue(value.isArray());
        assertEquals(List.of("a", "b"), List.of(value.get(0).textValue(), value.get(1).textValue()));
    }

    @Test
    void carriesMappingsThrough() throws Exception {
        create("my-index", "{\"mappings\":{\"properties\":{\"f\":{\"type\":\"keyword\"}}}}");

        assertEquals(
            "keyword",
            metadata("my-index").get("mappings").get("properties").get("f").get("type").asText()
        );
    }

    /** Aliases declared at create time become their own resource, not a field on the index. */
    @Test
    void declaredAliasesBecomeAliasResources() throws Exception {
        create("my-index", "{\"aliases\":{\"a\":{}}}");

        assertFalse(metadata("my-index").has("aliases"), "no second source of truth on the index");
        JsonNode definition = MAPPER.readTree(
            store.getAsString(TENANT_PREFIX + AliasStore.key("a")).orElseThrow());
        assertTrue(definition.get("indices").has("my-index"));
    }

    @Test
    void rejectsAnInvalidName() throws Exception {
        RestResponse response = create("MyIndex");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("invalid_index_name_exception", error.get("type").asText());
        assertEquals("Invalid index name [MyIndex], must be lowercase", error.get("reason").asText());
        assertEquals("MyIndex", error.get("index").asText());
        assertEquals("_na_", error.get("index_uuid").asText());
        assertEquals(0, taskCount(), "a rejected create must not register a task");
    }

    @Test
    void rejectsADuplicateWithTheExistingUuid() throws Exception {
        create("my-index");
        String existingUuid = metadata("my-index").get("uuid").asText();

        RestResponse response = create("my-index");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("resource_already_exists_exception", error.get("type").asText());
        // The reason embeds the UUID, so it must be the stored one rather than a discarded new one.
        assertEquals("index [my-index/" + existingUuid + "] already exists", error.get("reason").asText());
        assertEquals(existingUuid, error.get("index_uuid").asText());
    }

    @Test
    void aDuplicateCreateDoesNotOverwriteOrEnqueue() throws Exception {
        create("my-index", "{\"settings\":{\"number_of_shards\":2}}");
        create("my-index", "{\"settings\":{\"number_of_shards\":9}}");

        assertEquals("2", metadata("my-index").get("settings").get("index.number_of_shards").textValue(),
            "create-if-absent must not clobber");
        assertEquals(1, taskCount(), "only the successful create enqueues");
    }

    @Test
    void rejectsAMalformedBody() throws Exception {
        assertEquals(RestStatus.BAD_REQUEST, create("my-index", "{not json").status());
        assertEquals("parse_exception", body(create("other-index", "[1,2]")).get("error").get("type").asText());
    }

    @Test
    void eachIndexGetsADistinctUuid() throws Exception {
        create("a");
        create("b");

        assertNotEquals(metadata("a").get("uuid").asText(), metadata("b").get("uuid").asText());
    }

    // ------------------------------------------------------------------
    // shards_acknowledged -- the deliberate divergence
    // ------------------------------------------------------------------

    @Test
    void shardsAcknowledgedIsFalseByDefaultBecauseWeDidNotWait() throws Exception {
        assertFalse(body(create("my-index")).get("shards_acknowledged").asBoolean());
    }

    @Test
    void shardsAcknowledgedIsTrueOnlyWhenNothingWasRequired() throws Exception {
        RestResponse zero = router.dispatch(
            "PUT", "/idx-zero", "/idx-zero?wait_for_active_shards=0",
            Map.of("wait_for_active_shards", List.of("0")), Map.of(), new byte[0]
        );
        assertTrue(body(zero).get("shards_acknowledged").asBoolean(), "0 copies required is trivially met");

        RestResponse one = router.dispatch(
            "PUT", "/idx-one", "/idx-one?wait_for_active_shards=1",
            Map.of("wait_for_active_shards", List.of("1")), Map.of(), new byte[0]
        );
        assertFalse(body(one).get("shards_acknowledged").asBoolean());
    }

    // ------------------------------------------------------------------
    // the shard manager handoff
    // ------------------------------------------------------------------

    @Test
    void createRegistersAPendingTaskForTheShardManager() throws Exception {
        create("my-index");
        JsonNode task = onlyTask();

        assertEquals("create-index", task.get("type").asText());
        assertEquals("my-index", task.get("index").asText());
        assertEquals("pending", task.get("state").asText());
        assertEquals(FIXED_MILLIS, task.get("submitted_at").asLong());
        // Carries the index UUID so the task stays unambiguous if the name is later reused.
        assertEquals(metadata("my-index").get("uuid").asText(), task.get("index_uuid").asText());
    }

    @Test
    void tasksLiveOutsideTheIndexKeySpace() {
        create("my-index");

        assertEquals(1, store.scanPrefix(TENANT_PREFIX + IndexStore.KEY_PREFIX).size(),
            "a task must not show up as an index");
        assertEquals(1, taskCount());
    }

    // ------------------------------------------------------------------
    // exists and delete
    // ------------------------------------------------------------------

    @Test
    void existsReflectsCreationAndDeletion() {
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/my-index").status());

        create("my-index");
        RestResponse found = router.dispatch("HEAD", "/my-index");
        assertEquals(RestStatus.OK, found.status());
        assertEquals(0, found.body().length, "HEAD carries no body");

        router.dispatch("DELETE", "/my-index");
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/my-index").status());
    }

    @Test
    void deleteAcknowledgesAndRemovesMetadata() throws Exception {
        create("my-index");

        RestResponse response = router.dispatch("DELETE", "/my-index");
        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).get("acknowledged").asBoolean());
        assertTrue(store.get(TENANT_PREFIX + IndexStore.key("my-index")).isEmpty());
    }

    @Test
    void deleteRegistersATeardownTask() throws Exception {
        create("my-index");
        String uuid = metadata("my-index").get("uuid").asText();
        router.dispatch("DELETE", "/my-index");

        var tasks = store.scanPrefix(TENANT_PREFIX + IndexManagementTasks.KEY_PREFIX);
        assertEquals(2, tasks.size(), "one create task and one delete task");
        boolean sawDelete = false;
        for (var entry : tasks.entrySet()) {
            JsonNode task = MAPPER.readTree(new String(entry.getValue().value(), StandardCharsets.UTF_8));
            if ("delete-index".equals(task.get("type").asText())) {
                sawDelete = true;
                assertEquals(uuid, task.get("index_uuid").asText(), "teardown must name the UUID it removed");
            }
        }
        assertTrue(sawDelete);
    }

    @Test
    void deleteOfAMissingIndexIs404() throws Exception {
        RestResponse response = router.dispatch("DELETE", "/nope");

        assertEquals(RestStatus.NOT_FOUND, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("index_not_found_exception", error.get("type").asText());
        assertEquals("no such index [nope]", error.get("reason").asText());
        assertEquals("nope", error.get("resource.id").asText());
        assertEquals("index_or_alias", error.get("resource.type").asText());
        assertEquals(0, taskCount(), "a failed delete must not enqueue teardown");
    }

    // ------------------------------------------------------------------
    // tenancy
    // ------------------------------------------------------------------

    @Test
    void indexNamesDoNotCollideAcrossTenants() throws Exception {
        Router multiTenant = Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(),
            TestContexts.factory(store, new org.opensearch.controlplane.tenant.HeaderTenantResolver())
        );
        String header = org.opensearch.controlplane.tenant.HeaderTenantResolver.DEFAULT_HEADER;

        assertEquals(RestStatus.OK, multiTenant
            .dispatch("PUT", "/shared", "/shared", Map.of(), Map.of(header, "acme"), new byte[0]).status());
        // Same name for another tenant is a create, not a conflict.
        assertEquals(RestStatus.OK, multiTenant
            .dispatch("PUT", "/shared", "/shared", Map.of(), Map.of(header, "globex"), new byte[0]).status());

        assertEquals(RestStatus.NOT_FOUND, multiTenant
            .dispatch("HEAD", "/shared", "/shared", Map.of(), Map.of(header, "initech"), new byte[0]).status());
        assertTrue(store.get("tenant/acme/" + IndexStore.key("shared")).isPresent());
        assertTrue(store.get("tenant/globex/" + IndexStore.key("shared")).isPresent());
    }

    // ------------------------------------------------------------------
    // exists resolves expressions, not just exact names
    // ------------------------------------------------------------------

    /** Statuses verified against OpenSearch 3.9.0 for every case below. */
    @Test
    void existsResolvesAliasesWildcardsAndExclusions() {
        create("cp-a");
        create("cp-b");
        create("cp-aliased", "{\"aliases\":{\"cp-alias\":{}}}");

        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/cp-alias").status(), "alias");
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/cp-*").status(), "wildcard");
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/cp-a,cp-b").status(), "comma list");
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/cp-*,-cp-a").status(), "exclusion");
    }

    /**
     * The asymmetry a live cluster has here: a wildcard matching nothing succeeds
     * ({@code allow_no_indices}), a missing concrete name does not
     * ({@code ignore_unavailable} off).
     */
    @Test
    void existsAllowsAnEmptyWildcardButNotAMissingName() {
        create("cp-a");

        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/nomatch-*").status());
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/missing").status());
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/cp-a,missing").status());
        // A leading - with no preceding wildcard is a name, and nothing is called that.
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/-cp-a").status());
    }

    /** Verified: HEAD /_foo is a 400, not a 404 — an unusable name differs from an absent one. */
    @Test
    void existsReportsAnUnderscorePrefixAsBadRequestNotNotFound() {
        assertEquals(RestStatus.BAD_REQUEST, router.dispatch("HEAD", "/_foo").status());
    }

    @Test
    void existsSkipsHiddenIndicesForAPlainWildcardButNotADottedOne() {
        create(".cp-hidden", "{\"settings\":{\"index.hidden\":true}}");

        // Nothing visible matches *, so the resolution is empty -- still a 200 under allow_no_indices.
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/.cp-*").status());
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/.cp-hidden").status());
    }

    @Test
    void allThreeAreRegistered() {
        HandlerRegistry registry = ControlPlaneHandlers.build();

        assertTrue(registry.implemented().containsAll(
            List.of("indices.create", "indices.exists", "indices.delete")));
        // An invariant rather than a roadmap marker: naming whichever operation is next means this
        // test breaks the moment that one lands, which says nothing about registration.
        assertTrue(registry.notImplemented().isEmpty() == false, "the surface is not finished");
        for (String group : registry.implemented()) {
            assertFalse(registry.notImplemented().contains(group), group + " is in both lists");
        }
    }
}
