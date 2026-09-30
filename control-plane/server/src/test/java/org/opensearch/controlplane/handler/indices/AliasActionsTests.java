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
import org.opensearch.controlplane.store.InterleavingKeyValueStore;
import org.opensearch.controlplane.tenant.SingleTenantResolver;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Statuses and error bodies verified against OpenSearch 3.9.0. */
class AliasActionsTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TENANT_PREFIX = SingleTenantResolver.DEFAULT_TENANT.keyPrefix();

    private InMemoryKeyValueStore store;
    private InterleavingKeyValueStore interleaving;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        interleaving = new InterleavingKeyValueStore(store);
        router = Router.fromGeneratedRoutes(ControlPlaneHandlers.build(), TestContexts.factory(interleaving));
        create("logs-v1");
        create("logs-v2");
    }

    private void create(String index) {
        router.dispatch("PUT", "/" + index, "/" + index, Map.of(), Map.of(), new byte[0]);
    }

    private RestResponse create(String index, String body) {
        return router.dispatch("PUT", "/" + index, "/" + index, Map.of(), Map.of(),
            body.getBytes(StandardCharsets.UTF_8));
    }

    /** Another writer's version of an alias, written straight to the store. */
    private Runnable concurrentWrite(String alias, String definition) {
        return () -> store.put(TENANT_PREFIX + AliasStore.key(alias), definition.getBytes(StandardCharsets.UTF_8));
    }

    private String aliasKey(String alias) {
        return TENANT_PREFIX + AliasStore.key(alias);
    }

    private RestResponse actions(String body) {
        return router.dispatch("POST", "/_aliases", "/_aliases", Map.of(), Map.of(),
            body.getBytes(StandardCharsets.UTF_8));
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private List<String> membersOf(String alias) throws Exception {
        return store.getAsString(TENANT_PREFIX + AliasStore.key(alias))
            .map(json -> {
                try {
                    JsonNode indices = MAPPER.readTree(json).get("indices");
                    return indices.propertyStream().map(Map.Entry::getKey).sorted().toList();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            })
            .orElse(List.of());
    }

    // ------------------------------------------------------------------
    // the operation the data model exists for
    // ------------------------------------------------------------------

    /**
     * The zero-downtime cutover. Both actions touch one alias document, so this is a single write and
     * there is no instant at which {@code logs} points at nothing.
     */
    @Test
    void anAtomicSwapMovesAnAliasBetweenIndices() throws Exception {
        assertEquals(RestStatus.OK, actions(
            "{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"logs\"}}]}").status());
        assertEquals(List.of("logs-v1"), membersOf("logs"));

        RestResponse swap = actions("{\"actions\":["
            + "{\"remove\":{\"index\":\"logs-v1\",\"alias\":\"logs\"}},"
            + "{\"add\":{\"index\":\"logs-v2\",\"alias\":\"logs\"}}]}");

        assertEquals(RestStatus.OK, swap.status());
        assertTrue(body(swap).get("acknowledged").asBoolean());
        assertEquals(List.of("logs-v2"), membersOf("logs"), "moved, never absent");
    }

    @Test
    void theSwapIsASingleKeyWrite() {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"logs\"}}]}");
        long before = store.revisionOf(TENANT_PREFIX + AliasStore.key("logs"));

        actions("{\"actions\":["
            + "{\"remove\":{\"index\":\"logs-v1\",\"alias\":\"logs\"}},"
            + "{\"add\":{\"index\":\"logs-v2\",\"alias\":\"logs\"}}]}");

        // One revision bump: both actions were staged and written together.
        assertEquals(before + 1, store.revisionOf(TENANT_PREFIX + AliasStore.key("logs")));
    }

    // ------------------------------------------------------------------
    // add and remove
    // ------------------------------------------------------------------

    @Test
    void addAcceptsAnIndicesListAndWildcards() throws Exception {
        actions("{\"actions\":[{\"add\":{\"indices\":[\"logs-v1\",\"logs-v2\"],\"alias\":\"both\"}}]}");
        assertEquals(List.of("logs-v1", "logs-v2"), membersOf("both"));

        actions("{\"actions\":[{\"add\":{\"index\":\"logs-*\",\"alias\":\"wild\"}}]}");
        assertEquals(List.of("logs-v1", "logs-v2"), membersOf("wild"));
    }

    @Test
    void addStoresPerIndexProperties() throws Exception {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"f\","
            + "\"filter\":{\"term\":{\"x\":1}},\"search_routing\":\"r\"}}]}");

        JsonNode member = MAPPER.readTree(
            store.getAsString(TENANT_PREFIX + AliasStore.key("f")).orElseThrow())
            .get("indices").get("logs-v1");
        assertEquals(1, member.get("filter").get("term").get("x").asInt());
        assertEquals("r", member.get("search_routing").asText());
    }

    @Test
    void removingTheLastIndexDeletesTheAlias() {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"gone\"}}]}");
        actions("{\"actions\":[{\"remove\":{\"index\":\"logs-v1\",\"alias\":\"gone\"}}]}");

        assertTrue(store.get(TENANT_PREFIX + AliasStore.key("gone")).isEmpty(),
            "an alias pointing at nothing should not exist");
    }

    // ------------------------------------------------------------------
    // validation
    // ------------------------------------------------------------------

    @Test
    void anEmptyActionListIsRejected() throws Exception {
        RestResponse response = actions("{\"actions\":[]}");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("No action specified", body(response).get("error").get("reason").asText());
    }

    @Test
    void removingAnAbsentAliasIs404UnlessMustExistIsFalse() throws Exception {
        RestResponse strict = actions("{\"actions\":[{\"remove\":{\"index\":\"logs-v1\",\"alias\":\"nope\"}}]}");
        assertEquals(RestStatus.NOT_FOUND, strict.status());
        JsonNode error = body(strict).get("error");
        assertEquals("aliases_not_found_exception", error.get("type").asText());
        assertEquals("aliases [nope] missing", error.get("reason").asText());
        assertEquals("aliases", error.get("resource.type").asText());

        assertEquals(RestStatus.OK, actions(
            "{\"actions\":[{\"remove\":{\"index\":\"logs-v1\",\"alias\":\"nope\",\"must_exist\":false}}]}").status());
    }

    @Test
    void anAliasNameTakenByAnIndexIsRejected() throws Exception {
        RestResponse response = actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"logs-v2\"}}]}");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("invalid_alias_name_exception", error.get("type").asText());
        assertEquals(
            "Invalid alias name [logs-v2], an index exists with the same name as the alias",
            error.get("reason").asText()
        );
    }

    @Test
    void namingAMissingIndexIs404() throws Exception {
        RestResponse response = actions("{\"actions\":[{\"add\":{\"index\":\"nope\",\"alias\":\"a\"}}]}");

        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals("index_not_found_exception", body(response).get("error").get("type").asText());
    }

    /**
     * Considered divergence: OpenSearch answers 500 illegal_state_exception here. A request the client
     * got wrong is a client error, so this answers 400 with the same reason.
     */
    @Test
    void twoWriteIndicesIsRejectedAs400NotTheUpstream500() throws Exception {
        RestResponse response = actions("{\"actions\":["
            + "{\"add\":{\"index\":\"logs-v1\",\"alias\":\"w\",\"is_write_index\":true}},"
            + "{\"add\":{\"index\":\"logs-v2\",\"alias\":\"w\",\"is_write_index\":true}}]}");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("illegal_argument_exception", error.get("type").asText());
        assertTrue(error.get("reason").asText().startsWith("alias [w] has more than one write index ["));
        assertEquals(List.of(), membersOf("w"), "a rejected request writes nothing");
    }

    @Test
    void nothingIsWrittenWhenALaterActionFails() throws Exception {
        RestResponse response = actions("{\"actions\":["
            + "{\"add\":{\"index\":\"logs-v1\",\"alias\":\"ok\"}},"
            + "{\"add\":{\"index\":\"nope\",\"alias\":\"bad\"}}]}");

        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals(List.of(), membersOf("ok"), "validation completes before any write");
    }

    // ------------------------------------------------------------------
    // put_alias and delete_alias are the same engine
    // ------------------------------------------------------------------

    @Test
    void putAliasAddsOne() throws Exception {
        RestResponse response = router.dispatch("PUT", "/logs-v1/_alias/single");

        assertEquals(RestStatus.OK, response.status());
        assertTrue(body(response).get("acknowledged").asBoolean());
        assertEquals(List.of("logs-v1"), membersOf("single"));
    }

    @Test
    void putAliasTakesIndexAndAliasFromTheBodyWhenThePathOmitsThem() throws Exception {
        RestResponse response = router.dispatch("PUT", "/_alias", "/_alias", Map.of(), Map.of(),
            "{\"index\":\"logs-v1\",\"alias\":\"from-body\"}".getBytes(StandardCharsets.UTF_8));

        assertEquals(RestStatus.OK, response.status());
        assertEquals(List.of("logs-v1"), membersOf("from-body"));
    }

    @Test
    void deleteAliasRemovesOneAndReportsAMissingOne() throws Exception {
        router.dispatch("PUT", "/logs-v1/_alias/doomed");
        assertEquals(RestStatus.OK, router.dispatch("DELETE", "/logs-v1/_alias/doomed").status());
        assertEquals(List.of(), membersOf("doomed"));

        RestResponse missing = router.dispatch("DELETE", "/logs-v1/_alias/never");
        assertEquals(RestStatus.NOT_FOUND, missing.status());
        assertEquals("aliases_not_found_exception", body(missing).get("error").get("type").asText());
    }

    // ------------------------------------------------------------------
    // interaction with index lifecycle and the resolver
    // ------------------------------------------------------------------

    @Test
    void deletingAnIndexRemovesItFromItsAliases() throws Exception {
        actions("{\"actions\":[{\"add\":{\"indices\":[\"logs-v1\",\"logs-v2\"],\"alias\":\"both\"}}]}");

        router.dispatch("DELETE", "/logs-v1");
        assertEquals(List.of("logs-v2"), membersOf("both"), "no dangling membership");
    }

    @Test
    void anAliasResolvesForExists() {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"resolvable\"}}]}");

        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/resolvable").status());
    }

    // ------------------------------------------------------------------
    // reads: get_alias and exists_alias
    // ------------------------------------------------------------------

    private RestResponse get(String path) {
        return router.dispatch("GET", path);
    }

    /** Responses are index-keyed even when the request names an alias. Verified. */
    @Test
    void getAliasAnswersIndexKeyedDocuments() throws Exception {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v2\",\"alias\":\"logs\"}}]}");

        JsonNode json = body(get("/_alias/logs"));
        assertEquals(List.of("logs-v2"), json.propertyStream().map(Map.Entry::getKey).toList());
        assertTrue(json.get("logs-v2").get("aliases").has("logs"));
    }

    @Test
    void getAliasSupportsAliasWildcards() throws Exception {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v2\",\"alias\":\"logs\"}}]}");

        assertTrue(body(get("/_alias/lo*")).has("logs-v2"));
    }

    /**
     * The rule that decides both membership and status: with no alias name every matching index is
     * reported, even one with no aliases; with an alias name, only matches, and none is a 404.
     */
    @Test
    void anIndexWithNoAliasesAppearsOnlyWhenNoAliasNameIsGiven() throws Exception {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v2\",\"alias\":\"logs\"}}]}");

        JsonNode unfiltered = body(get("/_alias"));
        assertTrue(unfiltered.has("logs-v1"), "reported despite having no aliases");
        assertTrue(unfiltered.get("logs-v1").get("aliases").isEmpty());

        assertEquals(RestStatus.OK, get("/logs-v1/_alias").status());
        assertTrue(body(get("/logs-v1/_alias")).get("logs-v1").get("aliases").isEmpty());

        // Filtered by an alias the index does not carry: omitted, and so a 404.
        assertEquals(RestStatus.NOT_FOUND, get("/logs-v1/_alias/logs").status());
    }

    /** A read of a missing alias uses the bare-string shape; a delete uses the structured one. */
    @Test
    void readingAMissingAliasUsesTheBareStringShape() throws Exception {
        RestResponse response = get("/_alias/never");

        assertEquals(RestStatus.NOT_FOUND, response.status());
        JsonNode json = body(response);
        assertTrue(json.get("error").isTextual(), "bare string, unlike the delete path");
        assertEquals("alias [never] missing", json.get("error").asText());
        assertEquals(404, json.get("status").asInt());
    }

    @Test
    void existsAliasIsScopedByIndex() {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v2\",\"alias\":\"logs\"}}]}");

        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/_alias/logs").status());
        assertEquals(RestStatus.OK, router.dispatch("HEAD", "/logs-v2/_alias/logs").status());
        // Both exist, but not together.
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/logs-v1/_alias/logs").status());
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/_alias/never").status());
    }

    @Test
    void existsAliasCarriesNoBody() {
        assertEquals(0, router.dispatch("HEAD", "/_alias/never").body().length);
    }

    @Test
    void getAliasReportsPerIndexProperties() throws Exception {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"a\",\"search_routing\":\"r\"}},"
            + "{\"add\":{\"index\":\"logs-v2\",\"alias\":\"a\"}}]}");

        JsonNode json = body(get("/_alias/a"));
        assertEquals("r", json.get("logs-v1").get("aliases").get("a").get("search_routing").asText());
        assertTrue(json.get("logs-v2").get("aliases").get("a").isEmpty(), "properties are per index");
    }

    @Test
    void removeIndexActionDeletesTheIndexItself() throws Exception {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"a\"}}]}");

        assertEquals(RestStatus.OK, actions("{\"actions\":[{\"remove_index\":{\"index\":\"logs-v1\"}}]}").status());
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/logs-v1").status());
        assertFalse(membersOf("a").contains("logs-v1"));
    }

    // ------------------------------------------------------------------
    // concurrent writers
    // ------------------------------------------------------------------

    /** Each alias write is a compare-and-set, so two requests on one alias cannot both win. */
    @Test
    void aConcurrentChangeToTheSameAliasIsAConflictNotALostUpdate() throws Exception {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"logs\"}}]}");
        interleaving.afterNextGet(aliasKey("logs"),
            concurrentWrite("logs", "{\"indices\":{\"logs-v1\":{},\"other\":{}}}"));

        RestResponse response = actions("{\"actions\":[{\"add\":{\"index\":\"logs-v2\",\"alias\":\"logs\"}}]}");
        assertEquals(RestStatus.CONFLICT, response.status());
        assertEquals("alias [logs] was updated concurrently; re-read and retry",
            body(response).get("error").get("reason").asText());
        assertEquals(List.of("logs-v1", "other"), membersOf("logs"), "the other writer's change survives");
    }

    /** Dropping a deleted index from an alias is retried rather than reported: it is still right. */
    @Test
    void deletingAnIndexRetriesItsAliasCleanup() throws Exception {
        actions("{\"actions\":[{\"add\":{\"indices\":[\"logs-v1\",\"logs-v2\"],\"alias\":\"logs\"}}]}");
        interleaving.afterNextGet(aliasKey("logs"),
            concurrentWrite("logs", "{\"indices\":{\"logs-v1\":{},\"logs-v2\":{},\"other\":{}}}"));

        assertEquals(RestStatus.OK, router.dispatch("DELETE", "/logs-v1").status());
        assertEquals(List.of("logs-v2", "other"), membersOf("logs"));
    }

    // ------------------------------------------------------------------
    // indices.create runs the same checks
    // ------------------------------------------------------------------

    /** Upstream's validateIndexName: an index may not take an alias's name and shadow it. */
    @Test
    void createRejectsAnIndexNamedAfterAnAlias() throws Exception {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"logs\"}}]}");

        RestResponse response = create("logs", "{}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        JsonNode error = body(response).get("error");
        assertEquals("invalid_index_name_exception", error.get("type").asText());
        assertEquals("Invalid index name [logs], already exists as alias", error.get("reason").asText());
        // Not HEAD /logs, which resolves the alias.
        assertTrue(store.getAsString(TENANT_PREFIX + IndexStore.key("logs")).isEmpty());
    }

    @Test
    void createRejectsARequestAliasNamedAfterAnIndex() throws Exception {
        RestResponse response = create("new", "{\"aliases\":{\"logs-v1\":{}}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("invalid_alias_name_exception", body(response).get("error").get("type").asText());
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/new").status());
    }

    /** Upstream's 500 from its metadata builder, reported as a 400 with the same reason. */
    @Test
    void createRejectsAnAliasNamedAfterTheIndexItself() throws Exception {
        RestResponse response = create("new", "{\"aliases\":{\"new\":{}}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertTrue(body(response).get("error").get("reason").asText().startsWith(
            "index, alias, and data stream names need to be unique, but the following duplicates were found "
                + "[new (alias of [new/"));
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/new").status());
    }

    @Test
    void createRejectsASecondWriteIndex() throws Exception {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"w\",\"is_write_index\":true}}]}");

        RestResponse response = create("new", "{\"aliases\":{\"w\":{\"is_write_index\":true}}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("alias [w] has more than one write index [logs-v1,new]",
            body(response).get("error").get("reason").asText());
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/new").status());
        assertEquals(List.of("logs-v1"), membersOf("w"));
    }

    /** The checks are repeated at write time; one that fails then undoes the create. */
    @Test
    void createRollsBackWhenAnAliasChangesUnderIt() throws Exception {
        interleaving.afterNextGet(aliasKey("w"),
            concurrentWrite("w", "{\"indices\":{\"logs-v1\":{\"is_write_index\":true}}}"));

        RestResponse response = create("new", "{\"aliases\":{\"w\":{\"is_write_index\":true}}}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals(RestStatus.NOT_FOUND, router.dispatch("HEAD", "/new").status());
        assertEquals(List.of("logs-v1"), membersOf("w"));
    }

    /** Adding one member is still right after a lost race, so create retries instead of failing. */
    @Test
    void createRetriesAnAliasWriteItLost() throws Exception {
        actions("{\"actions\":[{\"add\":{\"index\":\"logs-v1\",\"alias\":\"all\"}}]}");
        // The first read is the up-front check; the race is against the second, the write's own read.
        interleaving.afterNextGet(aliasKey("all"), () -> interleaving.afterNextGet(aliasKey("all"),
            concurrentWrite("all", "{\"indices\":{\"logs-v1\":{},\"logs-v2\":{}}}")));

        assertEquals(RestStatus.OK, create("new", "{\"aliases\":{\"all\":{}}}").status());
        assertEquals(List.of("logs-v1", "logs-v2", "new"), membersOf("all"));
    }
}
