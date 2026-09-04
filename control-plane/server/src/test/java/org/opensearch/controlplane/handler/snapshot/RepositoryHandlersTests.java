/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.snapshot;

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
 * Snapshot repository configuration: {@code PUT}, {@code GET} and {@code DELETE} on
 * {@code /_snapshot/{repository}}.
 *
 * <p>Every error path here was verified against OpenSearch 3.9.0. The <b>success</b> shapes could not
 * be: creating a real repository needs {@code path.repo} configured on the cluster, which the probe
 * cluster does not have, so {@code {"acknowledged": true}} and the name-keyed GET body come from the
 * spec's schemas rather than from a diff. Marked in the README's "Still unverified" accordingly.
 */
class RepositoryHandlersTests {

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

    private void create(String name, String body) {
        assertEquals(RestStatus.OK, send("PUT", "/_snapshot/" + name, body).status(), name);
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    private String reasonOf(RestResponse response) throws Exception {
        return body(response).get("error").get("reason").asText();
    }

    // ------------------------------------------------------------------
    // round trip
    // ------------------------------------------------------------------

    @Test
    void storesAndReturnsADefinition() throws Exception {
        create("backups", "{\"type\":\"fs\",\"settings\":{\"location\":\"/mnt/snaps\",\"compress\":true}}");

        JsonNode repository = body(send("GET", "/_snapshot/backups", null)).get("backups");
        assertEquals("fs", repository.get("type").asText());
        assertEquals("/mnt/snaps", repository.get("settings").get("location").asText());
        assertEquals("true", repository.get("settings").get("compress").asText(),
            "settings are stringified like every other settings document");
    }

    /** Repository settings carry no implicit prefix, unlike index settings. */
    @Test
    void settingsAreNotIndexPrefixed() throws Exception {
        create("backups", "{\"type\":\"fs\",\"settings\":{\"location\":\"/mnt/snaps\"}}");

        String stored = store.getAsString("tenant/default/" + RepositoryStore.key("backups")).orElseThrow();
        assertTrue(stored.contains("\"location\""), stored);
        assertFalse(stored.contains("index.location"), stored);
    }

    /** Nested and dotted settings both normalise, as everywhere else. */
    @Test
    void acceptsNestedAndDottedSettings() throws Exception {
        create("a", "{\"type\":\"s3\",\"settings\":{\"chunk\":{\"size\":\"1gb\"}}}");
        create("b", "{\"type\":\"s3\",\"settings\":{\"chunk.size\":\"1gb\"}}");

        for (String name : List.of("a", "b")) {
            assertEquals("1gb",
                body(send("GET", "/_snapshot/" + name, null)).get(name)
                    .get("settings").get("chunk").get("size").asText(),
                name);
        }
    }

    @Test
    void putIsAnUpsert() throws Exception {
        create("backups", "{\"type\":\"fs\",\"settings\":{\"location\":\"/one\"}}");
        create("backups", "{\"type\":\"fs\",\"settings\":{\"location\":\"/two\"}}");

        assertEquals("/two", body(send("GET", "/_snapshot/backups", null))
            .get("backups").get("settings").get("location").asText());
    }

    @Test
    void deleteRemovesItAndAcknowledges() throws Exception {
        create("backups", "{\"type\":\"fs\"}");

        assertTrue(body(send("DELETE", "/_snapshot/backups", null)).get("acknowledged").asBoolean());
        assertEquals(RestStatus.NOT_FOUND, send("GET", "/_snapshot/backups", null).status());
    }

    // ------------------------------------------------------------------
    // the name expression, whose rules are unlike every other one here
    // ------------------------------------------------------------------

    /** Absent and {@code _all} both mean everything, and both are a 200 when there is nothing. */
    @Test
    void absentAndAllAreTwoHundredEvenWhenEmpty() throws Exception {
        assertEquals(0, body(send("GET", "/_snapshot", null)).size());
        assertEquals(0, body(send("GET", "/_snapshot/_all", null)).size());
    }

    /**
     * The asymmetry worth pinning: {@code *} is a pattern and matching nothing is a 404, where
     * {@code _all} is not a pattern and answers {@code {}}. That is the reverse of
     * {@code indices.resolve_index}, where {@code _all} is the literal.
     */
    @Test
    void aWildcardMatchingNothingIsNotFoundWhereAllIsNot() throws Exception {
        RestResponse response = send("GET", "/_snapshot/*", null);

        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals("repository_missing_exception", body(response).get("error").get("type").asText());
        assertEquals("[*] missing", reasonOf(response), "the pattern is named, not expanded");

        assertEquals(RestStatus.OK, send("GET", "/_snapshot/_all", null).status());
    }

    @Test
    void aWildcardMatchesWhatItShould() throws Exception {
        create("backup-a", "{\"type\":\"fs\"}");
        create("backup-b", "{\"type\":\"fs\"}");
        create("other", "{\"type\":\"fs\"}");

        assertEquals(List.of("backup-a", "backup-b"),
            body(send("GET", "/_snapshot/backup-*", null)).propertyStream().map(Map.Entry::getKey).toList());
    }

    /**
     * A comma-separated list collects names in request order and reports the first that does not exist.
     * From {@code TransportGetRepositoriesAction}, which builds a {@code LinkedHashSet} to keep that
     * order precisely so the message is predictable.
     */
    @Test
    void aCommaListNamesTheFirstMissingName() throws Exception {
        create("present", "{\"type\":\"fs\"}");

        assertEquals("[absent] missing", reasonOf(send("GET", "/_snapshot/absent,present", null)));
        assertEquals("[gone] missing", reasonOf(send("GET", "/_snapshot/present,gone", null)));
    }

    /**
     * Once a repository has existed, a pattern matching nothing contributes nothing rather than erroring
     * — so an expression of only such patterns answers a bare {@code {}}. On a pristine cluster the same
     * request is a 404. See {@link RepositoryStore} on the metadata marker.
     *
     * <p>This is the trap: probing only an empty cluster shows the 404 and hides the rule.
     */
    @Test
    void aPatternMatchingNothingIsFineOnceARepositoryHasExisted() throws Exception {
        assertEquals(RestStatus.NOT_FOUND, send("GET", "/_snapshot/nope*", null).status(),
            "pristine: the early branch names the token verbatim");
        assertEquals("[nope*] missing", reasonOf(send("GET", "/_snapshot/nope*", null)));

        create("present", "{\"type\":\"fs\"}");

        RestResponse afterwards = send("GET", "/_snapshot/nope*", null);
        assertEquals(RestStatus.OK, afterwards.status(), "metadata now exists, so the pattern just matches nothing");
        assertEquals(0, body(afterwards).size());
    }

    /** Deleting is a single glob, and match-all is exempt from the 404 where a partial wildcard is not. */
    @Test
    void deleteTreatsMatchAllAsExemptButNotPartialWildcards() throws Exception {
        create("backup-a", "{\"type\":\"fs\"}");

        assertEquals(RestStatus.NOT_FOUND, send("DELETE", "/_snapshot/nope*", null).status());
        assertEquals("[nope*] missing", reasonOf(send("DELETE", "/_snapshot/nope*", null)));

        assertEquals(RestStatus.OK, send("DELETE", "/_snapshot/backup-*", null).status());
        assertEquals(RestStatus.OK, send("DELETE", "/_snapshot/*", null).status(),
            "match-all over nothing is a no-op, not a 404");
    }

    @Test
    void missingIsNotFoundOnBothReadAndDelete() throws Exception {
        for (String method : List.of("GET", "DELETE")) {
            RestResponse response = send(method, "/_snapshot/nope", null);
            assertEquals(RestStatus.NOT_FOUND, response.status(), method);
            assertEquals("repository_missing_exception", body(response).get("error").get("type").asText(), method);
            assertEquals("[nope] missing", reasonOf(response), method);
        }
    }

    // ------------------------------------------------------------------
    // rejections
    // ------------------------------------------------------------------

    /** Byte-exact, including the semicolon and the absent space after it. */
    @Test
    void rejectsAMissingType() throws Exception {
        for (String body : List.of("{}", "{\"settings\":{\"location\":\"/x\"}}")) {
            RestResponse response = send("PUT", "/_snapshot/backups", body);
            assertEquals(RestStatus.BAD_REQUEST, response.status(), body);
            assertEquals("action_request_validation_exception",
                body(response).get("error").get("type").asText(), body);
            assertEquals("Validation Failed: 1: type is missing;", reasonOf(response), body);
        }
    }

    @Test
    void rejectsAMissingOrMalformedBody() throws Exception {
        RestResponse noBody = send("PUT", "/_snapshot/backups", null);
        assertEquals(RestStatus.BAD_REQUEST, noBody.status());
        assertEquals("parse_exception", body(noBody).get("error").get("type").asText());
        assertEquals("request body is required", reasonOf(noBody));

        assertEquals(RestStatus.BAD_REQUEST, send("PUT", "/_snapshot/backups", "{not json").status());
    }

    /**
     * Any type is accepted, deliberately. A real cluster answers {@code 500 repository_exception},
     * {@code [x] repository type [nonesuch] does not exist} — but which types exist depends on installed
     * plugins and, unlike processors, a cluster reports nothing about them anywhere. See
     * {@link RepositoryHandlers}.
     */
    @Test
    void anUnknownTypeIsAcceptedBecauseThereIsNoListToCheckAgainst() throws Exception {
        assertEquals(RestStatus.OK, send("PUT", "/_snapshot/backups", "{\"type\":\"nonesuch\"}").status());
    }

    @Test
    void allThreeGroupsAreRegistered() {
        var registry = ControlPlaneHandlers.build();

        for (String group : List.of("snapshot.create_repository", "snapshot.get_repository",
            "snapshot.delete_repository")) {
            assertTrue(registry.implemented().contains(group), group);
        }
        assertTrue(registry.notImplemented().contains("snapshot.create"),
            "taking a snapshot stays a data plane operation");
    }
}
