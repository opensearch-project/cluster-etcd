/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.controlplane.TestContexts;
import org.opensearch.controlplane.handler.ControlPlaneHandlers;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.router.Router;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.tenant.HeaderTenantResolver;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Weighted shard routing, verified against OpenSearch 3.9.0.
 *
 * <p>Every expected message and status here was taken from a live cluster rather than from the spec —
 * see {@link WeightedRoutingHandlers} for the behaviours that are surprising enough to have needed it.
 */
class WeightedRoutingHandlerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String WEIGHTS_PATH = "/_cluster/routing/awareness/zone/weights";
    private static final String DELETE_PATH = "/_cluster/routing/awareness/weights";

    private InMemoryKeyValueStore store;
    private Router router;

    @BeforeEach
    void setUp() {
        store = new InMemoryKeyValueStore();
        router = Router.fromGeneratedRoutes(ControlPlaneHandlers.build(), TestContexts.factory(store));
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private void awarenessAttributes(String value) {
        RestResponse response = dispatch("PUT", "/_cluster/settings",
            "{\"persistent\":{\"cluster.routing.allocation.awareness.attributes\":\"" + value + "\"}}");
        assertEquals(RestStatus.OK, response.status());
    }

    private void forcedValues(String attribute, String values) {
        RestResponse response = dispatch("PUT", "/_cluster/settings",
            "{\"persistent\":{\"cluster.routing.allocation.awareness.force." + attribute
                + ".values\":\"" + values + "\"}}");
        assertEquals(RestStatus.OK, response.status());
    }

    private RestResponse dispatch(String method, String path, String body) {
        return router.dispatch(method, path, path, Map.of(), Map.of(),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8));
    }

    private RestResponse put(String body) {
        return dispatch("PUT", WEIGHTS_PATH, body);
    }

    private RestResponse get() {
        return dispatch("GET", WEIGHTS_PATH, null);
    }

    private RestResponse get(String attribute) {
        String path = "/_cluster/routing/awareness/" + attribute + "/weights";
        return dispatch("GET", path, null);
    }

    private RestResponse delete(String body) {
        return dispatch("DELETE", DELETE_PATH, body);
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
    // the awareness attribute gate
    // ------------------------------------------------------------------

    @Test
    void readingAnAttributeThatIsNotAnAwarenessAttributeIsRejected() throws Exception {
        RestResponse response = get();

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("action_request_validation_exception", typeOf(response));
        assertEquals("Validation Failed: 1: invalid awareness attribute zone requested for weighted routing;",
            reasonOf(response));
    }

    @Test
    void writingAnAttributeThatIsNotAnAwarenessAttributeIsRejected() throws Exception {
        RestResponse response = put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");

        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("Validation Failed: 1: invalid awareness attribute zone requested for weighted routing;",
            reasonOf(response));
    }

    /** A malformed body outranks the attribute check, because validate() runs before the transport action. */
    @Test
    void anInvalidBodyIsReportedBeforeAnInvalidAttribute() throws Exception {
        RestResponse response = put("{\"weights\":{},\"_version\":1}");

        assertEquals("Validation Failed: 1: Weights are missing;", reasonOf(response));
    }

    /** Both spellings of a list setting work, so a comma-separated string configures two attributes. */
    @Test
    void awarenessAttributesMayBeCommaSeparated() {
        awarenessAttributes("zone,rack");

        assertEquals(RestStatus.OK, get("zone").status());
        assertEquals(RestStatus.OK, get("rack").status());
    }

    // ------------------------------------------------------------------
    // reading
    // ------------------------------------------------------------------

    @Test
    void aClusterThatHasNeverHadWeightsAnswersAnEmptyObject() throws Exception {
        awarenessAttributes("zone");

        RestResponse response = get();
        assertEquals(RestStatus.OK, response.status());
        assertEquals(0, body(response).size());
    }

    @Test
    void weightsComeBackAsStringifiedDoubles() throws Exception {
        awarenessAttributes("zone");
        assertEquals(RestStatus.OK, put("{\"weights\":{\"a\":\"1\",\"b\":\"2.5\"},\"_version\":-1}").status());

        JsonNode weights = body(get()).get("weights");
        assertEquals("1.0", weights.get("a").textValue());
        assertEquals("2.5", weights.get("b").textValue());
    }

    @Test
    void aReadReportsTheVersionAndThatAnAuthorityWasFound() throws Exception {
        awarenessAttributes("zone");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");

        JsonNode read = body(get());
        assertEquals(0, read.get("_version").longValue());
        assertTrue(read.get("discovered_cluster_manager").booleanValue());
    }

    /**
     * There is one document, not one per attribute: a read of {@code rack} returns the weights stored for
     * {@code zone}. Verified, and reproduced deliberately.
     */
    @Test
    void aReadIgnoresItsOwnAttributeOnceItHasBeenValidated() throws Exception {
        awarenessAttributes("zone,rack");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");

        assertEquals("1.0", body(get("rack")).get("weights").get("a").textValue());
    }

    /** Likewise a write for a second attribute replaces the first rather than joining it. */
    @Test
    void writingASecondAttributeReplacesTheFirst() throws Exception {
        awarenessAttributes("zone,rack");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");

        RestResponse replaced = dispatch("PUT", "/_cluster/routing/awareness/rack/weights",
            "{\"weights\":{\"r1\":\"1\"},\"_version\":0}");
        assertEquals(RestStatus.OK, replaced.status());

        JsonNode weights = body(get()).get("weights");
        assertEquals(1, weights.size());
        assertEquals("1.0", weights.get("r1").textValue());
    }

    // ------------------------------------------------------------------
    // the version lifecycle
    // ------------------------------------------------------------------

    /** A first write claims to be replacing -1, and the stored version becomes 0. */
    @Test
    void theFirstWriteReplacesTheInitialVersion() throws Exception {
        awarenessAttributes("zone");

        assertTrue(body(put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}")).get("acknowledged").booleanValue());
        assertEquals(0, body(get()).get("_version").longValue());
    }

    @Test
    void aStaleVersionIsAConflict() throws Exception {
        awarenessAttributes("zone");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");

        RestResponse response = put("{\"weights\":{\"a\":\"2\"},\"_version\":-1}");
        assertEquals(RestStatus.CONFLICT, response.status());
        assertEquals("unsupported_weighted_routing_state_exception", typeOf(response));
        // Trailing space included: the server's format string ends with one.
        assertEquals("requested version is -1 but cluster weighted routing metadata is at a different version 0 ",
            reasonOf(response));
    }

    /** Same weights, correct version: acknowledged, and the version deliberately does not move. */
    @Test
    void rewritingTheSameWeightsDoesNotBumpTheVersion() throws Exception {
        awarenessAttributes("zone");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");

        assertEquals(RestStatus.OK, put("{\"weights\":{\"a\":\"1\"},\"_version\":0}").status());
        assertEquals(0, body(get()).get("_version").longValue());
    }

    @Test
    void changingTheWeightsBumpsTheVersion() throws Exception {
        awarenessAttributes("zone");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");

        assertEquals(RestStatus.OK, put("{\"weights\":{\"a\":\"2\"},\"_version\":0}").status());
        assertEquals(1, body(get()).get("_version").longValue());
    }

    // ------------------------------------------------------------------
    // parsing a put
    // ------------------------------------------------------------------

    /** Considered divergence: OpenSearch dereferences a null request object and answers 500. */
    @Test
    void aPutWithNoBodyIsABadRequestRatherThanACrash() throws Exception {
        awarenessAttributes("zone");

        RestResponse response = put(null);
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("parse_exception", typeOf(response));
        assertEquals("request body is required", reasonOf(response));
    }

    @Test
    void anEmptyBodyObjectIsRejected() throws Exception {
        awarenessAttributes("zone");

        assertEquals("Empty request body", reasonOf(put("{}")));
    }

    /** Weights are strings on the wire; a JSON number is a parse failure. */
    @Test
    void aNumericWeightIsAParseFailure() throws Exception {
        awarenessAttributes("zone");

        RestResponse response = put("{\"weights\":{\"a\":1},\"_version\":-1}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("parse_exception", typeOf(response));
        assertEquals("failed to parse weighted routing request attribute [null], unknown type",
            reasonOf(response));
    }

    /**
     * The message names the <em>previously</em> parsed weight, because the server's parser keeps that
     * variable outside its loop. Faithfully odd, and the reason the case above reports {@code [null]}.
     */
    @Test
    void aNumericWeightAfterAGoodOneNamesTheGoodOne() throws Exception {
        awarenessAttributes("zone");

        assertEquals("failed to parse weighted routing request attribute [1.0], unknown type",
            reasonOf(put("{\"weights\":{\"a\":\"1\",\"b\":2},\"_version\":-1}")));
    }

    /** _version is a number here and a string on delete; see the class javadoc for why. */
    @Test
    void aStringVersionIsAParseFailureOnPut() throws Exception {
        awarenessAttributes("zone");

        assertEquals("failed to parse weighted routing request [zone], unknown type",
            reasonOf(put("{\"weights\":{\"a\":\"1\"},\"_version\":\"1\"}")));
    }

    @Test
    void anUnknownFieldIsAParseFailure() throws Exception {
        awarenessAttributes("zone");

        assertEquals("failed to parse weighted routing request object [foo]",
            reasonOf(put("{\"foo\":\"1\",\"_version\":-1}")));
    }

    /** Two spaces after "object", which the server's format string puts there. */
    @Test
    void weightsMustBeAnObject() throws Exception {
        awarenessAttributes("zone");

        assertEquals("failed to parse weighted routing request object  [weights], expected object",
            reasonOf(put("{\"weights\":\"nope\",\"_version\":-1}")));
    }

    /** The JDK's own message, unwrapped, because the server lets the exception escape its parser. */
    @Test
    void aNonNumericWeightEscapesAsANumberFormatError() throws Exception {
        awarenessAttributes("zone");

        RestResponse response = put("{\"weights\":{\"a\":\"abc\"},\"_version\":-1}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("number_format_exception", typeOf(response));
        assertEquals("For input string: \"abc\"", reasonOf(response));
    }

    // ------------------------------------------------------------------
    // validating a put
    // ------------------------------------------------------------------

    /** Validation accumulates, one-based, each terminated by a semicolon and no space between entries. */
    @Test
    void missingWeightsAndVersionAreReportedTogether() throws Exception {
        awarenessAttributes("zone");

        assertEquals("Validation Failed: 1: Weights are missing;2: Version is missing;",
            reasonOf(put("{\"weights\":{}}")));
    }

    @Test
    void tooManyZeroWeightsAreRejected() throws Exception {
        awarenessAttributes("zone");

        RestResponse response = put("{\"weights\":{\"a\":\"0\",\"b\":\"0\",\"c\":\"1\"},\"_version\":-1}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertEquals("Validation Failed: 1: There are too many attribute values [{a=0.0, b=0.0, c=1.0}] "
            + "given zero weight [2]. Maximum expected number of routing weights having zero weight is [1];",
            reasonOf(response));
    }

    /** Half may be zero; the check is strictly greater than half. */
    @Test
    void halfTheWeightsMayBeZero() {
        awarenessAttributes("zone");

        assertEquals(RestStatus.OK,
            put("{\"weights\":{\"a\":\"0\",\"b\":\"1\"},\"_version\":-1}").status());
    }

    // ------------------------------------------------------------------
    // forced awareness values
    // ------------------------------------------------------------------

    @Test
    void everyForcedValueMustBeGivenAWeight() throws Exception {
        awarenessAttributes("zone");
        forcedValues("zone", "z1,z2,z3");

        RestResponse response = put("{\"weights\":{\"z1\":\"1\",\"z2\":\"1\"},\"_version\":-1}");
        assertEquals(RestStatus.CONFLICT, response.status());
        assertEquals("unsupported_weighted_routing_state_exception", typeOf(response));
        assertEquals("weight for [z3] is not set and it is part of forced awareness value "
            + "or a routing node has this attribute.", reasonOf(response));
    }

    @Test
    void coveringEveryForcedValueIsAccepted() {
        awarenessAttributes("zone");
        forcedValues("zone", "z1,z2,z3");

        assertEquals(RestStatus.OK,
            put("{\"weights\":{\"z1\":\"1\",\"z2\":\"1\",\"z3\":\"0\"},\"_version\":-1}").status());
    }

    /**
     * The cluster-level zero check counts only forced values, so extra values in the request can carry a
     * request-level majority past the first check and still fail the second. Note the wording differs by
     * one word from the request-level message for the same condition.
     */
    @Test
    void tooManyForcedValuesWithZeroWeightAreRejectedSeparately() throws Exception {
        awarenessAttributes("zone");
        forcedValues("zone", "z1,z2,z3");

        RestResponse response = put(
            "{\"weights\":{\"z1\":\"0\",\"z2\":\"0\",\"z3\":\"1\",\"z4\":\"1\",\"z5\":\"1\"},\"_version\":-1}");
        assertEquals(RestStatus.BAD_REQUEST, response.status());
        assertTrue(reasonOf(response).startsWith("Validation Failed: 1: There are too many discovered "
            + "attribute values ["), reasonOf(response));
        assertTrue(reasonOf(response).endsWith("given zero weight [2]. Maximum expected number of routing "
            + "weights having zero weight is [1];"), reasonOf(response));
    }

    /** With nothing forced there is nothing this service can check, so any value set is accepted. */
    @Test
    void withNoForcedValuesAnyValueSetIsAccepted() {
        awarenessAttributes("zone");

        assertEquals(RestStatus.OK,
            put("{\"weights\":{\"anything\":\"1\"},\"_version\":-1}").status());
    }

    // ------------------------------------------------------------------
    // deleting
    // ------------------------------------------------------------------

    /**
     * With nothing stored the version compared against is -1, so a bodiless delete conflicts -2 against
     * -1 rather than reporting nothing to delete. Verified on a pristine cluster.
     */
    @Test
    void aBodilessDeleteOnAPristineClusterIsAVersionConflict() throws Exception {
        RestResponse response = delete(null);

        assertEquals(RestStatus.CONFLICT, response.status());
        assertEquals("requested version is -2 but cluster weighted routing metadata is at a different version -1 ",
            reasonOf(response));
    }

    /** A pristine cluster with the right version reports the resource, not a conflict. */
    @Test
    void deletingFromAPristineClusterIsNotFound() throws Exception {
        RestResponse response = delete("{\"_version\":\"-1\"}");

        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals("resource_not_found_exception", typeOf(response));
        assertEquals("weighted routing metadata does not have weights set for awareness attribute null",
            reasonOf(response));
    }

    /** mapStrings() coerces before parsing, so a JSON number is accepted here where a put rejects one. */
    @Test
    void deleteAcceptsTheVersionAsANumberOrAString() {
        awarenessAttributes("zone");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");

        assertEquals(RestStatus.OK, delete("{\"_version\":0}").status());

        put("{\"weights\":{\"a\":\"1\"},\"_version\":1}");
        assertEquals(RestStatus.OK, delete("{\"_version\":\"2\"}").status());
    }

    /** Emptied, not removed: the document stays, with no weights and a bumped version. */
    @Test
    void deleteEmptiesTheDocumentRatherThanRemovingIt() throws Exception {
        awarenessAttributes("zone");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");
        assertEquals(RestStatus.OK, delete("{\"_version\":\"0\"}").status());

        JsonNode read = body(get());
        assertEquals(0, read.get("weights").size());
        assertEquals(1, read.get("_version").longValue());
        assertTrue(read.get("discovered_cluster_manager").booleanValue());
    }

    /** So it is not idempotent in the version: a second delete needs the version the first produced. */
    @Test
    void deletingAgainBumpsTheVersionAgain() throws Exception {
        awarenessAttributes("zone");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");
        delete("{\"_version\":\"0\"}");

        assertEquals(RestStatus.OK, delete("{\"_version\":\"1\"}").status());
        assertEquals(2, body(get()).get("_version").longValue());
    }

    @Test
    void deleteRejectsAnUnknownBodyField() throws Exception {
        assertEquals("failed to parse delete weighted routing request body [foo], unknown type",
            reasonOf(delete("{\"foo\":\"1\"}")));
    }

    @Test
    void deleteRejectsAnEmptyBodyObject() throws Exception {
        assertEquals("Empty request body", reasonOf(delete("{}")));
    }

    /**
     * Unlike get and put, delete never checks the attribute against the configured awareness attributes —
     * an unconfigured name gives 404 rather than the 400 the others give.
     *
     * <p>Reached by calling the handler directly because the API specification is missing the
     * {@code DELETE /_cluster/routing/awareness/{attribute}/weights} route that OpenSearch registers, so
     * no generated route binds the attribute yet. The handler is written for it regardless.
     */
    @Test
    void deleteMatchesTheStoredAttributeWithoutValidatingIt() throws Exception {
        awarenessAttributes("zone");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");

        RestResponse response = deleteAttribute("bogus", "{\"_version\":\"0\"}");
        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertEquals("weighted routing metadata does not have weights set for awareness attribute bogus",
            reasonOf(response));

        assertEquals(RestStatus.OK, deleteAttribute("zone", "{\"_version\":\"0\"}").status());
    }

    /** The stored attribute is emptied along with the weights, so a later delete cannot name it. */
    @Test
    void anEmptiedDocumentNoLongerMatchesItsOldAttribute() throws Exception {
        awarenessAttributes("zone");
        put("{\"weights\":{\"a\":\"1\"},\"_version\":-1}");
        delete("{\"_version\":\"0\"}");

        RestResponse response = deleteAttribute("zone", "{\"_version\":\"1\"}");
        assertEquals(RestStatus.NOT_FOUND, response.status());
        assertFalse(body(response).get("error").get("reason").asText().isEmpty());
    }

    private RestResponse deleteAttribute(String attribute, String body) throws Exception {
        String path = "/_cluster/routing/awareness/" + attribute + "/weights";
        RestRequest request = new RestRequest("DELETE", path, path, Map.of(),
            Map.of("attribute", attribute), Map.of(), body.getBytes(StandardCharsets.UTF_8),
            "cluster.delete_weighted_routing");
        // Through the same factory the router uses, so the handler sees the tenant-confined store rather
        // than the root one and reads the document the other requests in this test wrote.
        OperationContext context = TestContexts.factory(store).create(Map.of()).orElseThrow();
        return new WeightedRoutingHandlers.Delete().handle(request, context);
    }

    // ------------------------------------------------------------------
    // list settings
    // ------------------------------------------------------------------

    /** A list setting written as a JSON array works as well as the comma-separated spelling. */
    @Test
    void forcedValuesMayBeWrittenAsAJsonArray() throws Exception {
        awarenessAttributes("zone");
        dispatch("PUT", "/_cluster/settings",
            "{\"persistent\":{\"cluster.routing.allocation.awareness.force.zone.values\":[\"z1\",\"z2\"]}}");

        assertEquals("weight for [z2] is not set and it is part of forced awareness value "
            + "or a routing node has this attribute.",
            reasonOf(put("{\"weights\":{\"z1\":\"1\"},\"_version\":-1}")));
    }

    /**
     * The version counter is per tenant, as the document is. Two tenants each start from -1, and one
     * writing does not move the other's — which matters because the version is a client-visible token a
     * tenant is expected to round-trip.
     */
    @Test
    void theVersionCounterIsPerTenant() throws Exception {
        String header = HeaderTenantResolver.DEFAULT_HEADER;
        Router multiTenant = Router.fromGeneratedRoutes(
            ControlPlaneHandlers.build(), TestContexts.factory(store, new HeaderTenantResolver()));

        for (String tenant : List.of("acme", "globex")) {
            multiTenant.dispatch("PUT", "/_cluster/settings", "/_cluster/settings", Map.of(),
                Map.of(header, tenant),
                "{\"persistent\":{\"cluster.routing.allocation.awareness.attributes\":\"zone\"}}"
                    .getBytes(StandardCharsets.UTF_8));
        }

        // acme writes twice, so its version reaches 1 while globex has never written at all.
        for (long version : List.of(-1L, 0L)) {
            RestResponse written = multiTenant.dispatch("PUT", WEIGHTS_PATH, WEIGHTS_PATH, Map.of(),
                Map.of(header, "acme"),
                ("{\"weights\":{\"a\":\"" + (version + 2) + "\"},\"_version\":" + version + "}")
                    .getBytes(StandardCharsets.UTF_8));
            assertEquals(RestStatus.OK, written.status());
        }

        RestResponse globex = multiTenant.dispatch("GET", WEIGHTS_PATH, WEIGHTS_PATH, Map.of(),
            Map.of(header, "globex"), new byte[0]);
        assertEquals(0, body(globex).size());

        RestResponse acme = multiTenant.dispatch("GET", WEIGHTS_PATH, WEIGHTS_PATH, Map.of(),
            Map.of(header, "acme"), new byte[0]);
        assertEquals(1, body(acme).get("_version").longValue());
    }
}
