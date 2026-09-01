/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.tenant;

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
 * End-to-end tenant isolation through the real router and the real pipeline handlers.
 *
 * <p>This is the proof that the scoping is structural. The pipeline handlers contain no tenancy
 * logic whatsoever — no tenant ID in a key, no filtering, nothing to forget — yet two tenants using
 * identical pipeline IDs cannot observe each other.
 */
class TenantIsolationTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String HEADER = HeaderTenantResolver.DEFAULT_HEADER;

    private InMemoryKeyValueStore root;
    private Router router;

    @BeforeEach
    void setUp() {
        root = new InMemoryKeyValueStore();
        router = Router.fromGeneratedRoutes(
            ControlPlaneHandlers.registerAll(HandlerRegistry.withAllStubbed()),
            TestContexts.factory(root, new HeaderTenantResolver())
        );
    }

    private RestResponse as(String tenant, String method, String path, String body) {
        return router.dispatch(
            method,
            path,
            path,
            Map.of(),
            tenant == null ? Map.of() : Map.of(HEADER, tenant),
            body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8)
        );
    }

    private JsonNode body(RestResponse response) throws Exception {
        return MAPPER.readTree(new String(response.body(), StandardCharsets.UTF_8));
    }

    @Test
    void twoTenantsCanUseTheSamePipelineIdIndependently() throws Exception {
        assertEquals(
            RestStatus.OK,
            as("acme", "PUT", "/_ingest/pipeline/shared", "{\"description\":\"acme\",\"processors\":[]}").status()
        );
        assertEquals(
            RestStatus.OK,
            as("globex", "PUT", "/_ingest/pipeline/shared", "{\"description\":\"globex\",\"processors\":[]}").status()
        );

        assertEquals("acme", body(as("acme", "GET", "/_ingest/pipeline/shared", null)).get("shared").get("description").asText());
        assertEquals("globex", body(as("globex", "GET", "/_ingest/pipeline/shared", null)).get("shared").get("description").asText());
    }

    @Test
    void oneTenantsPipelinesAreInvisibleToAnother() throws Exception {
        as("acme", "PUT", "/_ingest/pipeline/acme-only", "{\"processors\":[]}");

        // A full listing for the other tenant must not see it -- and with nothing of its own, that
        // is the verified 404-on-empty.
        RestResponse listing = as("globex", "GET", "/_ingest/pipeline", null);
        assertEquals(RestStatus.NOT_FOUND, listing.status());
        assertEquals(0, body(listing).size());

        assertEquals(RestStatus.NOT_FOUND, as("globex", "GET", "/_ingest/pipeline/acme-only", null).status());
    }

    @Test
    void wildcardListingIsConfinedToTheRequestingTenant() throws Exception {
        as("acme", "PUT", "/_ingest/pipeline/logs-a", "{\"processors\":[]}");
        as("acme", "PUT", "/_ingest/pipeline/logs-b", "{\"processors\":[]}");
        as("globex", "PUT", "/_ingest/pipeline/logs-c", "{\"processors\":[]}");

        JsonNode acmeLogs = body(as("acme", "GET", "/_ingest/pipeline/logs-*", null));
        assertEquals(2, acmeLogs.size());
        assertTrue(acmeLogs.has("logs-a") && acmeLogs.has("logs-b"));
        assertFalse(acmeLogs.has("logs-c"), "wildcards must not cross the tenant boundary");
    }

    @Test
    void oneTenantCannotDeleteAnothersPipeline() {
        as("acme", "PUT", "/_ingest/pipeline/acme-only", "{\"processors\":[]}");

        // globex has never written anything, so this is the pristine-store no-op 200 -- and
        // crucially it must not remove acme's pipeline.
        assertEquals(RestStatus.OK, as("globex", "DELETE", "/_ingest/pipeline/acme-only", null).status());
        assertEquals(RestStatus.OK, as("acme", "GET", "/_ingest/pipeline/acme-only", null).status());
    }

    @Test
    void deleteAllIsConfinedToTheRequestingTenant() {
        as("acme", "PUT", "/_ingest/pipeline/p", "{\"processors\":[]}");
        as("globex", "PUT", "/_ingest/pipeline/p", "{\"processors\":[]}");

        assertEquals(RestStatus.OK, as("acme", "DELETE", "/_ingest/pipeline/*", null).status());

        assertEquals(RestStatus.NOT_FOUND, as("acme", "GET", "/_ingest/pipeline/p", null).status());
        assertEquals(RestStatus.OK, as("globex", "GET", "/_ingest/pipeline/p", null).status());
    }

    @Test
    void ingestMetadataMarkerIsPerTenant() {
        // acme has metadata, globex does not, so the same delete gets different answers.
        as("acme", "PUT", "/_ingest/pipeline/p", "{\"processors\":[]}");
        as("acme", "DELETE", "/_ingest/pipeline/p", null);

        assertEquals(RestStatus.NOT_FOUND, as("acme", "DELETE", "/_ingest/pipeline/missing", null).status());
        assertEquals(RestStatus.OK, as("globex", "DELETE", "/_ingest/pipeline/missing", null).status());
    }

    @Test
    void keysAreLaidOutUnderTheTenantPrefix() {
        as("acme", "PUT", "/_ingest/pipeline/p", "{\"processors\":[]}");

        assertTrue(root.get("tenant/acme/ingest/pipeline/p").isPresent());
        assertEquals(
            List.of("tenant/acme/ingest/metadata-initialized", "tenant/acme/ingest/pipeline/p"),
            List.copyOf(root.scanPrefix("").keySet())
        );
    }

    // ------------------------------------------------------------------
    // resolution failures
    // ------------------------------------------------------------------

    @Test
    void aRequestWithNoTenantIsRejectedBeforeTheHandlerRuns() throws Exception {
        RestResponse response = as(null, "PUT", "/_ingest/pipeline/p", "{\"processors\":[]}");

        assertEquals(RestStatus.UNAUTHORIZED, response.status());
        assertEquals("security_exception", body(response).get("error").get("type").asText());
        assertTrue(root.scanPrefix("").isEmpty(), "nothing should have been written");
    }

    @Test
    void aMalformedTenantIdIsRejectedRatherThanSanitised() {
        // Refusing beats sanitising: a value with a slash is indistinguishable from an attempt to
        // escape the key prefix.
        assertEquals(RestStatus.UNAUTHORIZED, as("acme/../globex", "GET", "/_ingest/pipeline", null).status());
        assertEquals(RestStatus.UNAUTHORIZED, as("", "GET", "/_ingest/pipeline", null).status());
    }

    @Test
    void unresolvedTenantStillShortCircuitsAStubbedOperation() {
        // Tenant resolution happens before dispatch, so it beats the 501 too.
        assertEquals(RestStatus.UNAUTHORIZED, as(null, "PUT", "/my-index", "{}").status());
        assertEquals(RestStatus.NOT_IMPLEMENTED, as("acme", "PUT", "/my-index", "{}").status());
    }

    @Test
    void restrictedResolverRejectsTenantsOutsideThePermittedSet() {
        Router restricted = Router.fromGeneratedRoutes(
            ControlPlaneHandlers.registerAll(HandlerRegistry.withAllStubbed()),
            TestContexts.factory(root, HeaderTenantResolver.restrictedTo(HEADER, java.util.Set.of(TenantId.of("acme"))))
        );

        assertEquals(
            RestStatus.NOT_FOUND,
            restricted.dispatch("GET", "/_ingest/pipeline", "/_ingest/pipeline", Map.of(), Map.of(HEADER, "acme"), new byte[0]).status()
        );
        assertEquals(
            RestStatus.UNAUTHORIZED,
            restricted.dispatch("GET", "/_ingest/pipeline", "/_ingest/pipeline", Map.of(), Map.of(HEADER, "globex"), new byte[0]).status()
        );
    }
}
