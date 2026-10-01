/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.clusterless.tenant.TenantResolver;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.usage.UsageService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * What the plugin is allowed to assume when each of its callbacks runs, and what it has to declare for the rest to
 * work at all.
 */
public class ClusterlessPluginTests extends OpenSearchTestCase {

    public void testTheIndexNameResolverSurvivesBeingAskedForBeforeAnythingIsWired() {
        // ClusterModule is constructed at Node.<init>:830 and asks for the custom resolvers there. createComponents
        // -- the first callback handed a ThreadPool, an Environment or a ClusterService -- runs at line 1212. So a
        // resolver that captured any of those at construction threw NullPointerException out of a plugin callback
        // during bootstrap, and the whole node failed with it.
        ClusterlessPlugin plugin = new ClusterlessPlugin();

        Collection<IndexNameExpressionResolver.ExpressionResolver> resolvers = plugin.getIndexNameCustomResolvers();

        assertEquals(1, resolvers.size());
        IndexNameExpressionResolver.Context context = new IndexNameExpressionResolver.Context(
            ClusterState.builder(new ClusterName("clusterless")).build(),
            IndicesOptions.lenientExpandOpen(),
            false
        );
        // And it has to answer, not merely exist. With no tenant resolvable it rewrites nothing, which is the same
        // answer it gives a request that carries no tenant -- an unqualified name then fails to resolve rather than
        // resolving to another tenant's data.
        List<String> expressions = List.of("my-index", Metadata.ALL);
        assertEquals(expressions, resolvers.iterator().next().resolve(context, expressions));
    }

    public void testTheTenantHeaderReachesTheThreadContext() {
        // RestController copies into the thread context only the headers some plugin declared (ActionModule builds
        // the set from getRestHeaders), and drops the rest silently. Undeclared, fromHeader never saw a tenant and
        // fixed never saw a request asking for another -- and every test that put the header in a ThreadContext by
        // hand still passed.
        AtomicReference<String> seen = new AtomicReference<>();
        FakeRestChannel channel = dispatch(Map.of(TenantResolver.HEADER, List.of("acme")), seen);

        assertEquals(RestStatus.OK, channel.capturedResponse().status());
        assertEquals("acme", seen.get());
    }

    public void testTwoTenantsInOneRequestAreRefused() {
        // Single-valued, so that a request naming two tenants is not joined into "acme,globex" for a resolver to
        // make sense of: the server refuses it before any handler runs.
        AtomicReference<String> seen = new AtomicReference<>();
        FakeRestChannel channel = dispatch(Map.of(TenantResolver.HEADER, List.of("acme", "globex")), seen);

        assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
        assertNull(seen.get());
    }

    private FakeRestChannel dispatch(Map<String, List<String>> headers, AtomicReference<String> seen) {
        try (NoOpNodeClient client = new NoOpNodeClient(getTestName())) {
            return dispatch(headers, seen, client);
        }
    }

    private static FakeRestChannel dispatch(Map<String, List<String>> headers, AtomicReference<String> seen, NodeClient client) {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        RestController controller = new RestController(
            java.util.Set.copyOf(new ClusterlessPlugin().getRestHeaders()),
            null,
            client,
            new NoneCircuitBreakerService(),
            new UsageService()
        );
        controller.registerHandler(new RestHandler() {
            @Override
            public List<Route> routes() {
                return List.of(new Route(RestRequest.Method.GET, "/_tenant_probe"));
            }

            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                seen.set(threadContext.getHeader(TenantResolver.HEADER));
                channel.sendResponse(new org.opensearch.rest.BytesRestResponse(RestStatus.OK, ""));
            }
        });
        FakeRestRequest request = new FakeRestRequest.Builder(org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.GET)
            .withPath("/_tenant_probe")
            .withHeaders(headers)
            .build();
        FakeRestChannel channel = new FakeRestChannel(request, false, 1);
        controller.dispatchRequest(request, channel, threadContext);
        return channel;
    }
}
