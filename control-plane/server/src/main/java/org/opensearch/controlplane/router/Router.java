/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.router;

import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationContextFactory;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Resolves a request against the generated route table and dispatches it to a
 * handler, reproducing OpenSearch's pre-dispatch failure behaviour.
 *
 * <p>Tenant resolution is a pre-dispatch concern like routing: it happens here, once, and the
 * handler receives a store already confined to that tenant.
 */
public final class Router {

    private final PathTrie trie = new PathTrie();
    private final HandlerRegistry registry;
    private final OperationContextFactory contextFactory;

    public Router(HandlerRegistry registry, List<Route> routes, OperationContextFactory contextFactory) {
        this.registry = registry;
        this.contextFactory = contextFactory;
        for (Route route : routes) {
            trie.insert(route);
        }
    }

    /** Builds a router over the full generated surface with the given handlers. */
    public static Router fromGeneratedRoutes(HandlerRegistry registry, OperationContextFactory contextFactory) {
        return new Router(registry, GeneratedRoutes.routes(), contextFactory);
    }

    /**
     * Dispatches a request.
     *
     * @param method      uppercase HTTP method
     * @param rawPath     path with query string stripped, still percent-encoded
     * @param uri         full request target, used verbatim in error messages
     * @param queryParams decoded query string
     * @param headers     lowercase-keyed request headers
     * @param body        request body, possibly empty
     */
    public RestResponse dispatch(
        String method,
        String rawPath,
        String uri,
        Map<String, List<String>> queryParams,
        Map<String, String> headers,
        byte[] body
    ) {
        boolean pretty = isPretty(queryParams);

        PathTrie.PathMatch pathMatch = trie.resolvePath(rawPath);
        if (pathMatch == null) {
            return ErrorResponses.noHandler(method, uri, pretty);
        }

        Route route = pathMatch.routesByMethod().get(method);
        if (route == null) {
            Set<String> allowed = new TreeSet<>(pathMatch.routesByMethod().keySet());
            return ErrorResponses.methodNotAllowed(method, uri, allowed, pretty);
        }

        // Resolved once, before the handler runs, so no handler can pick its own tenant.
        Optional<OperationContext> context = contextFactory.create(headers);
        if (context.isEmpty()) {
            return ErrorResponses.tenantUnresolved(pretty);
        }

        RestRequest request = new RestRequest(
            method,
            rawPath,
            uri,
            queryParams,
            pathMatch.pathParams(route),
            headers,
            body,
            route.operationGroup()
        );

        OperationHandler handler = registry.handlerFor(route.operationGroup());
        try {
            RestResponse response = handler.handle(request, context.get());
            // OpenSearch never sends a body for HEAD, only the status and
            // Content-Length of what a GET would have returned.
            if ("HEAD".equals(method) && response.body().length > 0) {
                return new RestResponse(response.status(), response.contentType(), new byte[0], response.headers());
            }
            return response;
        } catch (Exception e) {
            return ErrorResponses.internalError(e, pretty);
        }
    }

    /** Same {@code ?pretty} flag semantics as {@link RestRequest#paramAsBoolean}, needed before a request exists. */
    private static boolean isPretty(Map<String, List<String>> queryParams) {
        List<String> values = queryParams.get("pretty");
        if (values == null || values.isEmpty()) {
            return false;
        }
        String value = values.getFirst();
        return value == null || value.isEmpty() || Boolean.parseBoolean(value);
    }

    public PathTrie trie() {
        return trie;
    }

    public HandlerRegistry registry() {
        return registry;
    }

    /** Convenience for tests: dispatch with no query string, headers, or body. */
    public RestResponse dispatch(String method, String path) {
        return dispatch(method, path, path, Map.of(), Map.of(), new byte[0]);
    }
}
