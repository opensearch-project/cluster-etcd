/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane;

import org.opensearch.controlplane.handler.HandlerRegistry;
import org.opensearch.controlplane.handler.OperationContextFactory;
import org.opensearch.controlplane.http.NettyHttpServer;
import org.opensearch.controlplane.router.GeneratedRoutes;
import org.opensearch.controlplane.router.Route;
import org.opensearch.controlplane.router.Router;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Wires the generated route table to a handler registry and an HTTP front end.
 *
 * <p>Every operation group starts out stubbed with a 501. Implementing an
 * operation means calling {@link HandlerRegistry#register} with a real handler;
 * nothing about the routing changes.
 */
public final class ControlPlaneServer implements AutoCloseable {

    private final HandlerRegistry registry;
    private final Router router;
    private final NettyHttpServer httpServer;

    /**
     * @param registry       which operations are implemented
     * @param contextFactory resolves the tenant and hands handlers a store confined to it
     */
    public ControlPlaneServer(String host, int port, HandlerRegistry registry, OperationContextFactory contextFactory) {
        this.registry = registry;
        this.router = Router.fromGeneratedRoutes(registry, contextFactory);
        this.httpServer = new NettyHttpServer(host, port, router::dispatch);
    }

    public void start() throws InterruptedException {
        httpServer.start();
    }

    public int boundPort() {
        return httpServer.boundPort();
    }

    public void awaitShutdown() throws InterruptedException {
        httpServer.awaitShutdown();
    }

    public Router router() {
        return router;
    }

    public HandlerRegistry registry() {
        return registry;
    }

    @Override
    public void close() {
        httpServer.close();
    }

    /** A one-line summary of the served surface, logged at startup. */
    public String describeSurface() {
        List<Route> routes = GeneratedRoutes.routes();
        return String.format(
            Locale.ROOT,
            "%d routes, %d operation groups (%d implemented, %d stubbed) from spec %s",
            routes.size(),
            registry.size(),
            registry.implemented().size(),
            registry.notImplemented().size(),
            GeneratedRoutes.SPEC_VERSION
        );
    }

    /** The full route table grouped by namespace, for {@code --dump-routes}. */
    public String dumpRoutes() {
        Map<String, List<Route>> byNamespace = GeneratedRoutes.routes().stream()
            .collect(Collectors.groupingBy(
                route -> {
                    int dot = route.operationGroup().indexOf('.');
                    return dot < 0 ? "_core" : route.operationGroup().substring(0, dot);
                },
                TreeMap::new,
                Collectors.toList()
            ));

        StringBuilder out = new StringBuilder();
        for (Map.Entry<String, List<Route>> entry : byNamespace.entrySet()) {
            out.append(entry.getKey()).append(" (").append(entry.getValue().size()).append(" routes)\n");
            entry.getValue().stream()
                .sorted((a, b) -> {
                    int byGroup = a.operationGroup().compareTo(b.operationGroup());
                    return byGroup != 0 ? byGroup : a.path().compareTo(b.path());
                })
                .forEach(route -> {
                    boolean implemented = registry.implemented().contains(route.operationGroup());
                    out.append(String.format(
                        Locale.ROOT,
                        "  %-4s %-6s %-52s %s%s%n",
                        implemented ? "[ok]" : "[501]",
                        route.method(),
                        route.path(),
                        route.operationGroup(),
                        route.deprecated() ? " (deprecated)" : ""
                    ));
                    if (route.queryParams().isEmpty() == false) {
                        out.append("         params: ")
                           .append(String.join(", ", route.queryParams()))
                           .append('\n');
                    }
                });
        }
        return out.toString();
    }
}
