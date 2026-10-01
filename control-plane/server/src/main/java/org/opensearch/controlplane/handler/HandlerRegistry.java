/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler;

import org.opensearch.controlplane.router.GeneratedRoutes;
import org.opensearch.controlplane.router.Route;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Maps operation groups to handlers, defaulting every group in the generated
 * route table to {@link NotImplementedHandler}.
 *
 * <p>Registering a handler for a group not present in the generated table is an
 * error: it means the scope file and the handler disagree, which would otherwise
 * produce a handler that can never be reached.
 */
public final class HandlerRegistry {

    private final Map<String, OperationHandler> handlers = new TreeMap<>();
    private final Set<String> knownGroups;
    private final Set<String> implementedGroups = new TreeSet<>();

    /** Builds a registry covering every operation group in the generated route table. */
    public static HandlerRegistry withAllStubbed() {
        return new HandlerRegistry(GeneratedRoutes.routes());
    }

    public HandlerRegistry(Collection<Route> routes) {
        this.knownGroups = routes.stream()
            .map(Route::operationGroup)
            .collect(Collectors.toCollection(TreeSet::new));
        for (String group : knownGroups) {
            handlers.put(group, new NotImplementedHandler(group));
        }
    }

    /** Replaces the stub for this handler's operation group. */
    public HandlerRegistry register(OperationHandler handler) {
        String group = handler.operationGroup();
        if (knownGroups.contains(group) == false) {
            throw new IllegalArgumentException(
                "handler registered for operation group [" + group + "], which is not in the generated route table. "
                    + "Either the group name is wrong or spec/control-plane-scope.yaml does not include it."
            );
        }
        handlers.put(group, handler);
        implementedGroups.add(group);
        return this;
    }

    public OperationHandler handlerFor(String operationGroup) {
        OperationHandler handler = handlers.get(operationGroup);
        if (handler == null) {
            // Unreachable via the router, which only dispatches groups from the
            // generated table, but fail loudly rather than NPE if it happens.
            throw new IllegalStateException("no handler for operation group [" + operationGroup + "]");
        }
        return handler;
    }

    /** Operation groups with a real handler. */
    public Set<String> implemented() {
        return Set.copyOf(implementedGroups);
    }

    /** Operation groups still stubbed out. */
    public Set<String> notImplemented() {
        Set<String> stubbed = new TreeSet<>(knownGroups);
        stubbed.removeAll(implementedGroups);
        return stubbed;
    }

    public Map<String, OperationHandler> handlers() {
        return new LinkedHashMap<>(handlers);
    }

    public int size() {
        return handlers.size();
    }
}
