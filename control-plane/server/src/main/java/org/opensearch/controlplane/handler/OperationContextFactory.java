/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RegisteringKeyValueStore;
import org.opensearch.controlplane.store.TenantScopedKeyValueStore;
import org.opensearch.controlplane.tenant.TenantRegistry;
import org.opensearch.controlplane.tenant.TenantResolver;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Turns a request's headers into an {@link OperationContext}, resolving the tenant and confining the
 * store to it.
 *
 * <p>Single-tenant and multi-tenant deployments differ only in which {@link TenantResolver} is
 * supplied here. Nothing downstream changes.
 */
public final class OperationContextFactory {

    private final KeyValueStore rootStore;
    private final TenantResolver tenantResolver;
    private final Clock clock;

    /**
     * Records tenants at the root of the store so the supervisor can enumerate them.
     *
     * <p>Here because this is the only place holding both the <b>root</b> store and a resolved tenant. A
     * handler could not do it if it wanted to: it is handed a {@link TenantScopedKeyValueStore}, for which a
     * root key is not merely undiscovered but unreachable.
     */
    private final TenantRegistry tenantRegistry;

    public OperationContextFactory(KeyValueStore rootStore, TenantResolver tenantResolver) {
        this(rootStore, tenantResolver, Clock.systemUTC());
    }

    public OperationContextFactory(KeyValueStore rootStore, TenantResolver tenantResolver, Clock clock) {
        this.rootStore = Objects.requireNonNull(rootStore, "rootStore");
        this.tenantResolver = Objects.requireNonNull(tenantResolver, "tenantResolver");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.tenantRegistry = new TenantRegistry(rootStore, clock);
    }

    /**
     * @param headers request headers, keys lower-cased
     * @return the context, or empty when no tenant could be resolved, which the router turns into a
     *         401
     */
    public Optional<OperationContext> create(Map<String, String> headers) {
        return tenantResolver.resolve(headers).map(tenant -> new OperationContext(
            tenant,
            // Two decorators, composed rather than merged. The inner one confines the handler to its
            // tenant; the outer one notices the tenant's first write and registers it at the root. Keeping
            // them separate is what lets the tenant view stay a pure view -- see RegisteringKeyValueStore.
            new RegisteringKeyValueStore(
                new TenantScopedKeyValueStore(rootStore, tenant),
                () -> tenantRegistry.ensureRegistered(tenant)
            ),
            clock
        ));
    }
}
