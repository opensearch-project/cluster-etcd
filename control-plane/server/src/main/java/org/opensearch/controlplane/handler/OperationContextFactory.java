/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.TenantScopedKeyValueStore;
import org.opensearch.controlplane.tenant.TenantResolver;

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

    public OperationContextFactory(KeyValueStore rootStore, TenantResolver tenantResolver) {
        this.rootStore = Objects.requireNonNull(rootStore, "rootStore");
        this.tenantResolver = Objects.requireNonNull(tenantResolver, "tenantResolver");
    }

    /**
     * @param headers request headers, keys lower-cased
     * @return the context, or empty when no tenant could be resolved, which the router turns into a
     *         401
     */
    public Optional<OperationContext> create(Map<String, String> headers) {
        return tenantResolver.resolve(headers)
            .map(tenant -> new OperationContext(tenant, new TenantScopedKeyValueStore(rootStore, tenant)));
    }
}
