/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.tenant;

import java.util.Map;
import java.util.Optional;

/**
 * Resolves every request to one fixed tenant.
 *
 * <p>For single-tenant deployments and local development. Because all data still lands under a
 * tenant prefix, moving to real multi-tenancy later is a resolver swap rather than a data migration
 * — which is the reason to scope keys by tenant even when there is only one.
 */
public final class SingleTenantResolver implements TenantResolver {

    /** The tenant used when none is configured. */
    public static final TenantId DEFAULT_TENANT = TenantId.of("default");

    private final Optional<TenantId> tenant;

    public SingleTenantResolver() {
        this(DEFAULT_TENANT);
    }

    public SingleTenantResolver(TenantId tenant) {
        this.tenant = Optional.of(tenant);
    }

    @Override
    public Optional<TenantId> resolve(Map<String, String> headers) {
        return tenant;
    }
}
