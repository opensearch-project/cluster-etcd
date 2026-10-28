/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.tenant;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Reads the tenant from a request header.
 *
 * <p><b>This is client-asserted and therefore not sufficient on its own.</b> Any caller able to set
 * the header can name any tenant, so used alone this provides no isolation whatsoever — it merely
 * partitions the key space for well-behaved clients. It is included because it is genuinely useful
 * in two situations:
 *
 * <ul>
 *   <li>Behind a trusted proxy that authenticates the caller and <em>overwrites</em> the header, so
 *       clients cannot influence it.</li>
 *   <li>Constrained to a permitted set via {@link #restrictedTo}, which is the shape of an internal
 *       admin or migration tool acting on behalf of a known list of tenants.</li>
 * </ul>
 *
 * <p>For anything client-facing, resolve the tenant from the authenticated principal instead. See
 * {@link TenantResolver} for the options.
 */
public final class HeaderTenantResolver implements TenantResolver {

    /** Conventional header name. Not an OpenSearch header — nothing in the spec knows about it. */
    public static final String DEFAULT_HEADER = "x-tenant-id";

    private final String headerName;
    private final Set<TenantId> permitted;

    public HeaderTenantResolver() {
        this(DEFAULT_HEADER, Set.of());
    }

    public HeaderTenantResolver(String headerName) {
        this(headerName, Set.of());
    }

    private HeaderTenantResolver(String headerName, Set<TenantId> permitted) {
        this.headerName = headerName.toLowerCase(Locale.ROOT);
        this.permitted = Set.copyOf(permitted);
    }

    /**
     * A resolver that accepts the header only when it names one of {@code permitted}.
     *
     * <p>Still client-asserted within that set, so it bounds the blast radius rather than removing
     * it.
     */
    public static HeaderTenantResolver restrictedTo(String headerName, Set<TenantId> permitted) {
        if (permitted.isEmpty()) {
            throw new IllegalArgumentException("permitted tenant set must not be empty");
        }
        return new HeaderTenantResolver(headerName, permitted);
    }

    @Override
    public Optional<TenantId> resolve(Map<String, String> headers) {
        String raw = headers.get(headerName);
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }

        TenantId tenant;
        try {
            tenant = TenantId.of(raw.trim());
        } catch (IllegalArgumentException e) {
            // A malformed value is indistinguishable from an attempt to escape the key prefix.
            // Refuse rather than sanitise.
            return Optional.empty();
        }

        if (permitted.isEmpty() == false && permitted.contains(tenant) == false) {
            return Optional.empty();
        }
        return Optional.of(tenant);
    }
}
