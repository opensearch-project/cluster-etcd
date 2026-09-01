/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.tenant;

import java.util.Map;
import java.util.Optional;

/**
 * Determines which tenant a request belongs to.
 *
 * <h2>Tenant must be derived from identity, not asserted alongside it</h2>
 *
 * <p>The important property of any implementation is that a caller cannot <em>choose</em> its
 * tenant. If the tenant is read from a plain header that any client can set, then every
 * authenticated caller can address every other tenant's data, and isolation degrades to "every
 * handler remembers to check" — which is exactly the kind of invariant that fails silently.
 *
 * <p>Options, roughly in order of preference:
 *
 * <ul>
 *   <li><b>Derived from the authenticated principal.</b> The client sends credentials it already
 *       knows how to send — basic auth, a bearer token, SigV4 — and the control plane maps principal
 *       to tenant server-side. Vanilla OpenSearch clients support all of these, so there is no
 *       deviation from the spec and nothing extra for a generated client to know about.</li>
 *   <li><b>Derived from the endpoint.</b> Each tenant gets a hostname
 *       ({@code acme.control-plane.internal}) and the tenant comes from SNI or the {@code Host}
 *       header. Zero client change — a client already configures a base URL — plus network-level
 *       separation and the freedom to shard tenants across instances later. This is how managed
 *       OpenSearch presents per-domain endpoints.</li>
 *   <li><b>Derived from a client certificate</b> under mTLS. The transport would surface the
 *       certificate subject to this interface as a synthetic header.</li>
 *   <li><b>A plain header.</b> Acceptable only on a trusted network, or when validated against the
 *       authenticated principal's permitted set. See {@link HeaderTenantResolver}.</li>
 * </ul>
 *
 * <p>A URL path prefix is <em>not</em> a viable option here. Beyond generated clients not knowing to
 * emit it, {@code GET /{index}} is a real route, so {@code /acme/_settings} cannot be distinguished
 * from the settings of an index named {@code acme}. The ambiguity is unresolvable.
 *
 * @see SingleTenantResolver
 * @see HeaderTenantResolver
 */
public interface TenantResolver {

    /**
     * Resolves the tenant for a request.
     *
     * @param headers request headers, keys lower-cased. Transports surface non-header identity
     *                (TLS client certificate subject, SNI host) as synthetic entries here.
     * @return the tenant, or empty if the request carries no usable identity, which the caller
     *         turns into a 401
     */
    Optional<TenantId> resolve(Map<String, String> headers);
}
