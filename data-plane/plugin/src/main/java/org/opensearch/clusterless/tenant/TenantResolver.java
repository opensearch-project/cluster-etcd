/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.tenant;

import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.Optional;

/**
 * Which tenant the request being served belongs to.
 *
 * <p>Deliberately the same shape as the control plane's {@code TenantResolver}, down to the header name, because
 * the two services are answering one question about one request and an operator configuring isolation should not
 * have to learn it twice. That repository states the property this exists to protect, and it is worth repeating
 * where it will be read by whoever changes this file:
 *
 * <blockquote>a caller cannot choose its own tenant. If the tenant comes from a header any client can set, then
 * every authenticated caller can address every other tenant's data and isolation depends on the transport being
 * trustworthy.</blockquote>
 *
 * <p>So {@link #fromHeader} is <b>not</b> a security mechanism by itself. It is the mechanism that carries a
 * decision made somewhere trustworthy — an auth layer in front of the node that authenticates the caller and
 * overwrites whatever header they sent. Its counterpart {@link #fixed} needs no such layer, because the node
 * serves one tenant and a request cannot ask for another.
 *
 * <p><b>Empty is a real answer and the safe one.</b> A request with no resolvable tenant is not mapped to a
 * default: {@link TenantIndexNameResolver} leaves its index expressions untouched, so an unqualified name fails
 * to resolve rather than resolving to somebody's data.
 */
public interface TenantResolver {

    /** The header the control plane reads, and therefore the one this reads. */
    String HEADER = "x-tenant-id";

    Optional<TenantId> current();

    /** No tenant, ever: index expressions are never rewritten. */
    static TenantResolver none() {
        return Optional::empty;
    }

    /**
     * A node that serves exactly one tenant, from {@code clusterless.tenant}.
     *
     * <p>The configuration that does not depend on anything in front of it being trustworthy, and the one to
     * prefer where a deployment can afford a coordinator per tenant. On Kubernetes it usually can, subject to the
     * floor a JVM heap puts under a container.
     *
     * <p>A request carrying a <em>different</em> tenant in its header resolves to empty rather than to this
     * node's tenant. Serving the node's tenant would answer a question nobody asked with data they may not be
     * entitled to; empty fails the request closed.
     */
    static TenantResolver fixed(TenantId tenant, ThreadContext threadContext) {
        return () -> {
            String asked = threadContext == null ? null : threadContext.getHeader(HEADER);
            if (asked != null && asked.isEmpty() == false && asked.equals(tenant.value()) == false) {
                return Optional.empty();
            }
            return Optional.of(tenant);
        };
    }

    /**
     * The tenant named in the request's {@code x-tenant-id} header.
     *
     * <p>Trustworthy exactly to the degree that whatever sets the header is. See the class comment: this carries
     * an auth layer's decision and does not make one.
     */
    static TenantResolver fromHeader(ThreadContext threadContext) {
        return () -> {
            String header = threadContext == null ? null : threadContext.getHeader(HEADER);
            if (header == null || header.isBlank()) {
                return Optional.empty();
            }
            try {
                return Optional.of(TenantId.of(header.trim()));
            } catch (RuntimeException e) {
                // A malformed tenant is rejected rather than sanitised, for the reason the control plane gives:
                // the value becomes a key prefix, so a value containing a slash is indistinguishable from an
                // attempt to escape the namespace.
                return Optional.empty();
            }
        };
    }
}
