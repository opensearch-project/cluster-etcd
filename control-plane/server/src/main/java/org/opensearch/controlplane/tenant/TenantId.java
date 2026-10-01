/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.tenant;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A tenant identifier.
 *
 * <p>Validation here is security-critical, not cosmetic: the value becomes a key prefix in the
 * shared store, so a tenant ID containing {@code /} could escape its own namespace and read or
 * overwrite another tenant's data. The character set is therefore deliberately narrow.
 *
 * <p>Note that {@link org.opensearch.controlplane.store.KeyValueStore} does no path normalisation,
 * so {@code ..} in a key is inert — it is just part of the key. A filesystem- or object-backed
 * implementation that maps keys onto paths <em>must</em> normalise or reject traversal sequences,
 * otherwise the isolation this class provides is defeated one layer down.
 */
public record TenantId(String value) {

    /** Must start alphanumeric, then alphanumerics plus {@code _ . -}. Notably excludes {@code /}. */
    private static final Pattern VALID = Pattern.compile("[a-zA-Z0-9][a-zA-Z0-9_.\\-]{0,127}");

    public TenantId {
        Objects.requireNonNull(value, "tenant id must not be null");
        if (VALID.matcher(value).matches() == false) {
            throw new IllegalArgumentException(
                "invalid tenant id [" + value + "]: must be 1-128 characters of [A-Za-z0-9_.-], "
                    + "starting alphanumeric, and must not contain '/'"
            );
        }
    }

    public static TenantId of(String value) {
        return new TenantId(value);
    }

    /** The key-space prefix owned by this tenant, e.g. {@code tenant/acme/}. */
    public String keyPrefix() {
        return "tenant/" + value + "/";
    }

    @Override
    public String toString() {
        return value;
    }
}
