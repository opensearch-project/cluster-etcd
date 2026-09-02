/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.resolver;

import org.opensearch.controlplane.rest.RestRequest;

import java.util.Locale;

/**
 * How an index expression should be resolved, from {@code expand_wildcards},
 * {@code ignore_unavailable} and {@code allow_no_indices}.
 *
 * <p>Each API carries its own defaults, and they are not uniform: {@code indices.exists} treats a
 * wildcard matching nothing as success but a missing concrete name as failure, which is
 * {@code ignoreUnavailable=false} with {@code allowNoIndices=true}. Verified against OpenSearch 3.9.0.
 *
 * @param expandOpen        include open indices when expanding a wildcard
 * @param expandClosed      include closed indices when expanding a wildcard
 * @param expandHidden      include hidden indices when expanding a wildcard
 * @param ignoreUnavailable a concrete name that does not exist is skipped rather than an error
 * @param allowNoIndices    a wildcard matching nothing is allowed rather than an error
 */
public record IndicesOptions(
    boolean expandOpen,
    boolean expandClosed,
    boolean expandHidden,
    boolean ignoreUnavailable,
    boolean allowNoIndices
) {

    /** Wildcards expand to open indices only; missing concrete names are errors. */
    public static IndicesOptions strictExpandOpen() {
        return new IndicesOptions(true, false, false, false, true);
    }

    /** As used by {@code indices.exists}: a wildcard may match nothing, a missing name may not. */
    public static IndicesOptions forExists() {
        return new IndicesOptions(true, true, false, false, true);
    }

    /** Nothing is an error; used by APIs that report on whatever happens to be there. */
    public static IndicesOptions lenient() {
        return new IndicesOptions(true, false, false, true, true);
    }

    /**
     * Reads the overrides a request supplies, keeping this instance's values as defaults.
     *
     * <p>{@code expand_wildcards} is a comma-separated list of {@code open}, {@code closed},
     * {@code hidden}, {@code all} and {@code none}. {@code all} means open plus closed plus hidden;
     * {@code none} disables expansion entirely, which the resolver treats as leaving expressions
     * untouched.
     */
    public IndicesOptions withOverridesFrom(RestRequest request) {
        boolean open = expandOpen;
        boolean closed = expandClosed;
        boolean hidden = expandHidden;

        String expand = request.param("expand_wildcards");
        if (expand != null && expand.isBlank() == false) {
            open = false;
            closed = false;
            hidden = false;
            for (String value : expand.split(",")) {
                switch (value.trim().toLowerCase(Locale.ROOT)) {
                    case "open" -> open = true;
                    case "closed" -> closed = true;
                    case "hidden" -> hidden = true;
                    case "all" -> {
                        open = true;
                        closed = true;
                        hidden = true;
                    }
                    case "none" -> { }
                    default -> { }
                }
            }
        }

        return new IndicesOptions(
            open,
            closed,
            hidden,
            request.paramAsBoolean("ignore_unavailable", ignoreUnavailable),
            request.paramAsBoolean("allow_no_indices", allowNoIndices)
        );
    }

    /** With neither open nor closed expansion there is nothing a wildcard could expand to. */
    boolean expandsWildcards() {
        return expandOpen || expandClosed;
    }
}
