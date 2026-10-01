/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.resolver;

import java.util.List;

/**
 * The outcome of resolving an index expression: concrete names, or the reason it could not be done.
 *
 * <p>A result type rather than exceptions, for the same reason the rest of the validation here works
 * that way — a missing index is an expected outcome of a request, not an exceptional condition, and
 * the caller has to turn it into a specific response shape anyway. See
 * {@code ErrorResponses.internalError} on where exceptions do and do not earn their keep.
 *
 * <p>The two failures are distinct because OpenSearch reports them differently:
 * {@code index_not_found_exception} versus {@code invalid_index_name_exception}.
 */
public sealed interface IndexResolution {

    /** Resolved successfully. May be empty, which is not an error when {@code allow_no_indices}. */
    record Resolved(List<String> indices) implements IndexResolution {}

    /** A concrete name did not exist, or a pattern matched nothing without {@code allow_no_indices}. */
    record NotFound(String expression) implements IndexResolution {}

    /** The expression could never be an index name, e.g. a leading underscore. */
    record InvalidName(String expression, String reason) implements IndexResolution {}

    static IndexResolution resolved(List<String> indices) {
        return new Resolved(List.copyOf(indices));
    }

    static IndexResolution notFound(String expression) {
        return new NotFound(expression);
    }

    static IndexResolution invalidName(String expression, String reason) {
        return new InvalidName(expression, reason);
    }
}
