/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

/**
 * Thrown when a write's key or value exceeds what {@link KeyValueStore} guarantees every backend accepts.
 *
 * <p>An {@link IllegalArgumentException} because the store is not at fault and retrying cannot help: a
 * consumer should answer {@code 400}, or refuse the write the way an over-budget document is refused. It is
 * distinct from the {@link IllegalStateException} a backend throws when it cannot be reached, so "too big"
 * is never mistaken for "store down".
 */
public class LimitExceededException extends IllegalArgumentException {

    public LimitExceededException(String message) {
        super(message);
    }
}
