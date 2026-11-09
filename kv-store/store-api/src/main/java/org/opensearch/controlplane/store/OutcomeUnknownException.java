/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

/**
 * Thrown when a write may or may not have been applied, because the backend lost its answer: a connection
 * dropped or a timeout expired after the request was sent.
 *
 * <p>Distinct from {@link RevisionConflictException} on purpose. Reporting a lost answer as a conflict is
 * how a write that succeeded gets treated as one that lost — a created index answering "already exists"
 * and never being built, a renewed lease being given up. A caller that needs to know reads the key: its
 * own value at a new revision means the write applied.
 *
 * <p>An {@link IllegalStateException}, like every other failure to reach the store, so code that treats
 * store failures alike keeps doing so.
 */
public class OutcomeUnknownException extends IllegalStateException {

    public OutcomeUnknownException(String message, Throwable cause) {
        super(message, cause);
    }
}
