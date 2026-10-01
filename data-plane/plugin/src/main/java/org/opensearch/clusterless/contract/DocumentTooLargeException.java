/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

/**
 * A document that would exceed its size budget, and so was not written.
 *
 * <p>Thrown rather than handled here because only the caller knows what to publish instead. For the
 * inventory the answer is in D3.6 and it is not obvious: write <b>nothing</b> and report
 * {@code outcome: failed}. A truncated inventory is worse than a missing one — the supervisor reads
 * absent copies as gone, and starts rebuilding shards that exist.
 */
public class DocumentTooLargeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int actualBytes;
    private final int budgetBytes;

    public DocumentTooLargeException(String what, int actualBytes, int budgetBytes) {
        super(what + " is " + actualBytes + " bytes, over the contract's budget of " + budgetBytes);
        this.actualBytes = actualBytes;
        this.budgetBytes = budgetBytes;
    }

    public int actualBytes() {
        return actualBytes;
    }

    public int budgetBytes() {
        return budgetBytes;
    }
}
