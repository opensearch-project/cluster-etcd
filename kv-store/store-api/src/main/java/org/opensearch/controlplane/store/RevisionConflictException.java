/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.util.Locale;

/**
 * Thrown when a conditional write finds the key at a revision other than the expected one.
 *
 * <p>Unchecked because the common handler pattern is to retry the read-modify-write cycle or to
 * translate this into a {@code 409 Conflict}, neither of which benefits from a checked exception on
 * every store call.
 */
public class RevisionConflictException extends RuntimeException {

    /**
     * The actual revision, when the backend cannot report it without another round trip. Never a real
     * revision, since revisions are positive.
     */
    public static final long UNKNOWN = -1L;

    private final String key;
    private final long expectedRevision;
    private final long actualRevision;

    public RevisionConflictException(String key, long expectedRevision, long actualRevision) {
        super(String.format(
            Locale.ROOT,
            "key [%s] is at revision [%d], expected [%d]",
            key,
            actualRevision,
            expectedRevision
        ));
        this.key = key;
        this.expectedRevision = expectedRevision;
        this.actualRevision = actualRevision;
    }

    public String key() {
        return key;
    }

    public long expectedRevision() {
        return expectedRevision;
    }

    /**
     * Best effort, for messages and logs: the revision the key was found at, {@link KeyValueStore#ABSENT} if
     * it did not exist, or {@link #UNKNOWN} where finding out would cost another request. It may already be
     * stale. To act on the current state, read the key again.
     */
    public long actualRevision() {
        return actualRevision;
    }
}
