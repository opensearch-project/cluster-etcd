/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.Locale;

/**
 * How far this node has got with the assignment it was last given.
 *
 * <p>A hint, never a source of truth. The supervisor's model of what this node has is
 * {@code nodes/shards/<node-id>}, always: a supervisor that ignored this field entirely would still
 * converge. It exists to make a stuck or failing node visible sooner than the inventory alone would.
 */
public enum AppliedOutcome {

    /** Working on it. */
    APPLYING,

    /** The assignment has been applied in full. */
    APPLIED,

    /** It could not be applied, and the accompanying detail should say why. */
    FAILED;

    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
