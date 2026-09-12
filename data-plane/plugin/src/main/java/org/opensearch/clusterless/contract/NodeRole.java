/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.Locale;

/**
 * What a node may be given. {@code data} takes shard copies, {@code coordinator} takes routing and no
 * copies, and a node may have both.
 *
 * <p>No {@code UNKNOWN} sentinel here, unlike the supervisor's copy of this enum. The contract's
 * forward-compatibility rules — an unknown value becomes a sentinel that is never counted as healthy —
 * are obligations on a <em>reader</em>, and at this milestone the plugin only writes. The sentinel arrives
 * with the assignment reader, where a value this build has never heard of is a real possibility during a
 * rolling upgrade.
 */
public enum NodeRole {

    DATA,
    COORDINATOR;

    public String wireName() {
        return name().toLowerCase(Locale.ROOT);
    }
}
