/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.Locale;

/**
 * What a node may be given, and what a tenant entry in an assignment asks of it. {@code data} takes shard
 * copies, {@code coordinator} takes routing and no copies, and a node may have both.
 */
public enum NodeRole {

    DATA,
    COORDINATOR,

    /**
     * A role this build does not recognise.
     *
     * <p>Read-only. It grants no capability, so a tenant assigned an unknown role is a tenant this node does
     * nothing for — and says so — rather than one it guesses about. Failing the parse instead would lose the
     * whole assignment, including the tenants it does understand.
     */
    UNKNOWN;

    /**
     * @throws IllegalStateException if this is the sentinel.
     */
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name().toLowerCase(Locale.ROOT);
    }

    public static NodeRole from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "data" -> DATA;
            case "coordinator" -> COORDINATOR;
            default -> UNKNOWN;
        };
    }
}
