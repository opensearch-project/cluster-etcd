package org.opensearch.supervisor.model.node;

import java.util.Locale;

/**
 * How far a node has got with the assignment it was last given.
 *
 * <p>A hint, never a source of truth. {@code NODE_CONTRACT.md} is explicit that the supervisor's model of
 * what a node has is its shard inventory, always: a supervisor that ignored this field entirely would
 * still converge, and one that trusted it over the inventory would be believing what the node was told
 * rather than what it has. It exists to make a stuck or failing node visible sooner than the inventory
 * alone would.
 */
public enum AppliedOutcome implements WireEnum {

    /** Working on it. */
    APPLYING,

    /** The assignment has been applied in full. */
    APPLIED,

    /** The node could not apply it, and {@code Heartbeat.Applied#detail} should say why. */
    FAILED,

    /** An outcome this build does not recognise. Read-only; treated exactly like no ack at all. */
    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name().toLowerCase(Locale.ROOT);
    }

    public static AppliedOutcome from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "applying" -> APPLYING;
            case "applied" -> APPLIED;
            case "failed" -> FAILED;
            default -> UNKNOWN;
        };
    }
}
