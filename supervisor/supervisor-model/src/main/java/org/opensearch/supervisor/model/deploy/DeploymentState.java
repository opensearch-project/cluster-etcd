package org.opensearch.supervisor.model.deploy;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * The state a deployment document asks for ({@code DESIGN.md} §29.2). Upstream OpenSearch's {@code DeploymentState} is
 * {@code DRAIN} and {@code FINISH}, with further phases intended; the dual-run states are ours, named the same way.
 *
 * <p>{@code FINISH} is not a state a document is left in. It removes the deployment, as upstream does, so a document
 * that says it is read as invalid rather than as a deployment of its own.
 */
public enum DeploymentState implements WireEnum {

    /** Starts every deployment, with its attributes: the selected nodes are going down. */
    DRAIN,

    /** The new software is running on the selected nodes, with a second primary there and no traffic following it. */
    SHADOW,

    /** A fraction of each shard's search replicas follows the new primary. */
    DIAL_UP,

    /** The new writer is not good: everything returns to the old one. */
    ROLLBACK,

    /** Removes the deployment. Never stored. */
    FINISH,

    /** A value this build does not recognise. Read-only; never written back. */
    UNKNOWN;

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name();
    }

    public static DeploymentState from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
