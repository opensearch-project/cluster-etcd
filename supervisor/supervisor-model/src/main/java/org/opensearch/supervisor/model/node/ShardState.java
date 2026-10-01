package org.opensearch.supervisor.model.node;

import org.opensearch.supervisor.model.WireEnum;

import java.util.Locale;

/**
 * How a shard copy is getting on, as distinct from what it is — see {@link ShardRole}.
 *
 * <p>Two states from OpenSearch's routing table are deliberately absent, and the mapping is part of the
 * contract rather than left to each implementation:
 *
 * <ul>
 *   <li><b>{@code RELOCATING}</b> — a move is reported as two ordinary observations: {@code STARTED} on the
 *       source and {@code INITIALIZING} on the target. An interrupted move then leaves two states that mean
 *       what they say, rather than one state that nobody owns and that the supervisor would have to
 *       reason about differently from every other.
 *   <li><b>{@code CLOSED} / {@code CLOSING}</b> — a closed index is reported as having no copies at all.
 *       {@code SUPERVISOR_KEYS.md} already says a closed index is green with no shards, matching
 *       OpenSearch, so a state for it would exist only to be filtered out again.
 * </ul>
 *
 * <p>Both mappings lose information the data plane has. They are recorded here so the loss is a decision
 * rather than an omission, and so a node implementer does not have to guess.
 */
public enum ShardState implements WireEnum {

    /** Recovering or being created; present, occupying disk, not yet serving. */
    INITIALIZING,

    /** Serving. */
    STARTED,

    /** The copy exists and is broken. It occupies disk and cannot serve. */
    FAILED,

    /**
     * A state this build does not recognise, from a node newer than this supervisor.
     *
     * <p>Read-only, and the conservative reading is deliberate: an unknown state must never be counted as
     * serving, because that would let the supervisor believe a shard is served when it is not, and must
     * never be counted as absent either, because that would have it place a duplicate. It is "something is
     * there and I cannot say what", which is exactly a finding for a human.
     *
     * <p>The alternative — failing the whole inventory parse — loses every other shard on that node over
     * one unrecognised string, which during a rolling upgrade is how a cluster gets rebuilt underneath
     * itself.
     */
    UNKNOWN;

    /**
     * Whether this copy is serving traffic.
     *
     * <p>Every state occupies disk, including {@code FAILED} and {@code UNKNOWN} — so there is no
     * counterpart predicate: capacity counts copies, not states.
     */
    public boolean isServing() {
        return this == STARTED;
    }

    @Override
    public String wireName() {
        if (this == UNKNOWN) {
            throw new IllegalStateException("UNKNOWN is a read-only sentinel and must never be written");
        }
        return name();
    }

    public static ShardState from(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        return switch (value.toUpperCase(Locale.ROOT)) {
            case "INITIALIZING" -> INITIALIZING;
            case "STARTED" -> STARTED;
            case "FAILED" -> FAILED;
            default -> UNKNOWN;
        };
    }
}
