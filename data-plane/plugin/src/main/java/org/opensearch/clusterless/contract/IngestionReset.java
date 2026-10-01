/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * {@code ingestion_reset} on an index in this node's assignment — move a pull-based consumer to a position.
 *
 * <p><b>The only instruction in a document that is otherwise entirely desired state</b>, and it is worth
 * understanding why it had to be, because the distinction is the one this whole design rests on. Desired state is
 * a value a reconciler re-applies for ever without harm. "Start from offset 1000" re-applied for ever is a stream
 * that never advances — every pass would rewind it — so it cannot live in the index document beside
 * {@code ingestion_status}, which is exactly where it looks like it belongs.
 *
 * <p>So it rides here, tagged with the id of the control-plane task that asked for it, and the supervisor stops
 * writing it once this node acknowledges. {@code DESIGN.md} §21.2 and {@code NODE_CONTRACT.md}'s assignment
 * section carry the agreement; three obligations of it land on this class and the code around it:
 *
 * <ul>
 *   <li><b>Idempotence keys on {@link #id}, never on the assignment version.</b> The version moves for unrelated
 *       reasons — a shard placed elsewhere in the same document — and re-applying a reset because an unrelated
 *       field changed is the rewind-for-ever failure arriving by another route.
 *   <li><b>The poller must be paused before the reset is applied.</b> Not this class's job, but the reason it
 *       cannot be inferred from here: upstream refuses a reset on a running poller, and {@code is_paused: false}
 *       reaches this node <em>faster</em> than the reset does, since one is state in a document already read
 *       every pass and the other is an instruction that must be observed, planned, written and read. See
 *       {@code IngestionResets}.
 *   <li><b>An unrecognised mode is reported, not skipped.</b> Which is why {@link Mode#UNKNOWN} exists and is
 *       not simply dropped at parse time, unlike the shard roles in {@link ShardRole} — those describe a copy
 *       this node then declines to build, whereas silently discarding a reset would report success for an
 *       instruction nothing carried out.
 * </ul>
 *
 * @param id the task id that asked for this, opaque and compared only for equality
 * @param shards where to move each shard's consumer, keyed by shard number
 */
public record IngestionReset(String id, Map<Integer, Pointer> shards) {

    public IngestionReset {
        Objects.requireNonNull(id, "id");
        if (id.isEmpty()) {
            // Without an id there is nothing to key idempotence on, so this would be re-applied every pass for
            // as long as the supervisor kept writing it -- the failure the id exists to prevent.
            throw new IllegalArgumentException("an ingestion reset must carry the id of the task that asked for it");
        }
        shards = shards == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(shards));
    }

    /** Where one shard's consumer should be moved to. */
    public record Pointer(Mode mode, String value) {

        public Pointer {
            Objects.requireNonNull(mode, "mode");
            Objects.requireNonNull(value, "value");
        }
    }

    /**
     * How to interpret a pointer's value.
     *
     * <p>One spelling, pinned in {@code DESIGN.md} §21.6 because three repositories could each have guessed
     * differently: the control plane validates the client's word case-insensitively and stores it
     * <em>verbatim</em>, the supervisor normalises to lower case on the way into the assignment, and this node
     * maps that onto {@code StreamPoller.ResetState}. The value is a string in every case — an offset, or epoch
     * milliseconds for a timestamp.
     */
    public enum Mode {

        /** {@code StreamPoller.ResetState.RESET_BY_OFFSET}. */
        OFFSET,

        /** {@code StreamPoller.ResetState.RESET_BY_TIMESTAMP}, and the value is epoch millis. */
        TIMESTAMP,

        /**
         * A mode this build does not know, from a supervisor or control plane newer than this node.
         *
         * <p><b>Reported and left unapplied, which is the opposite of what an unknown shard role gets.</b> An
         * unrecognised role means a copy is not built, and the shard's absence is itself visible to the
         * supervisor's differ. Nothing makes an unapplied reset visible except this node saying so: the
         * supervisor completes the task on this node's {@code applied}, so a reset dropped quietly would be
         * reported as a pointer that moved when nothing moved it.
         */
        UNKNOWN;

        public static Mode from(String value) {
            if (value == null) {
                return UNKNOWN;
            }
            return switch (value.trim().toUpperCase(Locale.ROOT)) {
                case "OFFSET" -> OFFSET;
                case "TIMESTAMP" -> TIMESTAMP;
                default -> UNKNOWN;
            };
        }
    }

    /** Whether this reset says anything about a shard. */
    public boolean names(int shard) {
        return shards.containsKey(shard);
    }
}
