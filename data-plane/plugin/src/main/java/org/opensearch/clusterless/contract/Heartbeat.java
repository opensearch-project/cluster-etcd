/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.Objects;

/**
 * {@code nodes/heartbeat/<node-id>} — the one hot key.
 *
 * <p>Fixed shape, no unbounded field, and <b>the same size whether this node holds one shard copy or ten
 * thousand</b>. That is the entire reason it is separate from the inventory: 200 nodes writing every few
 * seconds is the store's dominant write load. Nothing may be added here that scales with shard count,
 * index count or tenant count.
 *
 * @param timestamp this node's own clock, and diagnostic only — liveness is measured by the supervisor
 *     from when it last saw this key's revision change, because two clocks that disagree by a minute would
 *     otherwise either kill a healthy cluster or leave a dead node's shards unrecovered. The field is
 *     required rather than optional precisely because that scheme depends on it: a heartbeat whose bytes
 *     did not change on every write would not advance the key's revision, and this node would look dead
 *     while heartbeating perfectly.
 * @param shardsDigest hex SHA-256 of the exact bytes last written to {@code nodes/shards/<node-id>}, or
 *     {@code null} if this node has never written an inventory
 * @param shardCopies how many copies this node holds <em>now</em>, which is what lets the supervisor spot
 *     a disagreement with the digest without re-reading the inventory
 * @param registrationRewrites how often this process has had to write its registration again after finding it
 *     missing, or {@code null} if it never has. Two scalars, so it does not scale with anything and R3.5 is
 *     satisfied; see {@link RegistrationRewrites} for why it is here and not on the registration.
 */
public record Heartbeat(
    NodeId nodeId,
    String ephemeralId,
    long timestamp,
    Applied applied,
    String shardsDigest,
    int shardCopies,
    Capacity capacity,
    RegistrationRewrites registrationRewrites
) {

    /** No assignment has been applied yet. Assignment versions start at 1, so 0 cannot collide. */
    public static final long NO_ASSIGNMENT = 0L;

    public Heartbeat {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(ephemeralId, "ephemeralId");
        Objects.requireNonNull(applied, "applied");
        Objects.requireNonNull(capacity, "capacity");
        if (shardCopies < 0) {
            throw new IllegalArgumentException("shardCopies must not be negative, was " + shardCopies);
        }
    }

    /** A heartbeat from a node that has never had to rewrite its registration, which is almost all of them. */
    public Heartbeat(
        NodeId nodeId,
        String ephemeralId,
        long timestamp,
        Applied applied,
        String shardsDigest,
        int shardCopies,
        Capacity capacity
    ) {
        this(nodeId, ephemeralId, timestamp, applied, shardsDigest, shardCopies, capacity, null);
    }

    /**
     * How often this process has found its own registration missing and written it again, and when it last did.
     *
     * <p><b>Why this is in the heartbeat and not in the registration</b>, which is where it looks like it belongs:
     * after a reap the registration is <em>absent</em>, and that is the window where the count matters most. A
     * reader would get no document at all, and could not tell "this node has never been reaped" from "this node is
     * being reaped right now and has not rewritten yet" — identical, with the second being the interesting case.
     * The heartbeat is written every interval, so it is either fresh or the node is itself the story.
     *
     * <p><b>Why {@code lastAt} earns its place beside the count.</b> A bare count is monotonic for the life of the
     * process, so the obvious rule — "count above zero is an incident" — fires on the first event and never clears
     * until somebody restarts the node. An alert that cannot clear gets suppressed, and a suppressed alert is the
     * same as no alert. With a timestamp, "a rewrite in the last fifteen minutes" is expressible, self-clearing,
     * and tells the single historical incident apart from the trickle that is the dangerous case.
     *
     * <p>Two semantics that a reader has to know and cannot infer:
     *
     * <ul>
     *   <li><b>Only rewrites caused by absence are counted.</b> An attribute change rewrites the registration too
     *       and is entirely ordinary; counting both would make the number mean nothing.
     *   <li><b>The count resets on restart</b>, because it is process-lifetime state and should not pretend to be
     *       anything else. A monitor differencing samples will see it go <em>backwards</em> across a restart, and
     *       the correct reading is "new process", not "lost event" — {@code ephemeral_id} in this same document is
     *       how it tells the two apart.
     * </ul>
     *
     * @param count how many times, always at least one — a zero-valued instance would be written as a field
     *     meaning "nothing happened", and absence already says that more cheaply
     * @param lastAt this node's clock at the most recent rewrite, with the same caveat as {@link #timestamp}
     */
    public record RegistrationRewrites(int count, long lastAt) {

        public RegistrationRewrites {
            if (count < 1) {
                throw new IllegalArgumentException("a registration-rewrite report must count at least one, was " + count);
            }
        }
    }

    /**
     * How far this node has got with the assignment it was last given.
     *
     * @param detail truncated at 512 characters, because this document has a hard size budget and the
     *     incident that produces failure detail is exactly the one that would produce a lot of it (D7.2)
     */
    public record Applied(long assignmentVersion, AppliedOutcome outcome, String detail) {

        public static final int MAX_DETAIL_LENGTH = 512;

        /** Nothing read, nothing applied — what a node says before it has ever seen an assignment. */
        public static final Applied NONE = new Applied(NO_ASSIGNMENT, AppliedOutcome.APPLIED, null);

        public Applied {
            Objects.requireNonNull(outcome, "outcome");
            if (detail != null && detail.length() > MAX_DETAIL_LENGTH) {
                detail = detail.substring(0, MAX_DETAIL_LENGTH);
            }
        }

        public static Applied failed(long assignmentVersion, String detail) {
            return new Applied(assignmentVersion, AppliedOutcome.FAILED, detail);
        }
    }

    /**
     * What allocation needs, and nothing else.
     *
     * <p>Absolute bytes rather than percentages, because a decider comparing two nodes needs the
     * denominator. Doc counts and per-shard statistics are deliberately absent: they are an observability
     * concern and would put an unbounded, fast-changing field into a document with a hard budget.
     */
    public record Capacity(long diskTotalBytes, long diskAvailableBytes, int heapUsedPercent, int cpuUsedPercent, double load1m) {

        /** What a node publishes when a probe failed. Zeroes, because the shape is fixed and the field is not optional. */
        public static final Capacity UNKNOWN = new Capacity(0, 0, 0, 0, 0);
    }
}
