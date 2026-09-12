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
 */
public record Heartbeat(
    NodeId nodeId,
    String ephemeralId,
    long timestamp,
    Applied applied,
    String shardsDigest,
    int shardCopies,
    Capacity capacity
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
