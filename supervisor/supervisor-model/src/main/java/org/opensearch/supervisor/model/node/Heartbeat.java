package org.opensearch.supervisor.model.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.supervisor.model.NodeId;

import java.util.Objects;
import java.util.Optional;

/**
 * {@code nodes/heartbeat/<node-id>} — the one hot key.
 *
 * <p>Fixed shape, no unbounded field, and <b>the same size whether the node holds one shard copy or ten
 * thousand</b>. That is the entire reason this document is separate from the inventory: R3.5 puts 200 nodes
 * writing every few seconds as the store's dominant write load. Nothing may be added here that scales with
 * shard count, index count or tenant count.
 *
 * @param timestamp the node's own clock, diagnostic only. <b>Liveness is not computed from it</b> — see
 *     {@code NODE_CONTRACT.md} on why staleness is measured from observed revision change instead. The
 *     field is required rather than optional precisely because that scheme depends on it: a heartbeat whose
 *     content did not change on every write would not advance the key's revision, and a node would look
 *     dead while heartbeating perfectly.
 * @param shardsDigest hex SHA-256 of the exact bytes last written to {@code nodes/shards/<node-id>}, or
 *     {@code null} if the node has never written an inventory. How the supervisor avoids re-reading
 *     inventories that have not changed.
 * @param shardCopies the count, so an inconsistency can be spotted without the re-read
 * @param registrationRewrites how often this process has rewritten its registration after finding the document
 *     missing, and when it last did — or null if it never has. Asked for and implemented by the data plane; see
 *     {@link RegistrationRewrites}.
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

    /**
     * How often a node has had to put its own registration back, and when it last did.
     *
     * <p><b>Self-healing hides the thing it heals.</b> A node rewrites its registration when it finds the document
     * missing, which is what makes {@code DESIGN.md} §20.2's reaping safe — and it means a supervisor that reaped a
     * <em>live</em> node leaves no trace of having done so. This is the trace. One rewrite says the reap conditions
     * were wrong once; a steady trickle says something is systematically deleting live nodes' registrations.
     *
     * <p>Absent entirely when the count is zero, so a reader never has to distinguish "nothing happened" from a
     * field it forgot to check.
     *
     * @param count reset on restart, because it is process-lifetime state. A reader differencing samples will see
     *     it go <em>backwards</em> across a restart, and the correct reading is "new process" — which
     *     {@code ephemeralId} in the same document confirms — not "a lost event".
     * @param lastAt epoch millis of the most recent rewrite, and the reason this is not just a count. A count alone
     *     is monotonic for the life of the process, so "count &gt; 0 is an incident" fires once and never clears,
     *     and an alert that cannot clear gets suppressed. With a timestamp the alarm is on <b>age</b>, which is the
     *     law everywhere else here.
     */
    public record RegistrationRewrites(int count, long lastAt) {}

    /** No assignment has been applied yet. Assignment versions start at 1. */
    public static final long NO_ASSIGNMENT = 0L;

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

    public Heartbeat {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(ephemeralId, "ephemeralId");
        if (shardCopies < 0) {
            throw new IllegalArgumentException("shardCopies must not be negative, was " + shardCopies);
        }
    }

    public Optional<String> inventoryDigest() {
        return Optional.ofNullable(shardsDigest);
    }

    /**
     * How far the node has got with the assignment it was last given.
     *
     * @param detail bounded at 512 characters by the contract, because this document has a hard size budget
     *     and a stack trace would blow it
     */
    public record Applied(long assignmentVersion, AppliedOutcome outcome, String detail) {

        public static final int MAX_DETAIL_LENGTH = 512;

        public static final Applied NONE = new Applied(NO_ASSIGNMENT, AppliedOutcome.APPLIED, null);

        public Applied {
            if (detail != null && detail.length() > MAX_DETAIL_LENGTH) {
                detail = detail.substring(0, MAX_DETAIL_LENGTH);
            }
        }
    }

    /**
     * What allocation needs, and nothing else.
     *
     * <p>Absolute bytes rather than percentages, because a decider comparing two nodes needs the
     * denominator. Doc counts and per-shard statistics are deliberately absent: they are an observability
     * concern and would put an unbounded, fast-changing field into a document with a hard budget.
     */
    public record Capacity(
        long diskTotalBytes,
        long diskAvailableBytes,
        int heapUsedPercent,
        int cpuUsedPercent,
        @JsonProperty("load_1m") double load1m
    ) {
        public static final Capacity UNKNOWN = new Capacity(0, 0, 0, 0, 0);
    }
}
