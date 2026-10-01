package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.node.Heartbeat;

/**
 * Refuses a node that is running out of disk.
 *
 * <p>Two watermarks rather than one, following OpenSearch, and the reason is that the two answer different
 * questions. Below the low watermark a node takes anything. Above it, the node takes no <em>new</em> copies but
 * keeps what it has — because moving a shard off a full node writes it somewhere else before deleting it here,
 * which needs disk it may not have. Above the high watermark nothing is placed at all.
 *
 * <p>A node whose heartbeat carries no capacity figures is refused rather than assumed empty. Guessing in the
 * optimistic direction fills up the one node nobody can see, and disk is the failure this whole rule exists to
 * avoid.
 */
public final class DiskThresholdDecider implements AllocationDecider {

    public static final String NAME = "disk_threshold";

    /** Above this fraction used, no new copies. */
    public static final double DEFAULT_LOW_WATERMARK = 0.85;

    /** Above this fraction used, nothing at all — and existing copies should be moved off. */
    public static final double DEFAULT_HIGH_WATERMARK = 0.90;

    private final double lowWatermark;
    private final double highWatermark;

    public DiskThresholdDecider() {
        this(DEFAULT_LOW_WATERMARK, DEFAULT_HIGH_WATERMARK);
    }

    public DiskThresholdDecider(double lowWatermark, double highWatermark) {
        if (lowWatermark > highWatermark) {
            throw new IllegalArgumentException(
                "low watermark " + lowWatermark + " must not exceed high watermark " + highWatermark
            );
        }
        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canAllocate(AllocationContext context, NodeId candidate) {
        var node = context.observation().nodes().get(candidate);
        Heartbeat heartbeat = node == null ? null : node.heartbeat();
        if (heartbeat == null || heartbeat.capacity() == null || heartbeat.capacity().diskTotalBytes() <= 0) {
            return Decision.no(NAME, "no_figures", "node [" + candidate + "] reports no usable disk figures, and assuming it"
                + " has room is how the one node nobody can see gets filled up");
        }

        Heartbeat.Capacity capacity = heartbeat.capacity();
        long total = capacity.diskTotalBytes();
        long free = Math.max(0, capacity.diskAvailableBytes());
        double used = 1.0 - ((double) free / total);

        if (used >= highWatermark) {
            return Decision.no(NAME, "high_watermark", String.format(
                "node [%s] is %.1f%% full, at or above the high watermark of %.0f%%; copies should be moving"
                    + " off it rather than onto it", candidate, used * 100, highWatermark * 100
            ));
        }
        if (used >= lowWatermark) {
            return Decision.no(NAME, "low_watermark", String.format(
                "node [%s] is %.1f%% full, at or above the low watermark of %.0f%%, so it keeps what it has"
                    + " but takes nothing new", candidate, used * 100, lowWatermark * 100
            ));
        }
        return Decision.yes(NAME, String.format(
            "node [%s] is %.1f%% full, under the low watermark of %.0f%%", candidate, used * 100, lowWatermark * 100
        ));
    }

    /** Free bytes on a node, or zero if it did not say — the tiebreak among candidates that all said yes. */
    public static long freeBytes(AllocationContext context, NodeId candidate) {
        var node = context.observation().nodes().get(candidate);
        if (node == null || node.heartbeat() == null || node.heartbeat().capacity() == null) {
            return 0;
        }
        return Math.max(0, node.heartbeat().capacity().diskAvailableBytes());
    }
}
