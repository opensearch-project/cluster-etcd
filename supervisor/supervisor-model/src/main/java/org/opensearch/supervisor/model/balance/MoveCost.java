package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.status.Replication;

/**
 * What a move costs, <b>per node rather than as one number</b>, because the cost is not symmetric and the
 * asymmetry decides which moves are cheap.
 *
 * <p>Under segment replication backed by a remote store, the source node does nothing at all: the segments
 * are already in the object store, and the target restores from there. The bill is the target's download, and
 * a node that is being drained can be drained as fast as the targets can pull — the busy source is not a
 * reason to slow down, because the source is not busy.
 *
 * <p>Under document replication — and under node-to-node segment replication, which is the same shape and
 * which as far as anyone knows nobody runs — the segments come off the source's disk and over the source's
 * network interface. The move costs both ends, and moving twenty shards off one overloaded node at once will
 * make that node slower at the job it was already struggling with. That is the case throttling has to see.
 *
 * <p>So a balancer that models cost as a single scalar gets remote-store moves wrong in the expensive
 * direction: it will refuse to drain a node that could be drained for free.
 *
 * @param sourceBytes bytes the source has to read and send; zero when the remote store already holds them
 * @param targetBytes bytes the target has to fetch and write, which is always paid — the data has to land
 *     somewhere
 */
public record MoveCost(long sourceBytes, long targetBytes, Replication replication, String because) {

    /** Whether this move loads the node it is moving away from. */
    public boolean loadsSource() {
        return sourceBytes > 0;
    }

    /**
     * The cost of moving one copy of a known size.
     *
     * <p>An unknown size is treated as zero rather than guessed at, and that is the safe direction here for
     * the same reason as in {@link ShardSizeSignal}: a copy with no reported size is still initializing, and
     * a still-initializing copy is one the deciders refuse to move anyway.
     */
    public static MoveCost of(long sizeBytes, Replication replication) {
        long bytes = Math.max(sizeBytes, 0L);
        if (replication == Replication.SEGMENT) {
            return new MoveCost(0L, bytes, replication, "segment replication via the remote store, so the"
                + " source pays nothing and the target restores " + mib(bytes) + " MiB from the store");
        }
        return new MoveCost(bytes, bytes, replication, "document replication, so " + mib(bytes) + " MiB crosses"
            + " the network from the source as well as onto the target");
    }

    private static long mib(long bytes) {
        return bytes / (1024 * 1024);
    }
}
