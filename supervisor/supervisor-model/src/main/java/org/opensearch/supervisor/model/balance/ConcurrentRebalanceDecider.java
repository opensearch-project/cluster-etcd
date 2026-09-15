package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.allocate.Decision;

/**
 * Paces movement, and is the decider that would be wrong if cost were a single number.
 *
 * <p>Three limits, and the third only sometimes applies:
 *
 * <ul>
 *   <li><b>Pool-wide.</b> A cap on copies in flight at once, so a pool that discovers it is badly balanced
 *       corrects over minutes rather than saturating every link it has.
 *   <li><b>Per target.</b> The target writes the bytes whatever the replication mode, so it is always metered.
 *   <li><b>Per source, only when the source pays.</b> Under document replication the segments come off the
 *       source's disk and out of its network card, so draining a struggling node by moving twenty shards at
 *       once makes it worse at the job it is already behind on. Under segment replication to a remote store the
 *       source does nothing at all — the target restores from the object store — so metering the source would
 *       refuse moves that are genuinely free.
 * </ul>
 *
 * <p>That last asymmetry is the whole reason {@link MoveCost} reports two figures. A balancer that reduced a
 * move to one scalar has to pick a side: meter the source always, and it refuses to drain a remote-store node
 * at the speed it could be drained; meter it never, and it hammers a document-replication node that is already
 * the bottleneck. Neither is a tuning mistake that can be fixed with a setting, because the right answer
 * differs per index.
 *
 * <p>Everything here throttles rather than refuses. Being busy is a fact about now.
 */
public final class ConcurrentRebalanceDecider implements RebalanceDecider {

    public static final String NAME = "concurrent_rebalance";

    /** {@code cluster.routing.allocation.cluster_concurrent_rebalance} in OpenSearch, and the same default. */
    public static final int DEFAULT_POOL_LIMIT = 2;

    /** {@code node_concurrent_incoming_recoveries}, same default. */
    public static final int DEFAULT_INCOMING_LIMIT = 2;

    /** {@code node_concurrent_outgoing_recoveries}, same default — and only consulted when the source pays. */
    public static final int DEFAULT_OUTGOING_LIMIT = 2;

    private final int poolLimit;
    private final int incomingLimit;
    private final int outgoingLimit;

    public ConcurrentRebalanceDecider() {
        this(DEFAULT_POOL_LIMIT, DEFAULT_INCOMING_LIMIT, DEFAULT_OUTGOING_LIMIT);
    }

    public ConcurrentRebalanceDecider(int poolLimit, int incomingLimit, int outgoingLimit) {
        this.poolLimit = poolLimit;
        this.incomingLimit = incomingLimit;
        this.outgoingLimit = outgoingLimit;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canRebalance(RebalanceContext context) {
        int inPool = context.arrivingInPool();
        if (inPool >= poolLimit) {
            return Decision.throttle(NAME, "pool_limit", inPool + " copies are already in flight across this pool, at the"
                + " limit of " + poolLimit + "; balancing is deliberately slow so that correcting a lopsided"
                + " pool does not itself become the reason the pool is slow");
        }

        NodeId target = context.target();
        int incoming = context.arrivingOn(target);
        if (incoming >= incomingLimit) {
            return Decision.throttle(NAME, "target_limit", "node [" + target + "] already has " + incoming + " copies arriving,"
                + " at its limit of " + incomingLimit + "; the target writes the bytes whichever replication"
                + " mode is in use, so it is metered on every move");
        }

        MoveCost cost = context.cost();
        if (cost.loadsSource() == false) {
            return Decision.yes(NAME, "in flight: " + inPool + "/" + poolLimit + " in the pool and " + incoming
                + "/" + incomingLimit + " arriving on [" + target + "]; the source is not metered because "
                + cost.because());
        }

        NodeId source = context.source();
        int outgoing = context.arrivingOn(source);
        if (outgoing >= outgoingLimit) {
            return Decision.throttle(NAME, "source_limit", "node [" + source + "] is already busy with " + outgoing
                + " recoveries, at its limit of " + outgoingLimit + ", and " + cost.because()
                + " — so moving more off it now would slow down the node this move is meant to relieve");
        }
        return Decision.yes(NAME, "in flight: " + inPool + "/" + poolLimit + " in the pool, " + incoming + "/"
            + incomingLimit + " arriving on [" + target + "], " + outgoing + "/" + outgoingLimit + " leaving ["
            + source + "]");
    }
}
