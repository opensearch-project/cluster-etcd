package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.allocate.Decision;

/**
 * Missing copies before tidy ones.
 *
 * <p>Rebalancing and recovery draw on the same finite thing — disk, network, and the target node's attention —
 * and they are not equally valuable. A shard with one copy where two were asked for is one failure from being
 * unavailable; a node holding slightly more than its share is an inefficiency. Spending recovery bandwidth on
 * the second while the first is outstanding is the wrong trade every time, and it is a trade a balancer makes
 * silently unless something stops it.
 *
 * <p>This also breaks a genuinely unpleasant feedback loop. A node that has just been lost leaves its shards
 * under-replicated <em>and</em> leaves the pool lopsided, so a balancer without this rule would start shuffling
 * shards between the survivors at exactly the moment they are busiest rebuilding what was lost — making the
 * rebuild slower, which extends the window in which a second failure loses data.
 *
 * <p>{@code THROTTLE} rather than {@code NO}, and the distinction earns its keep here: the pool is not
 * unbalanceable, it is busy, and an operator who reads "blocked" when the answer is "later" will go looking for
 * a fault that is not there.
 */
public final class RecoveryPriorityDecider implements RebalanceDecider {

    public static final String NAME = "recovery_priority";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canRebalance(RebalanceContext context) {
        int missing = context.underReplicatedCount();
        if (missing > 0) {
            return Decision.throttle(NAME, "under_replicated", missing + " shard(s) in this pool have fewer copies than they want;"
                + " restoring redundancy comes before evening out load, because both need the same recovery"
                + " bandwidth and only one of them is protecting data");
        }
        return Decision.yes(NAME, "every shard in this pool has the copies it wants, so recovery bandwidth is"
            + " free for balancing");
    }
}
