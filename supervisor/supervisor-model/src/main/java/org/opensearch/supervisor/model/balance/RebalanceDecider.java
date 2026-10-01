package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.allocate.Decision;

/**
 * One rule about whether a proposed move may happen.
 *
 * <p>The interface OpenSearch does not have, and the reason its balancer is hard to improve. There, the
 * decision to move a shard is spread between {@code AllocationDecider#canRebalance}, the weight function
 * inside {@code BalancedShardsAllocator}, and a threshold; adding a consideration means finding which of those
 * three it belongs in and usually editing the weight function, which every other consideration also shares. So
 * the rules cannot be listed, cannot be tested one at a time, and cannot be explained individually to the
 * operator asking why their cluster is lopsided.
 *
 * <p>Here a move has to survive two separate sets of rules, which are separate because they answer different
 * questions:
 *
 * <ul>
 *   <li>These deciders answer <b>"should this move happen?"</b> — is rebalancing switched on, is there more
 *       urgent recovery work, is the benefit worth the bytes, is too much already in flight.
 *   <li>The {@code AllocationDecider} set answers <b>"may a copy live there?"</b> — awareness, disk, shard
 *       limits, same-host. A move's target is a placement like any other and is judged by exactly the rules
 *       that judge a new copy, reused rather than restated. A balancer with its own private idea of a legal
 *       placement is a balancer that will eventually move a shard somewhere allocation would have refused.
 * </ul>
 *
 * <p>Verdicts are the same three as allocation, and {@code THROTTLE} carries its full weight here: almost
 * every reason to refuse a move is a reason to refuse it <em>now</em>. A cluster that is too busy to move
 * shards this minute is not a cluster that should never move them, and collapsing the two would make an
 * operator investigate a pacing decision as though it were a fault.
 */
public interface RebalanceDecider {

    /** Short, stable name, used in explanations. */
    String name();

    /** Whether the proposed move may proceed. */
    Decision canRebalance(RebalanceContext context);
}
