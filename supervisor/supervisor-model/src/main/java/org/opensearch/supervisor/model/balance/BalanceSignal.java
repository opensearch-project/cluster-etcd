package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardInventory;

/**
 * One measure of how much a shard copy costs the node holding it.
 *
 * <p>A signal is <b>a measure, not a veto</b> — which is the difference between this and
 * {@code AllocationDecider}, and the reason rebalancing needs its own interface rather than reusing that one.
 * A decider answers "may this?" and any one of them can refuse; a signal answers "how much?" and they are
 * combined. Conflating the two is what makes a balancer hard to extend: a rule that can only say no cannot
 * express "this shard is four times the problem that one is".
 *
 * <p>Adding a signal is the intended way to make balancing smarter. The obvious next one is heat — the share
 * of a node's search CPU a given shard is responsible for — which is why {@link #weightOf} is handed the whole
 * copy rather than just its size: the data a signal needs comes from the heartbeat and inventory, and a new
 * signal should be a new class rather than a new branch in an existing one.
 */
public interface BalanceSignal {

    /** Short, stable name, used in explanations and as the key for its relative importance. */
    String name();

    /**
     * How much this copy contributes to its node's load, in whatever unit this signal measures.
     *
     * <p>Units do not have to be comparable between signals — {@link LoadModel} normalises each signal by its
     * own mean before combining, because shards and bytes cannot be added.
     */
    double weightOf(ShardInventory.ShardCopy copy, IndexConfig index);
}
