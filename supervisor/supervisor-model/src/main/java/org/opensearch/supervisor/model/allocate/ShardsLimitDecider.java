package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;

/**
 * Caps the number of shard copies on one node.
 *
 * <p>Disk is not the only thing a shard consumes. Each one costs file handles, heap for its segment metadata,
 * and a slice of every refresh and merge cycle, and a node can be comfortable on disk while being useless
 * because it holds ten thousand tiny shards. OpenSearch's own guidance is a few hundred per node.
 *
 * <p>The limit comes from {@code pools/definitions/<pool>.max_shard_copies_per_node} — operator-owned, because
 * it depends on the machine and on what the tenant's shards are like, neither of which the supervisor can see.
 * Absent, the rule is off: guessing a cap would refuse placements for a reason nobody chose.
 */
public final class ShardsLimitDecider implements AllocationDecider {

    public static final String NAME = "shards_limit";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canAllocate(AllocationContext context, NodeId candidate) {
        var limit = context.maxCopiesPerNode();
        if (limit.isEmpty()) {
            return Decision.yes(NAME, "this pool sets no shard limit per node");
        }
        int current = context.copiesOn(candidate);
        if (current >= limit.get()) {
            return Decision.no(NAME, "at_limit", "node [" + candidate + "] holds or is assigned " + current
                + " copies, at this pool's limit of " + limit.get()
                + "; disk is not the only thing a shard costs");
        }
        return Decision.yes(NAME, "node [" + candidate + "] holds " + current + " of at most " + limit.get()
            + " copies");
    }
}
