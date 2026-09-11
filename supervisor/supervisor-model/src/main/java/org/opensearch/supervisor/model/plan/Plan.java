package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * What one pass intends to do, and the pass it was computed from.
 *
 * <p><b>A plan never outlives its pass.</b> R6.3: a supervisor that stalls and comes back must recompute
 * rather than resume, because plans are cheap and stale ones are dangerous — so this is passed straight from
 * the planner to the applier and then dropped. Nothing persists it, and the {@code observedAt} it carries is
 * what the applier checks before touching anything.
 *
 * @param epoch the lease epoch this plan was computed under, stamped into every assignment it writes so a
 *     split brain is diagnosable afterwards from data the system already wrote
 * @param balance how lopsided the pool is and what, if anything, is being done about it. Pool-level rather
 *     than per-shard, which is why it is its own field: "your pool is uneven and here is the rule stopping it
 *     being evened out" is not a statement about any one shard, and filing it under one would put it in a
 *     tenant's diff beside an unrelated finding — or, when nothing at all is wrong, nowhere.
 * @param refusals subject key to the allocator's reason, for copies it wanted to place and could not.
 *     <b>The most useful thing in this record.</b> "Shard 3 is unassigned" is the most common unanswerable
 *     question an operator has about a cluster, and the deciders compute the answer as a side effect of
 *     refusing — throwing it away, which an earlier version did, wasted the entire point of splitting the
 *     rules up.
 */
public record Plan(
    long pass,
    Instant observedAt,
    PoolId pool,
    long epoch,
    List<Instruction> instructions,
    boolean ceilingHit,
    Map<String, String> refusals,
    Balance balance
) {

    /**
     * The pool's load spread, and one sentence about it.
     *
     * @param spread gap between the busiest and idlest node, in the load model's normalised units, where an
     *     average node sits at about the sum of the signal importances. Published as a number as well as prose
     *     because "is this getting better?" is the convergence document's whole purpose and this is the one
     *     figure that answers it for balance.
     */
    public record Balance(double spread, String because) {}

    public Plan(
        long pass,
        Instant observedAt,
        PoolId pool,
        long epoch,
        List<Instruction> instructions,
        boolean ceilingHit,
        Map<String, String> refusals
    ) {
        this(pass, observedAt, pool, epoch, instructions, ceilingHit, refusals, null);
    }

    public Plan {
        Objects.requireNonNull(pool, "pool");
        Objects.requireNonNull(observedAt, "observedAt");
        instructions = instructions == null ? List.of() : List.copyOf(instructions);
        refusals = refusals == null ? Map.of() : Map.copyOf(refusals);
    }

    public static Plan empty(long pass, Instant observedAt, PoolId pool, long epoch) {
        return new Plan(pass, observedAt, pool, epoch, List.of(), false, Map.of());
    }

    public static Plan empty(long pass, Instant observedAt, PoolId pool, long epoch, Map<String, String> refusals) {
        return new Plan(pass, observedAt, pool, epoch, List.of(), false, refusals);
    }

    public boolean isEmpty() {
        return instructions.isEmpty();
    }

    /** Instructions grouped by the node whose assignment they change, since that is what gets written. */
    public Map<NodeId, List<Instruction>> byNode() {
        Map<NodeId, List<Instruction>> byNode = new LinkedHashMap<>();
        for (Instruction instruction : instructions) {
            NodeId node = switch (instruction) {
                case Instruction.PlaceCopy place -> place.node();
                case Instruction.RemoveCopy remove -> remove.node();
            };
            byNode.computeIfAbsent(node, ignored -> new java.util.ArrayList<>()).add(instruction);
        }
        return byNode;
    }

    public Set<NodeId> nodesTouched() {
        return new LinkedHashSet<>(byNode().keySet());
    }
}
