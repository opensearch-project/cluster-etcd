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
    Map<String, String> refusals
) {

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
            if (instruction instanceof Instruction.PlaceCopy place) {
                byNode.computeIfAbsent(place.node(), node -> new java.util.ArrayList<>()).add(instruction);
            }
        }
        return byNode;
    }

    public Set<NodeId> nodesTouched() {
        return new LinkedHashSet<>(byNode().keySet());
    }
}
