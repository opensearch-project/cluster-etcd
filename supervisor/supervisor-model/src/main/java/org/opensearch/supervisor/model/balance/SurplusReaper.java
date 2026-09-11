package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decides which surplus copies to drop — and, without ever knowing it, finishes the moves the rebalancer starts.
 *
 * <p>This is the second half of every move, and it is written as though the first half never happened. The
 * rebalancer builds a copy on a lightly loaded node; some passes later that copy starts; the shard now has one
 * more started copy than it asked for; this rule drops the one on the busiest node holding it. Nowhere is there
 * a record that a move was in progress, and that absence is the design.
 *
 * <p><b>Why the two halves must not know about each other.</b> A rebalancer that tracked its own moves would own
 * a small state machine — proposed, target building, target started, source dropped — living in the memory of a
 * process that can lose its lease between any two states, and every one of its transitions is a chance to drop
 * the source copy when the target never arrived. Deriving the removal from observed state instead means the only
 * states that exist are ones the store can describe: either a shard has a surplus started copy, which is untidy
 * and safe, or it does not. A supervisor that dies mid-move leaves a surplus, and its successor tidies up by
 * this same rule without needing to know what its predecessor intended.
 *
 * <p><b>Why the move lands where it was aimed.</b> Not luck — {@link MoveThresholdDecider} guarantees it. A move
 * is only accepted when the source's load exceeds the target's by more than the copy's weight {@code w}, so
 * after the target gains {@code w} the source is still the heavier of the two, and "drop the copy on the busiest
 * node" therefore picks the source. The threshold rule that stops oscillation is the same rule that makes the
 * move complete in the intended direction.
 *
 * <p>Three refusals, and each closes a way to lose data:
 *
 * <ul>
 *   <li><b>Only started copies count toward the surplus.</b> An initializing copy is a promise, not redundancy,
 *       and counting it would let a shard drop to one real copy the moment a second was merely requested.
 *   <li><b>Never a primary.</b> Dropping a primary is a demotion, which needs the fencing this service does not
 *       have — see {@link PrimaryMoveDecider}.
 *   <li><b>One copy per shard per pass.</b> A shard with three surplus copies loses them one pass at a time, so
 *       an observation that was wrong about the shard costs one copy rather than all of them.
 * </ul>
 */
public final class SurplusReaper {

    /** A copy to stop serving, and why. */
    public record Removal(ShardRef shard, NodeId node, ShardRole role, String because) {}

    private SurplusReaper() {}

    public static List<Removal> reap(Observation observation, LoadModel loads) {
        // Every started copy of every shard, and where it is. Assigned-but-not-started copies are deliberately
        // absent: they are what this rule must not mistake for redundancy.
        Map<ShardRef, List<LoadModel.Held>> started = new LinkedHashMap<>();
        Map<ShardRef, List<LoadModel.Held>> useless = new LinkedHashMap<>();
        loads.byNode().values().forEach(held -> held.forEach(copy -> {
            if (copy.present() == false) {
                return;
            }
            if (copy.state() == ShardState.STARTED) {
                started.computeIfAbsent(copy.shard(), ignored -> new ArrayList<>()).add(copy);
            } else if (copy.state() == ShardState.FAILED) {
                useless.computeIfAbsent(copy.shard(), ignored -> new ArrayList<>()).add(copy);
            }
        }));

        List<Removal> removals = new ArrayList<>();
        observation.tenants().forEach((tenant, desired) -> desired.indicesByUuid().values().forEach(index -> {
            if (index.open() == false) {
                // A closed index wants no copies at all, but its data must survive being reopened. Silence here
                // rather than a removal: the copies are not surplus, they are dormant.
                return;
            }
            for (int shard = 0; shard < index.numberOfShards(); shard++) {
                ShardRef ref = new ShardRef(tenant, index.uuid(), shard);
                removalFor(ref, index, started.getOrDefault(ref, List.of()),
                    useless.getOrDefault(ref, List.of()), loads).ifPresent(removals::add);
            }
        }));
        return List.copyOf(removals);
    }

    private static java.util.Optional<Removal> removalFor(
        ShardRef ref,
        IndexConfig index,
        List<LoadModel.Held> started,
        List<LoadModel.Held> failed,
        LoadModel loads
    ) {
        // The authorised count, not the configured one. Identical today; the distinction exists because this
        // method is the only thing in the service that deletes a copy for being one too many, which makes it
        // the only thing that would fight an autoscaler raising one shard's copy count. DESIGN.md §15.
        int desired = index.copiesAuthorisedFor(ref.shard());
        if (started.size() <= desired) {
            return java.util.Optional.empty();
        }

        // A failed copy first when there is one, because it is redundancy on paper only: dropping it costs
        // nothing and lets the node rebuild from scratch if the shard is placed back on it later.
        if (failed.isEmpty() == false) {
            LoadModel.Held copy = heaviestNodeFirst(failed, loads).getFirst();
            return java.util.Optional.of(new Removal(ref, copy.node(), copy.role(),
                "this shard has " + started.size() + " started copies where " + desired + " are wanted, and this"
                    + " copy on [" + copy.node() + "] has failed — dropping a failed copy reduces the surplus"
                    + " without reducing redundancy at all"));
        }

        List<LoadModel.Held> droppable = started.stream()
            .filter(copy -> copy.role() != ShardRole.PRIMARY)
            .toList();
        if (droppable.isEmpty()) {
            // Every surplus copy is a primary, which cannot happen from a plan this service produced and is
            // therefore not something to start deleting on the strength of one observation.
            return java.util.Optional.empty();
        }

        LoadModel.Held copy = heaviestNodeFirst(droppable, loads).getFirst();
        return java.util.Optional.of(new Removal(ref, copy.node(), copy.role(), String.format(
            java.util.Locale.ROOT,
            "this shard has %d started copies where %d are wanted; dropping the one on [%s], which at load %.3f"
                + " is the busiest node holding it, leaves %d started copies and relieves the node that most"
                + " needs relieving",
            started.size(), desired, copy.node(), loads.loadOf(copy.node()), started.size() - 1)));
    }

    /**
     * Busiest node first, then by node id.
     *
     * <p>Load rather than arbitrary choice is what turns this rule into the completion of a move, and the node
     * id tiebreak is not cosmetic: two supervisors computing the same removal from the same observation must
     * choose the same copy, or between them they will drop two.
     */
    private static List<LoadModel.Held> heaviestNodeFirst(List<LoadModel.Held> copies, LoadModel loads) {
        return copies.stream()
            .sorted(Comparator.comparingDouble((LoadModel.Held copy) -> loads.loadOf(copy.node())).reversed()
                .thenComparing(copy -> copy.node().value()))
            .toList();
    }
}
