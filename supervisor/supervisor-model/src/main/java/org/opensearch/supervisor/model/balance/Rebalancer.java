package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.allocate.AllocationContext;
import org.opensearch.supervisor.model.allocate.Allocator;
import org.opensearch.supervisor.model.allocate.Decision;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.observe.Observation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Proposes at most one move per pass, or explains why the pool is as balanced as it is going to get.
 *
 * <p><b>One move per pass, deliberately.</b> Proposing several would mean predicting the pool's load after each
 * accepted move, and a prediction is exactly what a level-triggered reconciler is built to avoid: two moves that
 * each look like an improvement against the same starting state can jointly overshoot, leaving the pool worse
 * than before and the balancer certain it has helped. Re-observing costs one pass, and passes are cheap. It also
 * means the concurrency limits are checked against something real rather than something forecast.
 *
 * <p>Search order is heaviest-source, heaviest-copy, lightest-target. Starting from the largest shard on the
 * busiest node is the ordering that fixes the most imbalance per byte moved, and it is the specific thing a
 * count-only balancer cannot do: with every shard weighing one, "the heaviest copy" is meaningless and the
 * balancer picks whichever shard it happens to iterate onto, which is why a cluster can be perfectly balanced by
 * shard count and still have one node holding most of the data.
 *
 * <p><b>The target is judged by the allocation deciders, not by anything here.</b> A move's destination is a
 * placement like any other — awareness, disk watermarks, shard limits and same-host all apply unchanged — so
 * those rules are reused rather than restated. A balancer with a private notion of a legal placement will
 * eventually move a copy somewhere allocation itself would have refused, and the two will disagree quietly.
 */
public final class Rebalancer {

    /** A copy to build somewhere new, so that the old one can be dropped once it has landed. */
    public record Move(
        ShardRef shard,
        ShardRole role,
        NodeId source,
        NodeId target,
        double weight,
        double spreadBefore,
        MoveCost cost,
        String because
    ) {}

    /** Why nothing is moving, kept per rule so the answer is specific. */
    public record Refusal(double spread, String because, Map<String, String> byDecider) {

        public Refusal {
            byDecider = byDecider == null ? Map.of() : Map.copyOf(byDecider);
        }
    }

    /** Either a move or a refusal — never both, and never neither. */
    public record Outcome(Move move, Refusal refusal) {

        public boolean moving() {
            return move != null;
        }
    }

    /** The default set, in the order their explanations read best. */
    public static List<RebalanceDecider> defaultDeciders() {
        return List.of(
            // The operator's own answer first, so a refusal they caused reads before any the cluster caused.
            new RebalanceEnabledDecider(),
            // Then whether balancing should be happening at all, before any per-move detail.
            new RecoveryPriorityDecider(),
            new PrimaryMoveDecider(),
            new MoveThresholdDecider(),
            // Pacing last: "not now" is only interesting once everything else has said "yes, in principle".
            new ConcurrentRebalanceDecider()
        );
    }

    private final List<RebalanceDecider> deciders;
    private final Allocator allocator;
    private final double threshold;

    public Rebalancer() {
        this(defaultDeciders(), new Allocator(), MoveThresholdDecider.DEFAULT_THRESHOLD);
    }

    public Rebalancer(List<RebalanceDecider> deciders, Allocator allocator, double threshold) {
        this.deciders = List.copyOf(Objects.requireNonNull(deciders, "deciders"));
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.threshold = threshold;
    }

    public Outcome propose(Observation observation) {
        return propose(observation, LoadModel.of(observation), List.of(), null);
    }

    public Outcome propose(
        Observation observation,
        LoadModel loads,
        List<String> awarenessAttributes,
        Integer maxCopiesPerNode
    ) {
        double spread = loads.spread();
        Map<String, String> byDecider = new LinkedHashMap<>();
        // Whether any candidate move at all would have improved the spread, regardless of what else refused it.
        // A "no" here subsumes every other reason: if nothing would help, the pool is simply balanced, and
        // reporting that primaries cannot move would send an operator after a gap that is costing them nothing.
        boolean anyMoveWouldHelp = false;

        List<NodeId> sources = loads.byLoadDescending();
        List<NodeId> targets = loads.byLoadAscending();
        if (sources.size() < 2) {
            return new Outcome(null, new Refusal(spread, "there is only " + sources.size() + " node in this"
                + " pool, so there is nowhere to move anything to", Map.of()));
        }

        for (NodeId source : sources) {
            for (LoadModel.Held copy : loads.heldBy(source)) {
                if (copy.isMovable() == false) {
                    // A copy that is still arriving cannot be moved away from a node it has not reached, and
                    // moving one mid-recovery throws away the bytes already transferred.
                    byDecider.putIfAbsent("copy_state", "the heaviest copies on the busiest nodes are still"
                        + " recovering, so there is nothing settled enough to move yet");
                    continue;
                }
                IndexConfig index = indexOf(observation, copy.shard());

                for (NodeId target : targets) {
                    if (target.equals(source)) {
                        continue;
                    }
                    RebalanceContext context = new RebalanceContext(
                        observation, loads, copy, target, index, threshold
                    );

                    Decision.Combined verdict = decide(context);
                    anyMoveWouldHelp |= saidYes(verdict, MoveThresholdDecider.NAME);
                    if (verdict.isAllowed() == false) {
                        record(byDecider, verdict);
                        continue;
                    }

                    // The target has to be somewhere a copy could legally be created, judged by exactly the
                    // rules that judge any other placement.
                    Allocator.Outcome placement = legalTarget(
                        observation, copy, index, target, awarenessAttributes, maxCopiesPerNode
                    );
                    if (placement.placed() == false) {
                        byDecider.putIfAbsent("allocation", "the lightest nodes are not legal homes for the"
                            + " heaviest copies: " + placement.refusal().because());
                        continue;
                    }

                    return new Outcome(new Move(
                        copy.shard(), copy.role(), source, target, copy.weight(), spread, context.cost(),
                        because(context, verdict)
                    ), null);
                }
            }
        }

        return new Outcome(null, new Refusal(spread, summarise(spread, byDecider, anyMoveWouldHelp), byDecider));
    }

    private Decision.Combined decide(RebalanceContext context) {
        List<Decision> decisions = new ArrayList<>(deciders.size());
        Decision.Type worst = Decision.Type.YES;
        for (RebalanceDecider decider : deciders) {
            Decision decision = decider.canRebalance(context);
            decisions.add(decision);
            if (decision.type().ordinal() > worst.ordinal()) {
                worst = decision.type();
            }
        }
        // Every decider runs even after one has refused, for the same reason as in allocation: a move blocked
        // for three reasons is worth knowing about, because fixing only the first will not unblock it.
        return new Decision.Combined(worst, decisions);
    }

    /**
     * Whether a copy of this shard could be allocated to the target, using the ordinary allocation rules.
     *
     * <p>The candidate list is the target alone. Asking "where would you put this?" would let the allocator
     * answer with some third node, which is not the question: the rebalancer has already chosen the target on
     * load grounds and needs to know whether that specific choice is legal.
     */
    private Allocator.Outcome legalTarget(
        Observation observation,
        LoadModel.Held copy,
        IndexConfig index,
        NodeId target,
        List<String> awarenessAttributes,
        Integer maxCopiesPerNode
    ) {
        if (index == null) {
            return new Allocator.Outcome(null, new Allocator.Refusal(Map.of(), false,
                "this copy belongs to an index that is no longer in the tenant's desired state, so there is no"
                    + " configuration to allocate it against; it is surplus rather than misplaced"));
        }
        AllocationContext context = new AllocationContext(
            copy.shard().tenant(), index, copy.shard().shard(), copy.role(), observation,
            awarenessAttributes, maxCopiesPerNode, List.of()
        );
        return allocator.choose(context, List.of(target));
    }

    private String because(RebalanceContext context, Decision.Combined verdict) {
        return "moving " + context.shard() + " off [" + context.source() + "] (load "
            + String.format(java.util.Locale.ROOT, "%.3f", context.sourceLoad()) + ") onto ["
            + context.target() + "] (load " + String.format(java.util.Locale.ROOT, "%.3f", context.targetLoad())
            + ") evens out the pool; " + context.cost().because();
    }

    private void record(Map<String, String> byDecider, Decision.Combined verdict) {
        for (Decision decision : verdict.decisions()) {
            if (decision.type() != Decision.Type.YES) {
                byDecider.putIfAbsent(decision.decider(), decision.because());
            }
        }
    }

    private static boolean saidYes(Decision.Combined verdict, String decider) {
        return verdict.decisions().stream()
            .anyMatch(decision -> decision.decider().equals(decider) && decision.type() == Decision.Type.YES);
    }

    /**
     * One sentence for the operator asking why their pool is uneven.
     *
     * <p>Ordered by which answer is most actionable: a setting they can change, then work that is competing,
     * then pacing, then the honest "this is as good as it gets".
     *
     * <p>Except that the honest answer comes <b>first</b> when it applies. If not one candidate move would have
     * reduced the spread, then the pool is balanced and every other refusal is beside the point — an evenly
     * loaded pool full of primaries would otherwise be reported as blocked on the primary-move gap, sending an
     * operator to chase a limitation that is costing them nothing at all.
     */
    private String summarise(double spread, Map<String, String> byDecider, boolean anyMoveWouldHelp) {
        if (anyMoveWouldHelp == false) {
            String detail = byDecider.get(MoveThresholdDecider.NAME);
            return String.format(java.util.Locale.ROOT, "load spread across this pool is %.3f, and no single"
                + " move would reduce it", spread) + (detail == null ? "" : ": " + detail);
        }
        for (String decider : List.of(
            RebalanceEnabledDecider.NAME,
            RecoveryPriorityDecider.NAME,
            ConcurrentRebalanceDecider.NAME,
            "allocation",
            PrimaryMoveDecider.NAME,
            "copy_state",
            MoveThresholdDecider.NAME
        )) {
            String because = byDecider.get(decider);
            if (because != null) {
                return String.format(java.util.Locale.ROOT, "load spread across this pool is %.3f; no move is"
                    + " being made because ", spread) + because;
            }
        }
        return String.format(java.util.Locale.ROOT, "load spread across this pool is %.3f, and no single move"
            + " would reduce it", spread);
    }

    private static IndexConfig indexOf(Observation observation, ShardRef ref) {
        Observation.DesiredState desired = observation.tenants().get(ref.tenant());
        return desired == null ? null : desired.indicesByUuid().get(ref.indexUuid());
    }
}
