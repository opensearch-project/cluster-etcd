package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.observe.Observation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Runs every decider over every candidate and picks a winner, or explains why there is none.
 *
 * <p>Two things it is careful about, and both are about being answerable rather than clever.
 *
 * <p><b>It always keeps the reasons.</b> "Shard 3 is unassigned" is the most common unanswerable question an
 * operator has about a cluster; "shard 3 is unassigned because every node in zone z1 is above the disk low
 * watermark and awareness will not put a third copy in z2" is a different kind of morning. So a failure to
 * place returns the full set of per-node verdicts, and the finding that results says which rule refused.
 *
 * <p><b>It distinguishes "not yet" from "never".</b> If every candidate was throttled the answer is that the
 * work is paced, not blocked, and the diff should not escalate. If any candidate said no for a structural
 * reason, that is worth an operator's attention.
 */
public final class Allocator {

    /** Where a copy should go, and the decisions that allowed it. */
    public record Placement(NodeId node, Decision.Combined decision, String because) {}

    /** Why nothing could be placed, with every candidate's verdict kept for the explanation. */
    public record Refusal(Map<NodeId, Decision.Combined> verdicts, boolean throttledOnly, String because) {

        public Refusal {
            verdicts = Map.copyOf(verdicts);
        }
    }

    /** Either a placement or a refusal — never both, and never neither. */
    public record Outcome(Placement placement, Refusal refusal) {

        public boolean placed() {
            return placement != null;
        }

        public Optional<NodeId> node() {
            return placement == null ? Optional.empty() : Optional.of(placement.node());
        }
    }

    /** The default set, in the order their explanations read best. */
    public static List<AllocationDecider> defaultDeciders() {
        return List.of(
            // First, so that a refusal an operator caused reads before every refusal the cluster caused.
            new EnableAllocationDecider(),
            new NodeRoleDecider(),
            new ReplicaAfterPrimaryDecider(),
            new SameShardDecider(),
            new AwarenessDecider(),
            new DiskThresholdDecider(),
            new ShardsLimitDecider(),
            new ThrottlingDecider()
        );
    }

    private final List<AllocationDecider> deciders;

    public Allocator() {
        this(defaultDeciders());
    }

    public Allocator(List<AllocationDecider> deciders) {
        this.deciders = List.copyOf(Objects.requireNonNull(deciders, "deciders"));
        if (this.deciders.isEmpty()) {
            throw new IllegalArgumentException(
                "an allocator with no deciders would place anything anywhere; pass defaultDeciders() to mean"
                    + " the usual set"
            );
        }
    }

    public Outcome choose(AllocationContext context, Iterable<NodeId> candidates) {
        Map<NodeId, Decision.Combined> verdicts = new LinkedHashMap<>();
        List<NodeId> allowed = new ArrayList<>();

        for (NodeId candidate : candidates) {
            Decision.Combined combined = decide(context, candidate);
            verdicts.put(candidate, combined);
            if (combined.isAllowed()) {
                allowed.add(candidate);
            }
        }

        if (allowed.isEmpty()) {
            boolean throttledOnly = verdicts.isEmpty() == false
                && verdicts.values().stream().allMatch(verdict -> verdict.type() == Decision.Type.THROTTLE);
            return new Outcome(null, new Refusal(verdicts, throttledOnly, refusalBecause(verdicts, throttledOnly)));
        }

        NodeId chosen = best(context, allowed);
        Decision.Combined decision = verdicts.get(chosen);
        return new Outcome(
            new Placement(chosen, decision, placementBecause(context, chosen, allowed.size())),
            null
        );
    }

    private Decision.Combined decide(AllocationContext context, NodeId candidate) {
        List<Decision> decisions = new ArrayList<>(deciders.size());
        Decision.Type worst = Decision.Type.YES;

        for (AllocationDecider decider : deciders) {
            Decision decision = decider.canAllocate(context, candidate);
            decisions.add(decision);
            if (decision.type().ordinal() > worst.ordinal()) {
                worst = decision.type();
            }
        }
        // Every decider runs even once one has said no, because a candidate rejected for three reasons is
        // worth knowing about: fixing only the first would not help, and an operator who cannot see the other
        // two will try.
        return new Decision.Combined(worst, decisions);
    }

    /**
     * Among nodes that are all allowed, prefer the one holding fewest copies, then the one with most free disk.
     *
     * <p>Shard count before disk on purpose. Disk is what the deciders already guard, and balancing on it alone
     * concentrates shards on whichever machine happens to have the largest volume — which is exactly how a
     * cluster ends up with one node holding most of its shards and plenty of space left. Count is the better
     * proxy for the costs that are not bytes.
     *
     * <p>The node id breaks ties, so two supervisors computing the same plan from the same observation choose
     * the same node. Not cosmetic: a non-deterministic tiebreak means two of them create two copies.
     */
    private NodeId best(AllocationContext context, List<NodeId> allowed) {
        return allowed.stream()
            .min(Comparator
                .comparingInt(context::copiesOn)
                .thenComparing(Comparator.comparingLong(
                    (NodeId node) -> DiskThresholdDecider.freeBytes(context, node)).reversed())
                .thenComparing(NodeId::value))
            .orElseThrow();
    }

    private String placementBecause(AllocationContext context, NodeId chosen, int candidates) {
        return "node [" + chosen + "] holds " + context.copiesOn(chosen) + " copies and has "
            + DiskThresholdDecider.freeBytes(context, chosen) / (1024 * 1024) + " MiB free, the least loaded of "
            + candidates + " node(s) every decider allowed";
    }

    /** Distinct refusal reasons shown before the rest are summarised as a count. */
    static final int MAX_DISTINCT_REASONS = 3;

    /**
     * A second bound, on characters, because the number of reasons is not the only thing that grows.
     *
     * <p>Each clause carries <em>every</em> rule that refused that candidate — deliberately, since fixing only
     * the first of three would not unblock anything — so one clause can be several sentences. Three of those is
     * comfortably inside this; a pathological pool is not, and this is what stops it. Whole clauses are dropped
     * rather than characters, because a reason cut off mid-sentence is worse than a reason replaced by a count.
     */
    static final int MAX_BECAUSE_LENGTH = 1200;

    private String refusalBecause(Map<NodeId, Decision.Combined> verdicts, boolean throttledOnly) {
        if (verdicts.isEmpty()) {
            return "there are no candidate nodes in this pool at all";
        }
        if (throttledOnly) {
            // Unchanged, deliberately. "Everything is throttled, so this is paced rather than blocked" is already
            // the whole answer, and grouping identical pacing messages would bury it in arithmetic.
            return "every one of the " + verdicts.size() + " candidate node(s) is throttled, so this is paced"
                + " rather than blocked: " + first(verdicts);
        }

        List<ReasonGroup> groups = groupByReason(verdicts);
        StringBuilder because = new StringBuilder("no node was allowed by every decider; ")
            .append(verdicts.size()).append(" candidate(s) refused for ").append(groups.size())
            .append(" distinct reason(s)");

        int shown = 0;
        for (int i = 0; i < Math.min(groups.size(), MAX_DISTINCT_REASONS); i++) {
            ReasonGroup group = groups.get(i);
            String clause = (i == 0 ? ": " : "; ") + "node [" + group.example() + "]: " + group.summary()
                + " (" + group.nodes() + (group.nodes() == 1 ? " node)" : " nodes)");
            // The first clause always goes in whatever its length: a refusal with no example at all would be
            // worse than a long one, and it is the most-affected reason by the ordering above.
            if (shown > 0 && because.length() + clause.length() > MAX_BECAUSE_LENGTH) {
                break;
            }
            because.append(clause);
            shown++;
        }
        if (groups.size() > shown) {
            // Bounded because this string is projected into the store and read by the control plane; a pool with
            // hundreds of nodes must not produce hundreds of clauses. Same discipline as the differ's truncation.
            because.append("; and ").append(groups.size() - shown).append(" further distinct reason(s) not shown");
        }
        return because.toString();
    }

    /**
     * One example per distinct refusal reason, most-affected first.
     *
     * <p>Showing an arbitrary single refusal wasted the whole point of having rules that explain themselves, and
     * the arbitrary choice was biased towards the useless one: the node legitimately holding the primary sorts
     * early and is always refused, so "that node already has a copy" hid the reason that was actually blocking
     * progress.
     *
     * <p>Ordering by the number of nodes affected is what makes this systematically useful rather than merely
     * complete. The reason blocking four of five candidates is far more likely to be the one worth acting on than
     * the one blocking a single node, and it is also the one that survives the cap. The node id breaks ties, so
     * two supervisors reading the same observation produce the same sentence.
     */
    private List<ReasonGroup> groupByReason(Map<NodeId, Decision.Combined> verdicts) {
        Map<List<String>, ReasonGroup> byReason = new LinkedHashMap<>();
        verdicts.forEach((node, verdict) -> {
            ReasonGroup existing = byReason.get(verdict.reasonKey());
            if (existing == null) {
                byReason.put(verdict.reasonKey(), new ReasonGroup(node, verdict.summary(), 1));
            } else {
                byReason.put(verdict.reasonKey(), existing.andAnother());
            }
        });
        return byReason.values().stream()
            .sorted(Comparator.comparingInt(ReasonGroup::nodes).reversed()
                .thenComparing(group -> group.example().value()))
            .toList();
    }

    /** @param example one node refused this way, whose full text illustrates the group */
    private record ReasonGroup(NodeId example, String summary, int nodes) {
        ReasonGroup andAnother() {
            return new ReasonGroup(example, summary, nodes + 1);
        }
    }

    private String first(Map<NodeId, Decision.Combined> verdicts) {
        return verdicts.entrySet()
            .stream()
            .findFirst()
            .map(entry -> "node [" + entry.getKey() + "]: " + entry.getValue().summary())
            .orElse("no detail");
    }

    /** Candidate nodes: everything in the pool that could conceivably take a copy, before any rule runs. */
    public static List<NodeId> candidatesOf(Observation observation) {
        return observation.placeableNodes().keySet().stream().sorted(Comparator.comparing(NodeId::value)).toList();
    }
}
