package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.allocate.AllocationContext;
import org.opensearch.supervisor.model.allocate.Allocator;
import org.opensearch.supervisor.model.balance.LoadModel;
import org.opensearch.supervisor.model.balance.RecoveryPriorityDecider;
import org.opensearch.supervisor.model.balance.Rebalancer;
import org.opensearch.supervisor.model.balance.SurplusReaper;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Finding;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.observe.Observation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Turns a diff into instructions. A pure function, like the differ, and for the same reason: this is where a
 * mistake first becomes capable of changing the cluster, so it had better be the part that is easiest to
 * test.
 *
 * <p><em>Where</em> a copy goes is decided by {@link Allocator}, not here; which copies are surplus is decided
 * by {@link SurplusReaper}; and whether a settled pool is worth stirring is decided by {@link Rebalancer}. This
 * class decides only what work a pass is allowed to do and in what order.
 *
 * <p><b>The order is the interesting part, and it is a priority order rather than a pipeline.</b> Three kinds of
 * shard work compete for the same recovery bandwidth, and they are worth wildly different amounts:
 *
 * <ol>
 *   <li><b>Missing copies.</b> A shard short of the copies it wants is one failure from being unavailable.
 *       Everything else waits.
 *   <li><b>Surplus copies.</b> Cheap to fix, frees capacity, and is how a move finishes — so it comes before
 *       starting new ones.
 *   <li><b>Rebalancing.</b> Only when a pass has found nothing else to do at all.
 * </ol>
 *
 * <p>That last gate is stricter than it needs to be, and deliberately. {@link RecoveryPriorityDecider} already
 * refuses to rebalance while anything is under-replicated, so the rule is nearly redundant — but only nearly,
 * because the load model is built from the observation and cannot see the placements <em>this</em> pass has just
 * decided on. A pass that adds five copies and then asks "how loaded is everything?" gets an answer that is
 * already wrong by five copies, and a move chosen from it would be a move chosen from fiction. Waiting a pass
 * costs seconds and makes the figures real.
 */
public final class Planner {

    /**
     * @param workCeiling instructions per pass, across all tenants. R2.6: a pass over ten thousand indices
     *     must not be one giant write, and a large backlog should make progress rather than time out forever.
     * @param perTenantFloor instructions each tenant may have before the ceiling is shared out, so one large
     *     tenant in a shared pool cannot starve the others.
     * @param minimumPasses how long a finding must have held before it is acted on. Not a delay for its own
     *     sake: it is what stops the supervisor reacting to a torn read, and one pass of patience costs a
     *     couple of seconds where a wrong placement costs a recovery.
     */
    public record Policy(int workCeiling, int perTenantFloor, long minimumPasses, boolean rebalance) {

        public Policy(int workCeiling, int perTenantFloor, long minimumPasses) {
            this(workCeiling, perTenantFloor, minimumPasses, true);
        }

        public static Policy defaults() {
            return new Policy(200, 5, 2, true);
        }

        /** For a caller that wants placement only — chiefly tests that assert on an exact instruction list. */
        public Policy withoutRebalancing() {
            return new Policy(workCeiling, perTenantFloor, minimumPasses, false);
        }
    }

    private Planner() {}

    public static Plan plan(Observation observation, Diff diff, long epoch, Policy policy) {
        return plan(observation, diff, epoch, policy, new Allocator(), List.of(), null);
    }

    public static Plan plan(
        Observation observation,
        Diff diff,
        long epoch,
        Policy policy,
        Allocator allocator,
        List<String> awarenessAttributes,
        Integer maxCopiesPerNode
    ) {
        return plan(observation, diff, epoch, policy, allocator, awarenessAttributes, maxCopiesPerNode,
            new Rebalancer());
    }

    /**
     * @param awarenessAttributes attributes to spread a shard's copies across, from the pool definition
     * @param maxCopiesPerNode this pool's cap on copies per node, or {@code null} for none
     */
    public static Plan plan(
        Observation observation,
        Diff diff,
        long epoch,
        Policy policy,
        Allocator allocator,
        List<String> awarenessAttributes,
        Integer maxCopiesPerNode,
        Rebalancer rebalancer
    ) {
        return plan(observation, diff, epoch, policy, allocator, awarenessAttributes, maxCopiesPerNode,
            false, rebalancer);
    }

    /**
     * @param refuseCopiesSharingAHost this pool's host rule, from the pool definition. Off by default; see
     *     {@link org.opensearch.supervisor.model.allocate.SameShardDecider} for why an address is a poor proxy
     *     for a failure domain.
     */
    public static Plan plan(
        Observation observation,
        Diff diff,
        long epoch,
        Policy policy,
        Allocator allocator,
        List<String> awarenessAttributes,
        Integer maxCopiesPerNode,
        boolean refuseCopiesSharingAHost,
        Rebalancer rebalancer
    ) {
        // Primaries before replicas, always. The ReplicaAfterPrimaryDecider makes a replica placed too early
        // wait rather than fail, but ordering the work means it does not have to: a pass that gets to the
        // primary first converges in one round instead of two.
        List<Finding> actionable = new ArrayList<>();
        actionable.addAll(diff.of(FindingKind.UNASSIGNED_PRIMARY));
        actionable.addAll(diff.of(FindingKind.UNASSIGNED_REPLICA));
        // Last, because a search replica recovers from the remote store and so waits for nobody. Placing the
        // copies that *do* have an ordering constraint first costs nothing and saves a round trip.
        actionable.addAll(diff.of(FindingKind.UNASSIGNED_SEARCH_REPLICA));
        actionable = actionable.stream()
            .filter(finding -> finding.passes() >= policy.minimumPasses())
            .toList();

        // Not an early return even when there is nothing to place: a pool with every copy where it should be is
        // exactly the pool that has surplus copies to drop and moves worth making.
        boolean canPlace = observation.placeableNodes().isEmpty() == false;

        // Copies planned this pass, carried into every later decision so that two placements in one plan
        // cannot both land on the same node or both fill the same zone.
        List<AllocationContext.PlannedCopy> planned = new ArrayList<>();
        List<NodeId> candidates = Allocator.candidatesOf(observation);

        Map<TenantId, Integer> perTenant = new LinkedHashMap<>();
        Map<String, String> refusals = new LinkedHashMap<>();
        List<Instruction> instructions = new ArrayList<>();
        boolean ceilingHit = false;

        for (Finding finding : canPlace ? actionable : List.<Finding>of()) {
            if (instructions.size() >= policy.workCeiling()) {
                ceilingHit = true;
                break;
            }
            TenantId tenant = finding.subject().tenant();
            Integer shard = finding.subject().shard();
            String indexUuid = finding.subject().indexUuid();
            if (tenant == null || shard == null || indexUuid == null) {
                continue;
            }

            int alreadyForTenant = perTenant.getOrDefault(tenant, 0);
            if (alreadyForTenant >= fairSliceOf(policy, observation.tenants().size())) {
                // Not the ceiling, just this tenant's slice of it. Another tenant gets the rest of the pass.
                ceilingHit = true;
                continue;
            }

            IndexConfig index = indexOf(observation, tenant, indexUuid);
            if (index == null || index.open() == false) {
                continue;
            }

            ShardRole role = roleWantedBy(finding.kind());

            if (role == ShardRole.PRIMARY) {
                // A primary needs a term, and a term needs a counter that can be read: placing one without knowing
                // which terms have been issued is how a term gets issued twice. So this index takes no new primary
                // until the counter reads again, and says why.
                var counter = observation.termCounter(tenant, indexUuid);
                if (counter.isPresent() && counter.get().readable() == false) {
                    refusals.put(finding.refusalKey(), "the primary-term counter for this index cannot be read, and"
                        + " a primary placed without it could be told a term already issued; nothing places a primary"
                        + " for this index until [" + org.opensearch.supervisor.model.Keys.primaryTerms(tenant, indexUuid)
                        + "] reads again");
                    continue;
                }

                Promoter.Outcome promotion = Promoter.propose(observation, tenant, index, shard);
                if (promotion.refusing()) {
                    refusals.put(finding.refusalKey(), promotion.refusal());
                    continue;
                }
                if (promotion.promoting()) {
                    NodeId node = promotion.promote();
                    planned.add(new AllocationContext.PlannedCopy(tenant, indexUuid, shard, ShardRole.PRIMARY, node));
                    perTenant.merge(tenant, 1, Integer::sum);
                    instructions.add(new Instruction.PlaceCopy(
                        tenant, indexUuid, index.name(), shard, ShardRole.PRIMARY, node, index.configRevision(),
                        finding.because() + "; promoting the started replica on [" + node + "] in place, with a new"
                            + " primary term, because under document replication that replica is the data"
                    ));
                    continue;
                }
            }

            // How many copies of this shard already exist or are on their way. The allocator's same-shard rule
            // stops a duplicate landing on one node; this stops planning a copy the shard does not want.
            AllocationContext context = new AllocationContext(
                tenant, index, shard, role, observation, awarenessAttributes, maxCopiesPerNode,
                refuseCopiesSharingAHost, planned
            );
            // Two ceilings, and both are needed. The per-role one is what the finding is actually about; the
            // total is a safety net that also covers the per-shard authorised count, which is not split by role.
            if (context.copiesOfThisShardInRole(role) >= index.desiredCopiesOf(role)) {
                continue;
            }
            if (context.nodesWithThisShard().size() >= index.copiesAuthorisedFor(shard)) {
                continue;
            }

            Allocator.Outcome outcome = allocator.choose(context, candidates);
            if (outcome.placed() == false) {
                // A primary with nowhere to go gets one more question asked on its behalf: is a read-only copy
                // the only thing in the way? On a pool worn down to one node that is a permanent deadlock rather
                // than a wait, and it is the one case where this service gives up a copy that is currently
                // serving. See Displacer -- including why it only removes, and lets a later pass place.
                Displacer.Outcome displacement = Displacer.propose(context, allocator, candidates);
                if (displacement.displacing()) {
                    Displacer.Displacement giveUp = displacement.displacement();
                    instructions.add(new Instruction.RemoveCopy(
                        tenant, indexUuid, shard, giveUp.node(), giveUp.because()
                    ));
                    refusals.put(finding.refusalKey(),
                        outcome.refusal().because() + "; " + giveUp.because());
                    continue;
                }

                // Not a finding of its own -- the diff already says the shard is unassigned -- but the reason
                // is kept and projected alongside that finding. An earlier version computed this sentence and
                // dropped it, which threw away the entire benefit of having rules that can explain themselves.
                refusals.put(finding.refusalKey(), displacement.because() == null
                    ? outcome.refusal().because()
                    : outcome.refusal().because() + "; " + displacement.because());
                continue;
            }

            NodeId node = outcome.placement().node();
            planned.add(new AllocationContext.PlannedCopy(tenant, indexUuid, shard, role, node));
            perTenant.merge(tenant, 1, Integer::sum);
            instructions.add(new Instruction.PlaceCopy(
                tenant,
                indexUuid,
                index.name(),
                shard,
                role,
                node,
                index.configRevision(),
                finding.because() + "; " + outcome.placement().because()
            ));
        }

        // Orphans before surpluses, of the two kinds of removal. A surplus copy is merely waste, and waiting a
        // pass costs disk; an orphan is holding up its index's deletion task and stopping its node from ever
        // reporting an assignment as applied, so it blocks other work rather than just occupying space.
        OrphanReaper.Outcome orphans = OrphanReaper.reap(observation, diff, policy.minimumPasses());
        refusals.putAll(orphans.refusals());
        for (OrphanReaper.Removal removal : orphans.removals()) {
            if (instructions.size() >= policy.workCeiling()) {
                ceilingHit = true;
                break;
            }
            instructions.add(new Instruction.RemoveCopy(
                removal.tenant(), removal.indexUuid(), removal.shard(), removal.node(), removal.because()
            ));
        }

        // Surplus copies next, computed from observed state alone. This is also how every move finishes; see
        // SurplusReaper for why the two halves are deliberately unaware of each other.
        LoadModel loads = LoadModel.of(observation);
        for (SurplusReaper.Removal removal : SurplusReaper.reap(observation, loads)) {
            if (instructions.size() >= policy.workCeiling()) {
                ceilingHit = true;
                break;
            }
            instructions.add(new Instruction.RemoveCopy(
                removal.shard().tenant(), removal.shard().indexUuid(), removal.shard().shard(),
                removal.node(), removal.because()
            ));
        }

        // And a move, only if this pass found nothing else at all to do.
        Plan.Balance balance = null;
        if (policy.rebalance() && instructions.isEmpty() && canPlace) {
            Rebalancer.Outcome outcome = rebalancer.propose(
                observation, loads, awarenessAttributes, maxCopiesPerNode, refuseCopiesSharingAHost
            );
            if (outcome.moving()) {
                Rebalancer.Move move = outcome.move();
                IndexConfig index = indexOf(observation, move.shard().tenant(), move.shard().indexUuid());
                if (index != null) {
                    instructions.add(new Instruction.PlaceCopy(
                        move.shard().tenant(), move.shard().indexUuid(), index.name(), move.shard().shard(),
                        move.role(), move.target(), index.configRevision(), move.because()
                    ));
                    balance = new Plan.Balance(move.spreadBefore(), move.because());
                }
            } else {
                balance = new Plan.Balance(outcome.refusal().spread(), outcome.refusal().because());
            }
        } else if (policy.rebalance()) {
            balance = new Plan.Balance(loads.spread(), "not looking for moves this pass: " + instructions.size()
                + " shard change(s) are already under way, and load measured before those land would not"
                + " describe the pool a move would be landing in");
        }

        return new Plan(observation.pass(), observation.observedAt(), observation.pool(), epoch,
            instructions, ceilingHit, refusals, balance);
    }

    /**
     * Instructions one tenant may take from a pass.
     *
     * <p>The floor is what stops a ten-thousand-index tenant consuming an entire pass in a shared pool while
     * a small tenant next to it waits for ever. Above the floor the ceiling is shared evenly, which is crude
     * and is enough: fairness here only decides who makes progress first, never whether they make it.
     */
    private static int fairSliceOf(Policy policy, int tenants) {
        if (tenants <= 1) {
            return policy.workCeiling();
        }
        int even = policy.workCeiling() / tenants;
        return Math.max(policy.perTenantFloor(), even);
    }

    private static IndexConfig indexOf(Observation observation, TenantId tenant, String indexUuid) {
        Observation.DesiredState desired = observation.tenants().get(tenant);
        return desired == null ? null : desired.indicesByUuid().get(indexUuid);
    }

    /**
     * The role a finding is asking for.
     *
     * <p>Read from the kind rather than derived from the index, which is where it used to come from. The mapping
     * of settings onto roles now lives in {@link IndexConfig#desiredCopiesOf} — one place, with the reasoning —
     * and the differ has already applied it by the time a finding exists. Deriving it a second time here is how
     * the two would eventually disagree.
     */
    private static ShardRole roleWantedBy(FindingKind kind) {
        return switch (kind) {
            case UNASSIGNED_PRIMARY -> ShardRole.PRIMARY;
            case UNASSIGNED_SEARCH_REPLICA -> ShardRole.SEARCH_REPLICA;
            case UNASSIGNED_REPLICA -> ShardRole.REPLICA;
            default -> throw new IllegalArgumentException(
                "finding kind " + kind + " does not ask for a shard copy; only the three unassigned kinds are"
                    + " actionable here, and adding a fourth means deciding what role it wants");
        };
    }

}
