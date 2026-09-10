package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.allocate.AllocationContext;
import org.opensearch.supervisor.model.allocate.Allocator;
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
 * <p><b>It still only ever adds.</b> Nothing removes a copy, so the worst outcome of a wrong decision remains
 * a surplus copy — a self-clearing finding rather than lost data. What has changed is that it now places
 * replicas as well as primaries, and that <em>where</em> a copy goes is decided by {@link Allocator} rather
 * than by this class.
 *
 * <p>Those two went together rather than in sequence: the rules that matter most — never two copies of a shard
 * on one node, spread copies across zones — are <b>meaningless with one copy per shard</b>. An allocator
 * without replicas to place would have been rules nothing exercised, and replicas without an allocator would
 * have stacked every copy on the emptiest machine.
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
    public record Policy(int workCeiling, int perTenantFloor, long minimumPasses) {
        public static Policy defaults() {
            return new Policy(200, 5, 2);
        }
    }

    private Planner() {}

    public static Plan plan(Observation observation, Diff diff, long epoch, Policy policy) {
        return plan(observation, diff, epoch, policy, new Allocator(), List.of(), null);
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
        Integer maxCopiesPerNode
    ) {
        // Primaries before replicas, always. The ReplicaAfterPrimaryDecider makes a replica placed too early
        // wait rather than fail, but ordering the work means it does not have to: a pass that gets to the
        // primary first converges in one round instead of two.
        List<Finding> actionable = new ArrayList<>();
        actionable.addAll(diff.of(FindingKind.UNASSIGNED_PRIMARY));
        actionable.addAll(diff.of(FindingKind.UNASSIGNED_REPLICA));
        actionable = actionable.stream()
            .filter(finding -> finding.passes() >= policy.minimumPasses())
            .toList();

        if (actionable.isEmpty()) {
            return Plan.empty(observation.pass(), observation.observedAt(), observation.pool(), epoch);
        }

        Map<NodeId, Observation.NodeObservation> placeable = observation.placeableNodes();
        if (placeable.isEmpty()) {
            // Nothing to be done, and the diff already says so with a reason. Producing instructions that
            // cannot be carried out would turn a visible problem into a busy loop.
            return Plan.empty(observation.pass(), observation.observedAt(), observation.pool(), epoch);
        }

        // Copies planned this pass, carried into every later decision so that two placements in one plan
        // cannot both land on the same node or both fill the same zone.
        List<AllocationContext.PlannedCopy> planned = new ArrayList<>();
        List<NodeId> candidates = Allocator.candidatesOf(observation);

        Map<TenantId, Integer> perTenant = new LinkedHashMap<>();
        Map<String, String> refusals = new LinkedHashMap<>();
        List<Instruction> instructions = new ArrayList<>();
        boolean ceilingHit = false;

        for (Finding finding : actionable) {
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

            ShardRole role = finding.kind() == FindingKind.UNASSIGNED_PRIMARY
                ? ShardRole.PRIMARY
                : replicaRoleOf(index);

            // How many copies of this shard already exist or are on their way. The allocator's same-shard rule
            // stops a duplicate landing on one node; this stops planning a copy the shard does not want.
            AllocationContext context = new AllocationContext(
                tenant, index, shard, role, observation, awarenessAttributes, maxCopiesPerNode, planned
            );
            if (context.nodesWithThisShard().size() >= index.copiesPerShard()) {
                continue;
            }

            Allocator.Outcome outcome = allocator.choose(context, candidates);
            if (outcome.placed() == false) {
                // Not a finding of its own -- the diff already says the shard is unassigned -- but the reason
                // is kept and projected alongside that finding. An earlier version computed this sentence and
                // dropped it, which threw away the entire benefit of having rules that can explain themselves.
                refusals.put(finding.subject().key(), outcome.refusal().because());
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

        return new Plan(observation.pass(), observation.observedAt(), observation.pool(), epoch,
            instructions, ceilingHit, refusals);
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
     * What a replica of this index is called.
     *
     * <p>Segment replication calls its read-only copies search replicas and document replication calls them
     * replicas, and the difference is not cosmetic: a node given the wrong one either waits for a primary that
     * will never contact it or contacts a primary that is not expecting it.
     */
    private static ShardRole replicaRoleOf(IndexConfig index) {
        return index.replication() == org.opensearch.supervisor.model.status.Replication.SEGMENT
            ? ShardRole.SEARCH_REPLICA
            : ShardRole.REPLICA;
    }

}
