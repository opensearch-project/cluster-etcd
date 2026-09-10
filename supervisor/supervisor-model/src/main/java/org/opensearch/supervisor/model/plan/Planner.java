package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.diff.Diff;
import org.opensearch.supervisor.model.diff.Finding;
import org.opensearch.supervisor.model.diff.FindingKind;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Heartbeat;
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
 * <p><b>It acts on exactly one finding.</b> {@code UNASSIGNED_PRIMARY}, and only where the shard has no copy
 * and no pending assignment anywhere in the pool — the narrowest useful case, which
 * {@code REQUIREMENTS.md} §10 names as the first thing worth acting on. Everything else in the diff stays
 * observation. Narrowing it this far is what makes the first acting milestone safe: the only thing it can do
 * is create a copy that does not exist, and the worst outcome of a wrong decision is a surplus copy, which
 * is a self-clearing finding rather than lost data.
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
        List<Finding> actionable = diff.of(FindingKind.UNASSIGNED_PRIMARY)
            .stream()
            .filter(finding -> finding.passes() >= policy.minimumPasses())
            .sorted(Finding.BY_INTEREST)
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

        // Copies planned this pass, so two instructions in one plan cannot both target the same shard, and
        // so a node's projected load includes what it is about to be given.
        Set<String> planned = new LinkedHashSet<>();
        Map<NodeId, Integer> extraCopies = new LinkedHashMap<>();

        Map<TenantId, Integer> perTenant = new LinkedHashMap<>();
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
            if (planned.add(tenant.value() + "/" + indexUuid + "/" + shard) == false) {
                continue;
            }
            if (alreadyPlacedOrAssigned(observation, tenant, indexUuid, shard)) {
                // Level-triggered, and this is the check that makes re-placement impossible without any
                // memory of what was issued last pass: a shard already named in some node's assignment is
                // being worked on, even though no node reports holding it yet.
                continue;
            }

            Optional<NodeId> target = choose(placeable, extraCopies);
            if (target.isEmpty()) {
                continue;
            }

            NodeId node = target.get();
            extraCopies.merge(node, 1, Integer::sum);
            perTenant.merge(tenant, 1, Integer::sum);
            instructions.add(new Instruction.PlaceCopy(
                tenant,
                indexUuid,
                index.name(),
                shard,
                ShardRole.PRIMARY,
                node,
                index.configRevision(),
                because(finding, node, placeable.get(node))
            ));
        }

        return new Plan(observation.pass(), observation.observedAt(), observation.pool(), epoch,
            instructions, ceilingHit);
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
     * Whether anything in the pool already holds or has been told to hold this shard.
     *
     * <p>The second half is what matters. A node that has just been given an assignment reports nothing for
     * a pass or two, so a planner that looked only at inventories would place the same shard again, on a
     * different node each time, and produce a surplus copy per pass. Reading its own previous instruction
     * out of the store rather than remembering it keeps the planner a pure function of observed state.
     */
    private static boolean alreadyPlacedOrAssigned(
        Observation observation,
        TenantId tenant,
        String indexUuid,
        int shard
    ) {
        for (Observation.NodeObservation node : observation.nodes().values()) {
            // A node given up for dead contributes nothing -- neither the copies it reports nor the
            // assignment it was given. Counting a lost node's assignment was a real bug: its stale
            // assignment named the shard for ever, so the planner believed the shard was being dealt with
            // and never rebuilt it anywhere. Node loss would never have recovered, which is the one case
            // this milestone exists to handle. A STALE node still counts, because stale is a hiccup.
            if (node.copiesCount() == false) {
                continue;
            }
            if (node.inventory() != null) {
                var tenantShards = node.inventory().tenants().get(tenant);
                if (tenantShards != null) {
                    var index = tenantShards.indices().get(indexUuid);
                    if (index != null && index.shards().containsKey(shard)) {
                        return true;
                    }
                }
            }
            if (node.isAssigned(tenant, indexUuid, shard)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Which node to place on.
     *
     * <p><b>Deliberately the simplest thing that is defensible</b>, and not the allocation policy this
     * service will eventually need. Most free disk wins, counting what has already been planned this pass,
     * with the node id as a tiebreak so the answer does not depend on iteration order.
     *
     * <p>What is missing, and is {@code DESIGN.md} open question 7 rather than an oversight: awareness
     * attributes, disk watermarks, shard-count balance, and any notion of not putting every copy of
     * everything on the emptiest machine. A real allocator is its own design; this one exists so that
     * milestone 5 can act at all, and the diff will say plainly when it has chosen badly.
     */
    private static Optional<NodeId> choose(
        Map<NodeId, Observation.NodeObservation> placeable,
        Map<NodeId, Integer> extraCopies
    ) {
        return placeable.entrySet()
            .stream()
            .max(Comparator
                .comparingLong((Map.Entry<NodeId, Observation.NodeObservation> entry) -> freeBytes(entry.getValue()))
                .thenComparing(entry -> -extraCopies.getOrDefault(entry.getKey(), 0))
                .thenComparing(entry -> entry.getKey().value(), Comparator.reverseOrder()))
            .map(Map.Entry::getKey);
    }

    private static long freeBytes(Observation.NodeObservation node) {
        Heartbeat heartbeat = node.heartbeat();
        if (heartbeat == null || heartbeat.capacity() == null) {
            return 0;
        }
        return heartbeat.capacity().diskAvailableBytes();
    }

    private static String because(Finding finding, NodeId node, Observation.NodeObservation chosen) {
        return "placing a primary on [" + node + "] because " + finding.because()
            + "; that node is live, in this pool, and has " + freeBytes(chosen) / (1024 * 1024)
            + " MiB free, which is the most of any placeable node";
    }
}
