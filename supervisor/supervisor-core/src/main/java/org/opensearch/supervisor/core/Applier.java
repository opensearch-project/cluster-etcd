package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.store.RevisionConflictException;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.NodeRole;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.plan.Instruction;
import org.opensearch.supervisor.model.plan.Plan;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 * Writes a plan into the store — the only place in this service that changes anything a data node will act
 * on, and therefore the only place where being wrong costs more than a pass.
 *
 * <p>{@code DESIGN.md} §12 works through the failure that actually happens: a supervisor observes, pauses,
 * loses its lease, and wakes holding a plan computed from state that is now old. Three defences, in order,
 * and all three live here:
 *
 * <ol>
 *   <li><b>The observation must be fresh.</b> A plan older than a bound is discarded. This catches almost
 *       everything and needs no coordination — and it fires for an ordinary GC pause too, not only for a
 *       lost lease.
 *   <li><b>The lease is re-checked immediately before writing</b>, narrowing the window to the write itself.
 *   <li><b>Every write is a compare-and-set on the revision read during observation.</b> If anyone has
 *       written that key since, the write fails and is dropped; the next pass recomputes.
 * </ol>
 *
 * <p>And where all three miss, the write is harmless, because reconciliation is level-triggered: the new
 * holder computes from the same observed state and either agrees, in which case the work was done for it, or
 * disagrees and overwrites next pass. That argument does <b>not</b> extend to non-idempotent side effects,
 * and there are none here yet — snapshot repository work will need real fencing rather than this reasoning.
 */
public final class Applier {

    /** A plan older than this is thrown away rather than applied. */
    public static final Duration DEFAULT_MAX_PLAN_AGE = Duration.ofSeconds(5);

    /** What happened, for the convergence document and for tests. */
    public record Outcome(
        int applied,
        int rejected,
        int skipped,
        boolean abandoned,
        String abandonedBecause
    ) {
        public static Outcome abandoned(String because) {
            return new Outcome(0, 0, 0, true, because);
        }

        public boolean didAnything() {
            return applied > 0;
        }
    }

    private final KeyValueStore store;
    private final Clock clock;
    private final Duration maxPlanAge;

    public Applier(KeyValueStore store, Clock clock) {
        this(store, clock, DEFAULT_MAX_PLAN_AGE);
    }

    public Applier(KeyValueStore store, Clock clock, Duration maxPlanAge) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.maxPlanAge = Objects.requireNonNull(maxPlanAge, "maxPlanAge");
    }

    /**
     * @param stillHoldsLease re-checked here rather than passed as a boolean, so that the check happens as
     *     late as possible instead of at whatever moment the caller happened to evaluate it
     */
    public Outcome apply(Plan plan, Observation observation, BooleanSupplier stillHoldsLease) {
        // No early return for an empty plan, which it used to have. An assignment carries two different kinds of
        // statement: what to place, which comes from the plan, and which tenants to coordinate for, which comes
        // from membership and has no plan behind it. Skipping the whole method when nothing is being placed meant
        // a pool with no shard work never told its coordinators anything -- and a coordinator's whole job is
        // exactly the case where nothing is being placed. The unchanged() guard below is what keeps this from
        // becoming a write per node per pass.
        Duration age = Duration.between(plan.observedAt(), clock.instant());
        if (age.compareTo(maxPlanAge) > 0) {
            return Outcome.abandoned("plan is " + age.toMillis() + "ms old, over the " + maxPlanAge.toMillis()
                + "ms bound; recomputing is cheaper than reasoning about whether it is still right");
        }

        if (stillHoldsLease.getAsBoolean() == false) {
            return Outcome.abandoned("no longer holding this pool's lease");
        }

        int applied = 0;
        int rejected = 0;
        int skipped = 0;

        // Plan-touched nodes first, then every other node whose coordinator entries are wrong. The second set is
        // usually empty; when it is not, nothing in it consumes the plan's work ceiling. A pass that spent its
        // budget placing shards should still be able to tell a new coordinator what it serves, because nothing is
        // moving and the alternative is a coordinator that waits for a quiet pass to learn its own job.
        Map<NodeId, List<Instruction>> byNode = new LinkedHashMap<>(plan.byNode());
        for (NodeId node : observation.placeableNodes().keySet()) {
            byNode.putIfAbsent(node, List.of());
        }

        for (var entry : byNode.entrySet()) {
            NodeId node = entry.getKey();
            Observation.NodeObservation observed = observation.nodes().get(node);
            if (observed == null) {
                skipped++;
                continue;
            }

            // Re-checked per node rather than once for the batch. A lease lost halfway through a plan should
            // stop the rest of it, not carry on because the check passed before the first write.
            if (stillHoldsLease.getAsBoolean() == false) {
                return new Outcome(applied, rejected, skipped, true, "lease lost partway through the plan");
            }

            Assignment next = merge(observed, entry.getValue(), plan, observation);

            // Nothing to say. Worth checking explicitly now that every placeable node is considered every pass:
            // writing an identical assignment would bump its version and revision, which the node reads as a
            // change and the liveness tracker reads as activity. A no-op write is not free, it is noise.
            if (unchanged(observed.assignment(), next)) {
                continue;
            }

            try {
                store.put(Keys.nodeAssignment(node), NodeJson.write(next), observed.assignmentRevision());
                applied += Math.max(entry.getValue().size(), 1);
            } catch (RevisionConflictException e) {
                // Somebody wrote this node's assignment since it was observed. Dropping the write is the
                // mechanism working, not a failure: whatever they wrote was based on a newer view than this.
                rejected += Math.max(entry.getValue().size(), 1);
            }
        }

        return new Outcome(applied, rejected, skipped, false, null);
    }

    /**
     * Builds the node's next assignment: everything it was already told, plus what this plan adds.
     *
     * <p>An assignment is a <b>complete desired set</b> — a copy it names and the node does not hold gets
     * created, and one the node holds that is absent gets removed. So a write that dropped the existing
     * contents would not merely fail to add a shard, it would tell the node to discard everything else it
     * has. Merging rather than replacing is the whole of this method's reason to exist.
     *
     * <p>That same property is what makes a removal expressible at all: dropping a shard from this map <em>is</em>
     * the instruction to stop serving it, and no new verb is needed on the node contract. It is also why a
     * removal that finds nothing to remove is a silent no-op rather than an error — the desired state it was
     * asking for is already the case, which is success under a level-triggered reconciler, and the usual cause
     * is simply that a previous pass got there first.
     */
    private Assignment merge(
        Observation.NodeObservation observed,
        List<Instruction> changes,
        Plan plan,
        Observation observation
    ) {
        Assignment existing = observed.assignment();
        Map<TenantId, Assignment.TenantAssignment> tenants = new LinkedHashMap<>(
            existing == null ? Map.of() : existing.tenants()
        );

        for (Instruction instruction : changes) {
            // Positive patterns rather than this codebase's usual `== false` guard: flow scoping extends a
            // pattern variable through `!` but not through `== false`, so the binding would not be in scope.
            if (instruction instanceof Instruction.PlaceCopy place) {
                Assignment.TenantAssignment tenant = tenants.get(place.tenant());
                Map<String, Assignment.IndexAssignment> indices = new LinkedHashMap<>(
                    tenant == null ? Map.of() : tenant.indices()
                );

                Assignment.IndexAssignment index = indices.get(place.indexUuid());
                Map<Integer, ShardRole> shards = new LinkedHashMap<>(index == null ? Map.of() : index.shards());
                shards.put(place.shard(), place.role());

                indices.put(place.indexUuid(), new Assignment.IndexAssignment(
                    place.indexName(), place.configRevision(), shards
                ));
                tenants.put(place.tenant(), tenant == null
                    ? Assignment.TenantAssignment.data(indices)
                    // Keeps whatever else this node does for the tenant -- coordinating for it, most likely.
                    : tenant.withIndices(indices).with(NodeRole.DATA));
            } else if (instruction instanceof Instruction.RemoveCopy remove) {
                Assignment.TenantAssignment tenant = tenants.get(remove.tenant());
                if (tenant == null) {
                    continue;
                }
                Assignment.IndexAssignment index = tenant.indices().get(remove.indexUuid());
                if (index == null) {
                    continue;
                }
                Map<Integer, ShardRole> shards = new LinkedHashMap<>(index.shards());
                if (shards.remove(remove.shard()) == null) {
                    continue;
                }

                Map<String, Assignment.IndexAssignment> indices = new LinkedHashMap<>(tenant.indices());
                if (shards.isEmpty()) {
                    // Prune the index rather than leaving an entry with no shards in it. Not cosmetic: an
                    // assignment is a complete statement of desired state, and an empty index entry says
                    // "this node is responsible for this index, holding nothing", which is a different claim
                    // from silence. It would also accumulate one dead entry per index a node ever held,
                    // against a document with a size budget.
                    indices.remove(remove.indexUuid());
                } else {
                    indices.put(remove.indexUuid(), new Assignment.IndexAssignment(
                        index.name(), index.configRevision(), shards
                    ));
                }

                if (indices.isEmpty()) {
                    // No data left for this tenant, so the DATA role goes -- and with it the whole entry unless
                    // the node also coordinates for the tenant, which is a job that holds no shards by design.
                    tenant.withIndices(indices).without(NodeRole.DATA)
                        .ifPresentOrElse(
                            remaining -> tenants.put(remove.tenant(), remaining),
                            () -> tenants.remove(remove.tenant())
                        );
                } else {
                    tenants.put(remove.tenant(), tenant.withIndices(indices));
                }
            }
        }

        applyCoordinatorMembership(observed, observation, tenants);

        long version = existing == null ? 1 : existing.version() + 1;
        return new Assignment(
            version, plan.pool(), plan.epoch(), observed.nodeId(), clock.millis(), tenants
        );
    }

    /**
     * Adds or removes the coordinator role, for every tenant bound to this pool.
     *
     * <p><b>Membership, not placement</b>, which is why it lives here rather than as an instruction. Every other
     * assignment this service writes is the end of a chain that starts with a finding: the diff says a copy is
     * missing, the planner places it, this writes it. A coordinator entry has no such chain, because there is no
     * shard to place and therefore nothing to find missing — and inventing a finding per (node, tenant) would fill
     * the diff with items that are never <em>wrong</em>. A coordinator without a tenant is not a deficiency; it is
     * a node waiting to be told what it is for.
     *
     * <p>Level-triggered like everything else, and <b>the removal half matters more than it looks</b>: a tenant
     * rebound to another pool must lose its coordinator entry, or a coordinator keeps routing queries to shards a
     * different supervisor now owns. That falls out of computing the desired set from the bound tenants rather than
     * tracking changes — a tenant that is no longer bound is simply not in it.
     */
    private void applyCoordinatorMembership(
        Observation.NodeObservation observed,
        Observation observation,
        Map<TenantId, Assignment.TenantAssignment> tenants
    ) {
        boolean coordinates = observed.registration() != null
            && observed.registration().roles().contains(NodeRole.COORDINATOR);
        Set<TenantId> bound = observation.tenants().keySet();

        if (coordinates) {
            for (TenantId tenant : bound) {
                Assignment.TenantAssignment existing = tenants.get(tenant);
                tenants.put(tenant, existing == null
                    ? Assignment.TenantAssignment.coordinator()
                    : existing.with(NodeRole.COORDINATOR));
            }
        }

        // Strip the role from anything it should not be on: every tenant if this node does not coordinate, and
        // tenants no longer bound to this pool even if it does.
        for (TenantId tenant : Set.copyOf(tenants.keySet())) {
            if (coordinates && bound.contains(tenant)) {
                continue;
            }
            Assignment.TenantAssignment entry = tenants.get(tenant);
            if (entry.coordinates() == false) {
                continue;
            }
            entry.without(NodeRole.COORDINATOR)
                .ifPresentOrElse(remaining -> tenants.put(tenant, remaining), () -> tenants.remove(tenant));
        }
    }

    /**
     * Whether writing this would tell the node anything it does not already know.
     *
     * <p>Two cases, and the first is easy to miss: a node that was never told anything and has nothing to be told
     * must be left alone rather than handed an empty document, because an absent key already means exactly that.
     */
    private static boolean unchanged(Assignment existing, Assignment next) {
        if (existing == null) {
            // Nothing was said before and there is nothing to say now, so saying it would be a document whose
            // whole content is "no instructions" -- which is what the absence of the key already means. Without
            // this, every placeable node with no shards and no tenants to coordinate got an empty assignment the
            // moment the applier stopped skipping empty plans.
            return next.tenants().isEmpty();
        }
        // An existing assignment emptied by this pass is a real instruction: drop everything. Only the tenants
        // map is compared, because version and timestamp differ on every rebuild and including them would make
        // every assignment perpetually changed.
        return existing.tenants().equals(next.tenants());
    }

    /** Instructions that were dropped, for the convergence document. */
    public static List<String> reasons(Plan plan) {
        List<String> reasons = new ArrayList<>();
        for (Instruction instruction : plan.instructions()) {
            reasons.add(instruction.subject().key() + ": " + instruction.because());
        }
        return reasons;
    }
}
