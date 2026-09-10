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
        if (plan.isEmpty()) {
            return new Outcome(0, 0, 0, false, null);
        }

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

        for (var entry : plan.byNode().entrySet()) {
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

            Assignment next = merge(observed, entry.getValue(), plan);
            try {
                store.put(Keys.nodeAssignment(node), NodeJson.write(next), observed.assignmentRevision());
                applied += entry.getValue().size();
            } catch (RevisionConflictException e) {
                // Somebody wrote this node's assignment since it was observed. Dropping the write is the
                // mechanism working, not a failure: whatever they wrote was based on a newer view than this.
                rejected += entry.getValue().size();
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
     */
    private Assignment merge(Observation.NodeObservation observed, List<Instruction> additions, Plan plan) {
        Assignment existing = observed.assignment();
        Map<TenantId, Assignment.TenantAssignment> tenants = new LinkedHashMap<>(
            existing == null ? Map.of() : existing.tenants()
        );

        for (Instruction instruction : additions) {
            // Positive pattern rather than this codebase's usual `== false` guard: flow scoping extends a
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
                tenants.put(place.tenant(), new Assignment.TenantAssignment(
                    tenant == null ? NodeRole.DATA : tenant.role(), indices
                ));
            }
        }

        long version = existing == null ? 1 : existing.version() + 1;
        return new Assignment(
            version, plan.pool(), plan.epoch(), observed.nodeId(), clock.millis(), tenants
        );
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
