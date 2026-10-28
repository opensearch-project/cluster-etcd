package org.opensearch.supervisor.model.plan;

import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.supervisor.PrimaryTerms;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Which primary term every copy is told this pass, and which counters have to be written first ({@code DESIGN.md}
 * §28.6).
 *
 * <p>A pure function of the observation and the plan, so the rules are testable without a store:
 *
 * <ul>
 *   <li><b>A shard's term advances by one each time a primary is placed for it</b>, and at no other time. A fresh
 *       primary replacing a lost one gets a new term as well as a promoted replica does: the lost node may be alive on
 *       the far side of a partition, and the new term is what makes its writes refusable and its remote metadata
 *       unreadable.
 *   <li><b>A counter is never lowered</b>, and an absent one is rebuilt no lower than the highest term any observed
 *       assignment carries — skipping forward over every term already in circulation, never reusing one.
 *   <li><b>An unreadable counter is left alone</b>, and its index resolves to no terms. The planner refuses to place a
 *       primary for such an index, so nothing here needs to bump it.
 * </ul>
 */
public final class TermLedger {

    /** A counter to write before any assignment that carries its terms, compare-and-set on {@code expectedRevision}. */
    public record Write(TenantId tenant, String indexUuid, PrimaryTerms next, long expectedRevision) {}

    /**
     * @param terms the counter every readable index resolves to, after this pass's bumps
     * @param floors per index and shard, the highest term any observed assignment carries — the fallback for an index
     *     whose counter cannot be read
     */
    public record Resolution(
        Map<TenantId, Map<String, PrimaryTerms>> terms,
        List<Write> writes,
        Map<TenantId, Map<String, Map<Integer, Long>>> floors
    ) {

        public Optional<PrimaryTerms> termsOf(TenantId tenant, String indexUuid) {
            Map<String, PrimaryTerms> byIndex = terms.get(tenant);
            return byIndex == null ? Optional.empty() : Optional.ofNullable(byIndex.get(indexUuid));
        }

        public long floorOf(TenantId tenant, String indexUuid, int shard) {
            return floors.getOrDefault(tenant, Map.of()).getOrDefault(indexUuid, Map.of()).getOrDefault(shard, 1L);
        }
    }

    private TermLedger() {}

    public static Resolution resolve(Plan plan, Observation observation) {
        Map<TenantId, Map<String, Map<Integer, Long>>> floors = floorsOf(observation);
        Map<TenantId, Map<String, Set<Integer>>> promoted = primariesPlacedBy(plan);

        Map<TenantId, Map<String, PrimaryTerms>> terms = new LinkedHashMap<>();
        List<Write> writes = new ArrayList<>();

        observation.tenants().forEach((tenant, desired) -> desired.indicesByUuid().forEach((uuid, index) -> {
            Optional<Observation.TermCounter> counter = observation.termCounter(tenant, uuid);
            if (counter.isPresent() && counter.get().readable() == false) {
                return;
            }

            PrimaryTerms base = counter.map(Observation.TermCounter::terms).orElse(new PrimaryTerms(Map.of()));
            PrimaryTerms next = base
                .atLeast(everyShardAtLeastOne(index))
                .atLeast(floors.getOrDefault(tenant, Map.of()).getOrDefault(uuid, Map.of()));
            for (int shard : promoted.getOrDefault(tenant, Map.of()).getOrDefault(uuid, Set.of())) {
                next = next.advanced(shard);
            }

            terms.computeIfAbsent(tenant, ignored -> new LinkedHashMap<>()).put(uuid, next);
            if (counter.isEmpty() || next.equals(base) == false) {
                // Revision 0 is the store's "must not exist", so creating a counter and advancing one are the same
                // compare-and-set -- the same convention NodeObservation.assignmentRevision uses.
                writes.add(new Write(tenant, uuid, next, counter.map(Observation.TermCounter::revision).orElse(0L)));
            }
        }));

        return new Resolution(terms, writes, floors);
    }

    /** Created at 1 for every shard, so an absent counter for an index already seen is never read as a default. */
    private static Map<Integer, Long> everyShardAtLeastOne(IndexConfig index) {
        Map<Integer, Long> ones = new TreeMap<>();
        for (int shard = 0; shard < index.numberOfShards(); shard++) {
            ones.put(shard, 1L);
        }
        return ones;
    }

    /**
     * The highest term each shard has been told on any observed node, lost ones included.
     *
     * <p>Lost nodes count on purpose. Their assignments are the evidence of terms already issued, and a node that
     * stopped heartbeating may still be running at the term it was given.
     */
    private static Map<TenantId, Map<String, Map<Integer, Long>>> floorsOf(Observation observation) {
        Map<TenantId, Map<String, Map<Integer, Long>>> floors = new LinkedHashMap<>();
        observation.nodes().values().forEach(node -> {
            Assignment assignment = node.assignment();
            if (assignment == null) {
                return;
            }
            assignment.tenants().forEach((tenant, entry) -> entry.indices().forEach((uuid, index) -> {
                if (index.primaryTerms() == null) {
                    return;
                }
                Map<Integer, Long> byShard = floors
                    .computeIfAbsent(tenant, ignored -> new LinkedHashMap<>())
                    .computeIfAbsent(uuid, ignored -> new TreeMap<>());
                index.primaryTerms().forEach((shard, term) -> byShard.merge(shard, term, Math::max));
            }));
        });
        return floors;
    }

    /** Shards this plan places a primary for, each once however many instructions name it. */
    private static Map<TenantId, Map<String, Set<Integer>>> primariesPlacedBy(Plan plan) {
        Map<TenantId, Map<String, Set<Integer>>> placed = new LinkedHashMap<>();
        for (Instruction instruction : plan.instructions()) {
            if (instruction instanceof Instruction.PlaceCopy place && place.role() == ShardRole.PRIMARY) {
                placed.computeIfAbsent(place.tenant(), ignored -> new LinkedHashMap<>())
                    .computeIfAbsent(place.indexUuid(), ignored -> new LinkedHashSet<>())
                    .add(place.shard());
            }
        }
        return placed;
    }
}
