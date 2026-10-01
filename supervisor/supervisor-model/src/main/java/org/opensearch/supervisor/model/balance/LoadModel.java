package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Turns an observation into a comparable load per node, and a comparable weight per copy.
 *
 * <p>Three problems it exists to solve, and the first is arithmetic. Shard count is measured in shards and
 * shard size in bytes, so <b>they cannot be added</b>: a naive weighted sum is dominated entirely by whichever
 * signal happens to use the larger unit, which means the weights an operator sets would do almost nothing.
 * Each signal is therefore divided by its own mean across the pool before being combined, so every signal
 * contributes about {@code importance} to an average node and the weights mean what they say. A consequence
 * worth knowing: when every shard is the same size, size and count are the <em>same signal</em>, and their
 * relative weight cannot matter. The weights only start to bite once sizes are skewed, which is exactly the
 * case they are there for.
 *
 * <p>The second is that <b>load has to include copies that have not arrived yet</b>. A node that was handed
 * ten replicas one second ago looks empty, and a balancer that believes it will hand it ten more. So a copy
 * that has been assigned counts toward its target's load from the moment the assignment is written.
 *
 * <p>The third follows from the second: an assigned or initializing copy has no size, and a shard whose size
 * is unknown weighs nothing, which would reintroduce the stampede through the back door. It is filled in from
 * <b>the largest copy of the same shard anywhere in the pool</b> — which is not a guess, it is an observation
 * of that shard, and a replica ends up about the size of the primary it was built from. Only a shard with no
 * sized copy anywhere, meaning a genuinely new index, falls back to zero.
 */
public final class LoadModel {

    /** How much each signal counts. Absent means the signal is measured but ignored. */
    public record Importance(Map<String, Double> weights) {

        public Importance {
            weights = Map.copyOf(weights);
        }

        public double of(String signal) {
            return weights.getOrDefault(signal, 0.0);
        }

        /**
         * Size at 1.0, count at 0.5.
         *
         * <p>Size leads because bytes are the cost an operator actually feels: disk pressure, the time a
         * recovery takes, the cache that no longer fits. Count is kept at half rather than dropped because it
         * proxies the per-shard costs that do not scale with size at all — heap for segment metadata, file
         * handles, and a slice of every refresh and merge cycle — which is the same ground
         * {@code ShardsLimitDecider} guards as a hard ceiling. A node with two hundred empty shards has a real
         * problem that a size-only model cannot see.
         */
        public static Importance defaults() {
            return new Importance(Map.of(ShardSizeSignal.NAME, 1.0, ShardCountSignal.NAME, 0.5));
        }
    }

    /** One copy on one node, with the normalised weight it contributes. */
    public record Held(
        NodeId node,
        ShardRef shard,
        ShardRole role,
        ShardState state,
        long sizeBytes,
        double weight,
        boolean present
    ) {

        /**
         * Whether this copy is settled enough to be a candidate for moving away.
         *
         * <p>An initializing copy is mid-recovery and moving it throws away the work done so far; an assigned
         * copy the node has not acknowledged is not somewhere it can be moved <em>from</em> yet.
         */
        public boolean isMovable() {
            return present && state == ShardState.STARTED;
        }
    }

    private final List<BalanceSignal> signals;
    private final Importance importance;
    private final Map<NodeId, List<Held>> byNode;
    private final Map<NodeId, Double> loads;
    private final Map<String, Double> means;

    private LoadModel(
        List<BalanceSignal> signals,
        Importance importance,
        Map<NodeId, List<Held>> byNode,
        Map<NodeId, Double> loads,
        Map<String, Double> means
    ) {
        this.signals = signals;
        this.importance = importance;
        this.byNode = byNode;
        this.loads = loads;
        this.means = means;
    }

    public static List<BalanceSignal> defaultSignals() {
        return List.of(new ShardSizeSignal(), new ShardCountSignal());
    }

    public static LoadModel of(Observation observation) {
        return of(observation, defaultSignals(), Importance.defaults());
    }

    public static LoadModel of(Observation observation, List<BalanceSignal> signals, Importance importance) {
        Objects.requireNonNull(observation, "observation");
        List<BalanceSignal> theSignals = List.copyOf(signals);

        // Pass one: every copy that exists or is on its way, and the best known size of every shard.
        Map<NodeId, List<RawCopy>> raw = new LinkedHashMap<>();
        Map<ShardRef, Long> knownSizes = new LinkedHashMap<>();
        observation.placeableNodes().forEach((nodeId, node) -> {
            List<RawCopy> copies = new ArrayList<>();
            collectHeld(observation, nodeId, node, copies, knownSizes);
            collectAssigned(observation, nodeId, node, copies);
            raw.put(nodeId, copies);
        });

        // Pass two: fill in unknown sizes from a sized copy of the same shard elsewhere, then weigh.
        Map<String, Double> totals = new TreeMap<>();
        Map<NodeId, List<Held>> byNode = new LinkedHashMap<>();
        Map<NodeId, Map<String, Double>> perNodePerSignal = new LinkedHashMap<>();

        raw.forEach((nodeId, copies) -> {
            Map<String, Double> perSignal = new TreeMap<>();
            List<Held> held = new ArrayList<>(copies.size());
            for (RawCopy copy : copies) {
                long size = copy.sizeBytes >= 0 ? copy.sizeBytes : knownSizes.getOrDefault(copy.shard, -1L);
                ShardInventory.ShardCopy sized = new ShardInventory.ShardCopy(
                    copy.role, copy.state, size < 0 ? null : size, null
                );
                for (BalanceSignal signal : theSignals) {
                    double value = signal.weightOf(sized, copy.index);
                    perSignal.merge(signal.name(), value, Double::sum);
                    totals.merge(signal.name(), value, Double::sum);
                }
                held.add(new Held(
                    nodeId, copy.shard, copy.role, copy.state, Math.max(size, 0L), 0.0, copy.present
                ));
            }
            perNodePerSignal.put(nodeId, perSignal);
            byNode.put(nodeId, held);
        });

        int nodeCount = Math.max(raw.size(), 1);
        Map<String, Double> means = new TreeMap<>();
        for (BalanceSignal signal : theSignals) {
            means.put(signal.name(), totals.getOrDefault(signal.name(), 0.0) / nodeCount);
        }

        // Pass three: normalise. Now that the means are known, a copy's weight and a node's load are in the
        // same unit, which is what lets the rebalancer compare "how unbalanced is this" against "how much
        // would moving this copy change it".
        Map<NodeId, Double> loads = new LinkedHashMap<>();
        Map<NodeId, List<Held>> weighted = new LinkedHashMap<>();
        byNode.forEach((nodeId, held) -> {
            double load = 0.0;
            Map<String, Double> perSignal = perNodePerSignal.get(nodeId);
            for (BalanceSignal signal : theSignals) {
                double mean = means.getOrDefault(signal.name(), 0.0);
                if (mean <= 0.0) {
                    continue;
                }
                load += importance.of(signal.name()) * perSignal.getOrDefault(signal.name(), 0.0) / mean;
            }
            loads.put(nodeId, load);

            List<Held> reweighed = new ArrayList<>(held.size());
            for (Held copy : held) {
                reweighed.add(new Held(
                    copy.node(), copy.shard(), copy.role(), copy.state(), copy.sizeBytes(),
                    weigh(theSignals, importance, means, copy, indexOf(observation, copy.shard())), copy.present()
                ));
            }
            reweighed.sort(Comparator.comparingDouble(Held::weight).reversed()
                .thenComparing(copy -> copy.shard().toString()));
            weighted.put(nodeId, List.copyOf(reweighed));
        });

        return new LoadModel(theSignals, importance, weighted, loads, means);
    }

    private static double weigh(
        List<BalanceSignal> signals,
        Importance importance,
        Map<String, Double> means,
        Held copy,
        IndexConfig index
    ) {
        ShardInventory.ShardCopy sized = new ShardInventory.ShardCopy(
            copy.role(), copy.state(), copy.sizeBytes(), null
        );
        double weight = 0.0;
        for (BalanceSignal signal : signals) {
            double mean = means.getOrDefault(signal.name(), 0.0);
            if (mean <= 0.0) {
                continue;
            }
            weight += importance.of(signal.name()) * signal.weightOf(sized, index) / mean;
        }
        return weight;
    }

    // ---------------------------------------------------------------- reading it back

    /** Combined, normalised load. An average node is about the sum of the signal importances. */
    public double loadOf(NodeId node) {
        return loads.getOrDefault(node, 0.0);
    }

    public Map<NodeId, Double> loads() {
        return Map.copyOf(loads);
    }

    /** Copies on one node, heaviest first — which is the order the rebalancer wants to try them in. */
    public List<Held> heldBy(NodeId node) {
        return byNode.getOrDefault(node, List.of());
    }

    public Map<NodeId, List<Held>> byNode() {
        return Map.copyOf(byNode);
    }

    /** Nodes in load order, heaviest first. */
    public List<NodeId> byLoadDescending() {
        return loads.entrySet().stream()
            .sorted(Map.Entry.<NodeId, Double>comparingByValue().reversed()
                .thenComparing(entry -> entry.getKey().value()))
            .map(Map.Entry::getKey)
            .toList();
    }

    /** Nodes in load order, lightest first. */
    public List<NodeId> byLoadAscending() {
        return loads.entrySet().stream()
            .sorted(Map.Entry.<NodeId, Double>comparingByValue()
                .thenComparing(entry -> entry.getKey().value()))
            .map(Map.Entry::getKey)
            .toList();
    }

    /**
     * How lopsided the pool is: the gap between the busiest and the idlest node.
     *
     * <p>Max minus min rather than a variance or a standard deviation, because the number has to mean
     * something to the operator reading it and because it is the quantity a single move actually changes. A
     * variance improves slightly with almost any move, which is a good way to build a balancer that never
     * stops moving things.
     */
    public double spread() {
        if (loads.isEmpty()) {
            return 0.0;
        }
        double max = Double.NEGATIVE_INFINITY;
        double min = Double.POSITIVE_INFINITY;
        for (double load : loads.values()) {
            max = Math.max(max, load);
            min = Math.min(min, load);
        }
        return max - min;
    }

    public List<BalanceSignal> signals() {
        return signals;
    }

    public Importance importance() {
        return importance;
    }

    /** Mean per-node value of a signal, in that signal's own units. For explanations. */
    public double meanOf(String signal) {
        return means.getOrDefault(signal, 0.0);
    }

    // ---------------------------------------------------------------- collecting

    private record RawCopy(
        ShardRef shard,
        IndexConfig index,
        ShardRole role,
        ShardState state,
        long sizeBytes,
        boolean present
    ) {}

    private static void collectHeld(
        Observation observation,
        NodeId nodeId,
        Observation.NodeObservation node,
        List<RawCopy> into,
        Map<ShardRef, Long> knownSizes
    ) {
        ShardInventory inventory = node.inventory();
        if (inventory == null) {
            return;
        }
        inventory.tenants().forEach((tenant, tenantShards) -> {
            tenantShards.indices().forEach((uuid, indexShards) -> {
                indexShards.shards().forEach((shardNumber, copy) -> {
                    ShardRef ref = new ShardRef(tenant, uuid, shardNumber);
                    long size = copy.sizeBytes() == null ? -1L : copy.sizeBytes();
                    if (size >= 0) {
                        knownSizes.merge(ref, size, Math::max);
                    }
                    into.add(new RawCopy(
                        ref, indexOf(observation, ref), copy.role(), copy.state(), size, true
                    ));
                });
            });
        });
    }

    /**
     * Copies this node has been told to hold but is not reporting yet.
     *
     * <p>Skipped if the inventory already mentions the shard, or a copy that has landed would be counted
     * twice and the node would look twice as loaded as it is for as long as the assignment stands.
     */
    private static void collectAssigned(
        Observation observation,
        NodeId nodeId,
        Observation.NodeObservation node,
        List<RawCopy> into
    ) {
        Assignment assignment = node.assignment();
        if (assignment == null) {
            return;
        }
        assignment.tenants().forEach((tenant, tenantAssignment) -> {
            tenantAssignment.indices().forEach((uuid, indexAssignment) -> {
                indexAssignment.shards().forEach((shardNumber, role) -> {
                    ShardRef ref = new ShardRef(tenant, uuid, shardNumber);
                    boolean alreadyHeld = into.stream().anyMatch(copy -> copy.shard().equals(ref));
                    if (alreadyHeld) {
                        return;
                    }
                    into.add(new RawCopy(
                        ref, indexOf(observation, ref), role, ShardState.INITIALIZING, -1L, false
                    ));
                });
            });
        });
    }

    private static IndexConfig indexOf(Observation observation, ShardRef ref) {
        Observation.DesiredState desired = observation.tenants().get(ref.tenant());
        return desired == null ? null : desired.indicesByUuid().get(ref.indexUuid());
    }

    /** Every tenant a node holds a copy for, for the deciders that need per-tenant context. */
    public static List<TenantId> tenantsOf(Observation observation) {
        return observation.tenants().keySet().stream().sorted(Comparator.comparing(TenantId::value)).toList();
    }
}
