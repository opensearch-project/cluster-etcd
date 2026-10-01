package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.settings.AllocationEnable;
import org.opensearch.supervisor.model.settings.RebalanceEnable;
import org.opensearch.supervisor.model.status.Replication;

import java.util.Objects;

/**
 * Everything a rebalance decider needs to judge one proposed move, and the single place that knows how to
 * count what is already in flight.
 *
 * <p>Centralised for the same reason {@code AllocationContext} centralises copy counting: every pacing rule
 * needs the same answer to "how much movement is already happening", and a rule that computed it for itself
 * would eventually disagree with the others. Movement in flight is read as <b>copies assigned but not yet
 * reported</b>, which is the one definition available from an observation alone and needs no memory of what
 * previous passes decided.
 */
public final class RebalanceContext {

    private final Observation observation;
    private final LoadModel loads;
    private final LoadModel.Held copy;
    private final NodeId target;
    private final IndexConfig index;
    private final double threshold;

    public RebalanceContext(
        Observation observation,
        LoadModel loads,
        LoadModel.Held copy,
        NodeId target,
        IndexConfig index,
        double threshold
    ) {
        this.observation = Objects.requireNonNull(observation, "observation");
        this.loads = Objects.requireNonNull(loads, "loads");
        this.copy = Objects.requireNonNull(copy, "copy");
        this.target = Objects.requireNonNull(target, "target");
        this.index = index;
        this.threshold = threshold;
    }

    public Observation observation() {
        return observation;
    }

    public LoadModel loads() {
        return loads;
    }

    public LoadModel.Held copy() {
        return copy;
    }

    public ShardRef shard() {
        return copy.shard();
    }

    public NodeId source() {
        return copy.node();
    }

    public NodeId target() {
        return target;
    }

    public ShardRole role() {
        return copy.role();
    }

    public boolean isPrimary() {
        return copy.role() == ShardRole.PRIMARY;
    }

    /** May be null: a copy of an index the control plane has already deleted still weighs on its node. */
    public IndexConfig index() {
        return index;
    }

    /** How much of the pool's imbalance this one copy accounts for, in the load model's normalised units. */
    public double copyWeight() {
        return copy.weight();
    }

    public double sourceLoad() {
        return loads.loadOf(source());
    }

    public double targetLoad() {
        return loads.loadOf(target);
    }

    /** The minimum improvement a move must buy, as a fraction of the copy's own weight. */
    public double threshold() {
        return threshold;
    }

    /**
     * Replication mode, which is what decides whether the source pays for the move.
     *
     * <p>Defaults to document replication when the index is unknown, because that is the expensive reading and
     * an unknown index is not the moment to assume the cheap one.
     */
    public Replication replication() {
        return index == null ? Replication.DOCUMENT : index.replication();
    }

    public MoveCost cost() {
        return MoveCost.of(copy.sizeBytes(), replication());
    }

    // ---------------------------------------------------------------- what is already happening

    /** Copies assigned to a node that it is not yet reporting: work arriving, whoever caused it. */
    public int arrivingOn(NodeId node) {
        int count = 0;
        for (LoadModel.Held held : loads.heldBy(node)) {
            if (held.present() == false || held.state() == ShardState.INITIALIZING) {
                count++;
            }
        }
        return count;
    }

    /** Copies in flight anywhere in the pool. The figure a pool-wide concurrency limit compares against. */
    public int arrivingInPool() {
        int count = 0;
        for (NodeId node : loads.loads().keySet()) {
            count += arrivingOn(node);
        }
        return count;
    }

    /**
     * Whether any shard in the pool is short of the copies it wants.
     *
     * <p>The question that decides whether rebalancing should be happening at all. Recovering redundancy is
     * worth more than tidiness, and both draw on the same recovery bandwidth.
     */
    public boolean poolHasUnderReplicatedShards() {
        return underReplicatedCount() > 0;
    }

    /**
     * How many shards have fewer <b>started</b> copies than they want.
     *
     * <p>Started, not merely assigned, and that distinction is the whole rule. Counting a copy that is still
     * recovering would satisfy this test the instant a replacement was <em>requested</em> rather than when it
     * arrived — which defeats the rule in the one scenario it was written for. A lost node leaves its shards
     * short and the pool lopsided; the planner places replacements; those copies are now initializing; and a
     * balancer that counted them as redundancy would start shuffling shards between the survivors at exactly
     * the moment they are busiest rebuilding what was lost, slowing the rebuild and extending the window in
     * which a second failure loses data.
     *
     * <p>The cost of being strict, stated honestly: a pool with enough churn that something is always recovering
     * will rarely rebalance. That is the right trade — balancing is a background nicety and redundancy is not —
     * and the operator is told which rule is holding it up rather than left to wonder.
     */
    public int underReplicatedCount() {
        java.util.Map<ShardRef, Integer> counts = new java.util.LinkedHashMap<>();
        loads.byNode().values().forEach(held -> held.forEach(copy -> {
            if (copy.present() && copy.state() == ShardState.STARTED) {
                counts.merge(copy.shard(), 1, Integer::sum);
            }
        }));

        int short_ = 0;
        for (var tenantEntry : observation.tenants().entrySet()) {
            for (IndexConfig config : tenantEntry.getValue().indicesByUuid().values()) {
                if (config.open() == false) {
                    continue;
                }
                for (int shard = 0; shard < config.numberOfShards(); shard++) {
                    ShardRef ref = new ShardRef(tenantEntry.getKey(), config.uuid(), shard);
                    if (counts.getOrDefault(ref, 0) < config.copiesPerShard()) {
                        short_++;
                    }
                }
            }
        }
        return short_;
    }

    // ---------------------------------------------------------------- settings

    private Observation.DesiredState desired() {
        return observation.tenants().get(shard().tenant());
    }

    /** The rebalance brake for this shard's tenant. */
    public RebalanceEnable rebalanceEnable() {
        Observation.DesiredState desired = desired();
        return desired == null ? RebalanceEnable.ALL : desired.settings().rebalanceEnable();
    }

    public boolean rebalanceEnableIsRecognised() {
        Observation.DesiredState desired = desired();
        return desired == null || desired.settings().rebalanceEnableIsRecognised();
    }

    /** The allocation brake as it applies to this shard's index, since a move begins with an allocation. */
    public AllocationEnable allocationEnable() {
        Observation.DesiredState desired = desired();
        AllocationEnable clusterValue = desired == null
            ? AllocationEnable.ALL
            : desired.settings().allocationEnable();
        return index == null ? clusterValue : index.effectiveAllocationEnable(clusterValue);
    }

    /** Same proposal, different target. */
    public RebalanceContext to(NodeId otherTarget) {
        return new RebalanceContext(observation, loads, copy, otherTarget, index, threshold);
    }
}
