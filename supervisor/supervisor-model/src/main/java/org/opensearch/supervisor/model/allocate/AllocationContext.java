package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.settings.AllocationEnable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Everything a decider needs to judge one candidate, and the single place that knows how to count copies.
 *
 * <p>Counting is the part worth centralising, because there are three sources and every rule needs all of
 * them: what a node <em>reports holding</em>, what it has been <em>told to hold</em>, and what this pass is
 * <em>about to tell it</em>. A rule that looked only at inventories would let two placements in one pass both
 * land on the same node; one that looked only at assignments would ignore copies that predate the supervisor.
 *
 * <p><b>Nodes given up for dead contribute nothing</b>, consistently with the planner and the task queue: a
 * lost node's copies are not counted toward a shard's redundancy, its disk is not available, and its awareness
 * value is not represented.
 */
public final class AllocationContext {

    /** A copy the current pass has decided to create but has not yet written. */
    public record PlannedCopy(TenantId tenant, String indexUuid, int shard, ShardRole role, NodeId node) {}

    private final TenantId tenant;
    private final IndexConfig index;
    private final int shard;
    private final ShardRole role;
    private final Observation observation;
    private final List<String> awarenessAttributes;
    private final Integer maxCopiesPerNode;
    private final boolean refuseCopiesSharingAHost;
    private final List<PlannedCopy> planned;

    /**
     * Nodes whose copy of <em>this shard</em> is to be judged as though it were already gone.
     *
     * <p>Empty in every ordinary decision. It exists so that one question can be asked honestly: <em>would
     * removing that copy actually let the primary be placed here?</em> The alternative is to infer it from the
     * refusal — see {@code same_shard}, delete the copy, hope — and that guesses about the other eight deciders.
     * A node refused for disk as well as for holding the copy would lose a serving copy and gain nothing, which
     * is strictly worse than the deadlock it was meant to break.
     *
     * <p>Scoped to the shard being placed, because that is the only copy a displacement would remove. The node's
     * other copies still count against its limits, still fill its disk and still make it a worse candidate.
     */
    private final Set<NodeId> displacing;

    public AllocationContext(
        TenantId tenant,
        IndexConfig index,
        int shard,
        ShardRole role,
        Observation observation,
        List<String> awarenessAttributes,
        Integer maxCopiesPerNode,
        List<PlannedCopy> planned
    ) {
        this(tenant, index, shard, role, observation, awarenessAttributes, maxCopiesPerNode, false, planned);
    }

    /**
     * @param refuseCopiesSharingAHost whether two copies of one shard are refused when the nodes holding them
     *     share an address. Off unless the pool asks for it — see {@link SameShardDecider} for why an address is
     *     a poor proxy for a failure domain in both directions.
     */
    public AllocationContext(
        TenantId tenant,
        IndexConfig index,
        int shard,
        ShardRole role,
        Observation observation,
        List<String> awarenessAttributes,
        Integer maxCopiesPerNode,
        boolean refuseCopiesSharingAHost,
        List<PlannedCopy> planned
    ) {
        this(tenant, index, shard, role, observation, awarenessAttributes, maxCopiesPerNode,
            refuseCopiesSharingAHost, planned, Set.of());
    }

    private AllocationContext(
        TenantId tenant,
        IndexConfig index,
        int shard,
        ShardRole role,
        Observation observation,
        List<String> awarenessAttributes,
        Integer maxCopiesPerNode,
        boolean refuseCopiesSharingAHost,
        List<PlannedCopy> planned,
        Set<NodeId> displacing
    ) {
        this.tenant = Objects.requireNonNull(tenant, "tenant");
        this.index = Objects.requireNonNull(index, "index");
        this.shard = shard;
        this.role = Objects.requireNonNull(role, "role");
        this.observation = Objects.requireNonNull(observation, "observation");
        this.awarenessAttributes = awarenessAttributes == null ? List.of() : List.copyOf(awarenessAttributes);
        this.maxCopiesPerNode = maxCopiesPerNode;
        this.refuseCopiesSharingAHost = refuseCopiesSharingAHost;
        this.planned = planned == null ? List.of() : List.copyOf(planned);
        this.displacing = displacing == null ? Set.of() : Set.copyOf(displacing);
    }

    public TenantId tenant() {
        return tenant;
    }

    public IndexConfig index() {
        return index;
    }

    public int shard() {
        return shard;
    }

    /** The role being placed. */
    public ShardRole role() {
        return role;
    }

    public Observation observation() {
        return observation;
    }

    public List<String> awarenessAttributes() {
        return awarenessAttributes;
    }

    public Optional<Integer> maxCopiesPerNode() {
        return Optional.ofNullable(maxCopiesPerNode);
    }

    /** Whether this pool refuses two copies of one shard on nodes sharing an address. */
    public boolean refuseCopiesSharingAHost() {
        return refuseCopiesSharingAHost;
    }

    /**
     * The allocation brake as it applies to this index: its own setting if it has one, else the tenant's.
     *
     * <p>Resolved here rather than in the decider so that there is one place the precedence is written down.
     */
    public AllocationEnable allocationEnable() {
        Observation.DesiredState desired = observation.tenants().get(tenant);
        AllocationEnable clusterValue = desired == null
            ? AllocationEnable.ALL
            : desired.settings().allocationEnable();
        return index.effectiveAllocationEnable(clusterValue);
    }

    /** Total desired copies of this shard: one primary plus its replicas. */
    public int desiredCopiesOfThisShard() {
        return index.copiesAuthorisedFor(shard);
    }

    // ---------------------------------------------------------------- counting copies

    /** Nodes that hold, are assigned, or are about to be given a copy of the shard being placed. */
    public Set<NodeId> nodesWithThisShard() {
        Set<NodeId> nodes = new LinkedHashSet<>();
        observation.nodes().forEach((nodeId, node) -> {
            if (node.copiesCount() == false || displacing.contains(nodeId)) {
                return;
            }
            // Two different kinds of evidence, and they need different tests. What a node reports holding counts
            // whenever its copies count at all; what it has merely been told to hold counts only while it is
            // still reporting, or a node that died before acknowledging an instruction keeps the shard from ever
            // being placed anywhere. See NodeObservation.actsOnItsAssignment.
            if (heldCopy(node, shard).isPresent()) {
                nodes.add(nodeId);
            } else if (node.actsOnItsAssignment() && node.isAssigned(tenant, index.uuid(), shard)) {
                nodes.add(nodeId);
            }
        });
        for (PlannedCopy copy : planned) {
            if (copy.tenant().equals(tenant) && copy.indexUuid().equals(index.uuid()) && copy.shard() == shard) {
                nodes.add(copy.node());
            }
        }
        return nodes;
    }

    /**
     * Copies of the shard being placed that carry one particular role — held, assigned, or planned this pass.
     *
     * <p>Needed because a shard's desired copies are no longer one number. A shard wanting one replica and one
     * search replica is not satisfied by two search replicas, so "how many copies are there" cannot answer
     * "should another of *this* role be planned".
     */
    public int copiesOfThisShardInRole(ShardRole wanted) {
        int count = 0;
        for (Map.Entry<NodeId, Observation.NodeObservation> entry : observation.nodes().entrySet()) {
            Observation.NodeObservation node = entry.getValue();
            if (node.copiesCount() == false || displacing.contains(entry.getKey())) {
                continue;
            }
            Optional<ShardInventory.ShardCopy> held = heldCopy(node, shard);
            if (held.isPresent()) {
                // What a node reports holding beats what it was told to hold: the assignment may name a role it
                // has not applied yet, and counting both would double-count the same copy.
                if (held.get().role() == wanted) {
                    count++;
                }
                continue;
            }
            if (node.actsOnItsAssignment() && assignedRole(node) == wanted) {
                count++;
            }
        }
        for (PlannedCopy copy : planned) {
            if (copy.tenant().equals(tenant) && copy.indexUuid().equals(index.uuid())
                && copy.shard() == shard && copy.role() == wanted) {
                count++;
            }
        }
        return count;
    }

    private ShardRole assignedRole(Observation.NodeObservation node) {
        Assignment assignment = node.assignment();
        if (assignment == null) {
            return null;
        }
        Assignment.TenantAssignment tenantAssignment = assignment.tenants().get(tenant);
        if (tenantAssignment == null) {
            return null;
        }
        Assignment.IndexAssignment indexAssignment = tenantAssignment.indices().get(index.uuid());
        return indexAssignment == null ? null : indexAssignment.shards().get(shard);
    }

    /** Whether some node already has a started primary of this shard. */
    public boolean primaryIsActive() {
        for (Map.Entry<NodeId, Observation.NodeObservation> entry : observation.nodes().entrySet()) {
            Observation.NodeObservation node = entry.getValue();
            if (node.copiesCount() == false || displacing.contains(entry.getKey())) {
                continue;
            }
            Optional<ShardInventory.ShardCopy> copy = heldCopy(node, shard);
            if (copy.isPresent() && copy.get().isActivePrimary()) {
                return true;
            }
        }
        return false;
    }

    /** Copies of anything, on one node — held, assigned, or planned this pass. Used for shard limits. */
    public int copiesOn(NodeId nodeId) {
        Observation.NodeObservation node = observation.nodes().get(nodeId);
        int count = 0;
        if (node != null) {
            ShardInventory inventory = node.inventory();
            if (inventory != null) {
                count += inventory.copyCount();
            } else if (node.assignment() != null && node.actsOnItsAssignment()) {
                // No inventory yet: the assignment is the best available statement of what it will hold, and
                // ignoring it would let a node be filled up before it has reported anything at all. Only while
                // it is reporting, though -- a silent node is not filling up.
                count += node.assignment().copyCount();
            }
            if (displacing.contains(nodeId) && count > 0) {
                // The copy about to be displaced does not occupy a slot in the world being asked about, and this
                // is the number ShardsLimitDecider reads -- so a node at its cap would otherwise be refused the
                // primary it is being emptied for.
                count--;
            }
        }
        for (PlannedCopy copy : planned) {
            if (copy.node().equals(nodeId)) {
                count++;
            }
        }
        return count;
    }

    /** Copies currently recovering on one node, plus any this pass has just added. Used for throttling. */
    public int recoveringOn(NodeId nodeId) {
        Observation.NodeObservation node = observation.nodes().get(nodeId);
        int count = 0;
        if (node != null && node.inventory() != null) {
            for (ShardInventory.TenantShards tenantShards : node.inventory().tenants().values()) {
                for (ShardInventory.IndexShards indexShards : tenantShards.indices().values()) {
                    for (ShardInventory.ShardCopy copy : indexShards.shards().values()) {
                        if (copy.state() == ShardState.INITIALIZING) {
                            count++;
                        }
                    }
                }
            }
            if (displacing.contains(nodeId)
                && heldCopy(node, shard).filter(copy -> copy.state() == ShardState.INITIALIZING).isPresent()) {
                count--;
            }
        }
        for (PlannedCopy copy : planned) {
            if (copy.node().equals(nodeId)) {
                count++;
            }
        }
        return count;
    }

    // ---------------------------------------------------------------- awareness

    /** The value a node has for an attribute, or empty if it does not declare one. */
    public Optional<String> attributeOf(NodeId nodeId, String attribute) {
        Observation.NodeObservation node = observation.nodes().get(nodeId);
        if (node == null || node.registration() == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(node.registration().attributes().get(attribute));
    }

    /** Distinct values of an attribute across nodes that could take a copy. */
    public Set<String> attributeValues(String attribute) {
        Set<String> values = new java.util.TreeSet<>();
        observation.nodes().forEach((nodeId, node) -> {
            if (node.copiesCount() == false || node.registration() == null) {
                return;
            }
            String value = node.registration().attributes().get(attribute);
            if (value != null) {
                values.add(value);
            }
        });
        return values;
    }

    /** How many copies of the shard being placed sit in each value of an attribute. */
    public Map<String, Integer> copiesPerAttributeValue(String attribute) {
        Map<String, Integer> counts = new TreeMap<>();
        for (NodeId nodeId : nodesWithThisShard()) {
            attributeOf(nodeId, attribute).ifPresent(value -> counts.merge(value, 1, Integer::sum));
        }
        return counts;
    }

    // ---------------------------------------------------------------- helpers

    private Optional<ShardInventory.ShardCopy> heldCopy(Observation.NodeObservation node, int shardNumber) {
        ShardInventory inventory = node.inventory();
        if (inventory == null) {
            return Optional.empty();
        }
        ShardInventory.TenantShards tenantShards = inventory.tenants().get(tenant);
        if (tenantShards == null) {
            return Optional.empty();
        }
        ShardInventory.IndexShards held = tenantShards.indices().get(index.uuid());
        return held == null ? Optional.empty() : Optional.ofNullable(held.shards().get(shardNumber));
    }

    /** Same context, for a different shard and role. Cheap, because everything else is shared. */
    public AllocationContext forShard(int otherShard, ShardRole otherRole) {
        // Deliberately drops `displacing`, which names copies of *this* shard: carrying it to another shard
        // would hide a copy that is really there.
        return new AllocationContext(
            tenant, index, otherShard, otherRole, observation, awarenessAttributes, maxCopiesPerNode,
            refuseCopiesSharingAHost, planned, Set.of()
        );
    }

    /** Same context with one more planned copy, so the next decision sees what this one decided. */
    public AllocationContext plus(PlannedCopy copy) {
        List<PlannedCopy> next = new ArrayList<>(planned);
        next.add(copy);
        return new AllocationContext(
            tenant, index, shard, role, observation, awarenessAttributes, maxCopiesPerNode,
            refuseCopiesSharingAHost, next, displacing
        );
    }

    /**
     * The same question asked of a world where these nodes' copies of this shard are already gone.
     *
     * <p>Used to establish that a displacement would actually achieve something before a serving copy is
     * deleted for it. Every decider runs unchanged; only the copy counts differ.
     */
    public AllocationContext withoutCopiesOfThisShardOn(Set<NodeId> nodes) {
        return new AllocationContext(
            tenant, index, shard, role, observation, awarenessAttributes, maxCopiesPerNode,
            refuseCopiesSharingAHost, planned, nodes
        );
    }

    /**
     * The role a node has for the shard being placed — what it reports holding, else what it was told to hold.
     *
     * <p>Reported beats assigned, the same precedence as {@link #copiesOfThisShardInRole}: the assignment may
     * name a role the node has not applied yet.
     */
    public Optional<ShardRole> roleOfCopyOn(NodeId nodeId) {
        Observation.NodeObservation node = observation.nodes().get(nodeId);
        if (node == null || node.copiesCount() == false) {
            return Optional.empty();
        }
        Optional<ShardInventory.ShardCopy> held = heldCopy(node, shard);
        if (held.isPresent()) {
            return Optional.of(held.get().role());
        }
        return node.actsOnItsAssignment() ? Optional.ofNullable(assignedRole(node)) : Optional.empty();
    }

    /** Every assignment already in force, for a decider that needs the whole picture. */
    public Map<NodeId, Assignment> assignments() {
        Map<NodeId, Assignment> assignments = new LinkedHashMap<>();
        observation.nodes().forEach((nodeId, node) -> {
            if (node.assignment() != null) {
                assignments.put(nodeId, node.assignment());
            }
        });
        return assignments;
    }
}
