package org.opensearch.supervisor.fakenode;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.status.IndexStatus;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.AppliedOutcome;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A data plane node, as far as the store is concerned: it registers, heartbeats, keeps a shard inventory,
 * and obeys its assignment. It holds no data and serves nothing.
 *
 * <p>This is the first implementation of {@code NODE_CONTRACT.md} written from the document rather than
 * from the supervisor, which makes it the contract's first real review. Where the document was silent, the
 * choice made here is marked <b>GAP</b> in a comment and listed in that file's open questions — a contract
 * that needs a reader to guess is a contract that two implementations will guess differently.
 *
 * <p>{@link #tick()} is explicit rather than a background thread, for the same reason the supervisor's loop
 * is: a pass that only advances when asked is a pass a test can reason about.
 */
public final class FakeNode {

    private final KeyValueStore store;
    private final Clock clock;
    private final NodeId nodeId;
    private final Registration template;
    private final String ephemeralId;

    private final Map<Integer, Long> shardSizes = new TreeMap<>();
    private long allocationsMinted;
    private ShardInventory inventory;
    private String inventoryDigest;
    private long appliedVersion = Heartbeat.NO_ASSIGNMENT;
    private AppliedOutcome outcome = AppliedOutcome.APPLIED;
    private String failureDetail;

    public FakeNode(KeyValueStore store, Registration template, Clock clock) {
        this(store, template, clock, "eph-" + Long.toHexString(clock.millis()));
    }

    public FakeNode(KeyValueStore store, Registration template, Clock clock, String ephemeralId) {
        this.store = Objects.requireNonNull(store, "store");
        this.template = Objects.requireNonNull(template, "template");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.ephemeralId = Objects.requireNonNull(ephemeralId, "ephemeralId");
        this.nodeId = template.nodeId();
        this.inventory = ShardInventory.empty(nodeId, ephemeralId, clock.millis());
    }

    /**
     * Pins how big a shard reports itself to be, in bytes.
     *
     * <p>The default sizes vary by only a few percent, which was fine while nothing read them and is not fine
     * now that shard size is a balancing signal: a pool of near-identical shards cannot express the case the
     * size signal exists for, so a test written against the default would pass whether or not the supervisor
     * looked at bytes at all. A real shard is the same size wherever it lives, so a test setting this must set
     * it identically on every node holding the shard.
     */
    public FakeNode withShardSize(int shard, long bytes) {
        shardSizes.put(shard, bytes);
        return this;
    }

    /**
     * Announce this process. A new {@code ephemeralId} is what tells the supervisor the node restarted, so
     * this is written on every start even when nothing else about the node has changed.
     */
    public void start() {
        Registration registration = new Registration(
            nodeId,
            ephemeralId,
            template.name(),
            template.pool(),
            template.roles(),
            template.address(),
            template.port(),
            template.version(),
            template.attributes(),
            template.heartbeatIntervalMillis(),
            clock.millis(),
            // Carried through, not rebuilt: a node that dropped its own declared features on the way to the store
            // would be one the supervisor never promotes, for a reason nobody would think to look for.
            template.contractFeatures()
        );
        store.put(Keys.nodeRegistration(nodeId), NodeJson.write(registration));
    }

    /**
     * One pass: read the assignment, move the inventory one step toward it, and heartbeat.
     *
     * <p>The write order is the contract's, and it is not incidental. The inventory goes first, then the
     * heartbeat announcing its digest, so that a mismatch always means "the heartbeat is behind the
     * inventory" — safe for the supervisor to act on. The reverse ordering would have it chase an inventory
     * that does not exist yet.
     */
    public void tick() {
        Optional<Assignment> assignment = readAssignment();
        Map<Desired, ShardRole> desired = assignment.map(FakeNode::desiredOf).orElse(null);

        if (desired != null) {
            converge(desired, assignment.get());
        }

        ShardInventory current = inventory;
        byte[] inventoryBytes;
        try {
            inventoryBytes = NodeJson.write(current);
        } catch (RuntimeException e) {
            // NODE_CONTRACT.md: a node that cannot write its inventory writes nothing and says so. A
            // truncated or absent inventory read as "these copies are gone" is how the supervisor would be
            // told to replace shards that exist.
            outcome = AppliedOutcome.FAILED;
            failureDetail = "could not serialise inventory: " + e.getMessage();
            heartbeat(current);
            return;
        }

        String digest = NodeJson.digest(inventoryBytes);
        if (digest.equals(inventoryDigest) == false) {
            store.put(Keys.nodeShards(nodeId), inventoryBytes);
            inventoryDigest = digest;
        }

        heartbeat(current);
    }

    private Optional<Assignment> readAssignment() {
        return store.get(Keys.nodeAssignment(nodeId)).map(versioned -> {
            try {
                return NodeJson.readAssignment(versioned.value());
            } catch (RuntimeException e) {
                // An unreadable assignment is treated as no assignment, never as an empty one. An empty
                // assignment means "drop everything you hold", and inferring that from a parse failure
                // would let one corrupt write delete a node's shards.
                outcome = AppliedOutcome.FAILED;
                failureDetail = "could not parse assignment: " + e.getMessage();
                return null;
            }
        }).filter(Objects::nonNull);
    }

    /** One desired copy: which tenant, which index, which shard. */
    record Desired(TenantId tenant, String indexUuid, String indexName, long configRevision, int shard)
        implements Comparable<Desired> {

        @Override
        public int compareTo(Desired other) {
            int byTenant = tenant.compareTo(other.tenant);
            if (byTenant != 0) {
                return byTenant;
            }
            int byIndex = indexUuid.compareTo(other.indexUuid);
            return byIndex != 0 ? byIndex : Integer.compare(shard, other.shard);
        }
    }

    /**
     * The copies an assignment asks this node to hold.
     *
     * <p>Package-private rather than private so the node-contract adapter can ask this node what it made of a
     * document, using the same derivation the node itself acts on. Re-deriving it in the adapter would test the
     * adapter.
     */
    static Map<Desired, ShardRole> desiredOf(Assignment assignment) {
        Map<Desired, ShardRole> desired = new TreeMap<>();
        assignment.tenants().forEach((tenant, tenantAssignment) ->
            tenantAssignment.indices().forEach((uuid, index) ->
                index.shards().forEach((shard, role) ->
                    desired.put(new Desired(tenant, uuid, index.name(), index.configRevision(), shard), role)
                )
            )
        );
        return desired;
    }

    /**
     * Move one step toward the assignment.
     *
     * <p>A new copy appears as {@code INITIALIZING} and becomes serving on the following tick, so that the
     * supervisor's handling of a copy that exists but cannot yet serve is exercised by something other
     * than a hand-written fixture. Copies the assignment does not name are removed: the assignment is the
     * complete desired set, never a delta.
     *
     * <p><b>A copy of a closed index is reported as absent</b>, as {@code NODE_CONTRACT.md} says, while its entry
     * stays in the assignment so a real node keeps the data for a reopen. This node holds no data, so a reopened
     * index's copies simply come back as {@code INITIALIZING} and then serve, as a real node's recovery would.
     */
    private void converge(Map<Desired, ShardRole> assigned, Assignment assignment) {
        Map<Desired, ShardInventory.ShardCopy> held = flatten(inventory);
        Map<Desired, ShardInventory.ShardCopy> next = new TreeMap<>();
        Map<String, IndexConfig> indices = new java.util.HashMap<>();
        Map<Desired, ShardRole> desired = withoutClosedIndices(assigned, indices);

        for (Map.Entry<Desired, ShardRole> entry : desired.entrySet()) {
            Desired want = entry.getKey();
            ShardInventory.ShardCopy have = held.get(want);

            if (have == null) {
                // A brand-new copy exists on disk before it can serve, and it already knows what it is
                // going to be. The role is set immediately; only the state has to catch up.
                next.put(want, ShardInventory.ShardCopy.of(entry.getValue(), ShardState.INITIALIZING));
            } else if (have.role() != entry.getValue() || have.state() != ShardState.STARTED) {
                // Covers the INITIALIZING copy becoming STARTED and a promotion between roles. A real node
                // would have considerably more to do for either, and a docrep promotion in particular is
                // not instantaneous.
                next.put(want, ShardInventory.ShardCopy.of(entry.getValue(), ShardState.STARTED, simulatedSize(want)));
            } else {
                next.put(want, have);
            }
        }
        // Anything held and not desired is absent from next, and so is gone. Nothing models a slow delete.
        next.replaceAll((where, copy) -> withReplicationIds(where, copy, held.get(where), indices));

        appliedVersion = assignment.version();
        failureDetail = null;
        outcome = matches(next, desired) ? AppliedOutcome.APPLIED : AppliedOutcome.APPLYING;

        // Rebuild only when the copies actually changed. The rebuilt document carries a fresh
        // generated_at, so rebuilding unconditionally would give the inventory new bytes and a new digest
        // on every tick — which would have the supervisor re-read it every pass and put the document that
        // scales with shard count back onto the hot path. See NODE_CONTRACT.md: generated_at means "when
        // this inventory last changed", not "when it was last computed".
        if (next.equals(held) == false) {
            inventory = rebuild(next);
        }
    }

    /** The assigned copies whose index is not closed. */
    private Map<Desired, ShardRole> withoutClosedIndices(Map<Desired, ShardRole> assigned, Map<String, IndexConfig> indices) {
        Map<Desired, ShardRole> open = new TreeMap<>();
        assigned.forEach((copy, role) -> {
            IndexConfig index = indexOf(copy, indices);
            if (index == null || index.open()) {
                open.put(copy, role);
            }
        });
        return open;
    }

    /**
     * The index document a copy was assigned against, or null. Read every tick, as a real node fabricates its metadata
     * from it, found by name and trusted only if its uuid is the one the assignment named (DESIGN.md section 12): one
     * that is absent, unreadable or a namesake is null, and leaves the copy as it was.
     *
     * @param indices this tick's reads, so one index is read once however many of its shards are held
     */
    private IndexConfig indexOf(Desired copy, Map<String, IndexConfig> indices) {
        return indices.computeIfAbsent(copy.tenant() + "/" + copy.indexUuid(), ignored -> {
            var stored = store.get(Keys.indicesPrefix(copy.tenant()) + copy.indexName());
            if (stored.isEmpty()) {
                return null;
            }
            try {
                IndexConfig index = IndexConfig.parse(stored.get().value(), stored.get().revision());
                return index.uuid().equals(copy.indexUuid()) ? index : null;
            } catch (RuntimeException e) {
                return null;
            }
        });
    }

    /**
     * A document-replication copy's identity, and on a primary its in-sync set (finding 12); a segment-replication
     * copy carries neither.
     *
     * <p>The allocation id is minted when the copy is created and kept for as long as it exists, through a promotion
     * too, as OpenSearch keeps it. The in-sync set stands in for a primary's replication tracker: itself, plus every
     * replica of the shard that the index projection shows started with an id, which is the point at which a real
     * primary would have finished recovering it. Sorted, so an unchanged set leaves the inventory's bytes alone.
     */
    private ShardInventory.ShardCopy withReplicationIds(
        Desired where,
        ShardInventory.ShardCopy copy,
        ShardInventory.ShardCopy before,
        Map<String, IndexConfig> indices
    ) {
        IndexConfig index = indexOf(where, indices);
        if (index == null || index.replication() != org.opensearch.supervisor.model.status.Replication.DOCUMENT) {
            return copy.withReplicationIds(null, null);
        }
        String allocationId = before != null && before.allocationId() != null
            ? before.allocationId()
            // The ephemeral id is fresh per process, so a restarted node cannot mint an id its predecessor used.
            : ephemeralId + "-" + (++allocationsMinted);
        if (copy.role() != ShardRole.PRIMARY) {
            return copy.withReplicationIds(allocationId, null);
        }
        java.util.TreeSet<String> inSync = new java.util.TreeSet<>();
        inSync.add(allocationId);
        store.get(Keys.indexStatus(where.tenant(), where.indexUuid())).ifPresent(versioned -> {
            try {
                IndexStatus status = IndexStatus.from(versioned.value());
                for (IndexStatus.ShardPlacement replica : status.replicasOf(where.shard())) {
                    if (replica.role() == ShardRole.REPLICA && replica.state() == ShardState.STARTED
                        && replica.allocationId() != null) {
                        inSync.add(replica.allocationId());
                    }
                }
            } catch (RuntimeException e) {
                // An unreadable projection says nothing about the replicas; the primary is still in sync with itself.
            }
        });
        return copy.withReplicationIds(allocationId, List.copyOf(inSync));
    }

    /** Whether the inventory now says exactly what the assignment asked for: right role, and serving. */
    private static boolean matches(Map<Desired, ShardInventory.ShardCopy> next, Map<Desired, ShardRole> desired) {
        for (Map.Entry<Desired, ShardRole> entry : desired.entrySet()) {
            ShardInventory.ShardCopy copy = next.get(entry.getKey());
            if (copy == null || copy.role() != entry.getValue() || copy.state() != ShardState.STARTED) {
                return false;
            }
        }
        return next.size() == desired.size();
    }


    private Map<Desired, ShardInventory.ShardCopy> flatten(ShardInventory from) {
        Map<Desired, ShardInventory.ShardCopy> flat = new TreeMap<>();
        from.tenants().forEach((tenant, tenantShards) ->
            tenantShards.indices().forEach((uuid, index) ->
                index.shards().forEach((shard, copy) ->
                    flat.put(new Desired(tenant, uuid, index.name(), index.configRevision(), shard), copy)
                )
            )
        );
        return flat;
    }

    private ShardInventory rebuild(Map<Desired, ShardInventory.ShardCopy> copies) {
        Map<TenantId, Map<String, IndexBuilder>> byTenant = new TreeMap<>();
        copies.forEach((where, copy) -> byTenant
            .computeIfAbsent(where.tenant(), t -> new TreeMap<>())
            .computeIfAbsent(where.indexUuid(), u -> new IndexBuilder(where.indexName(), where.configRevision()))
            .shards.put(where.shard(), copy));

        Map<TenantId, ShardInventory.TenantShards> tenants = new LinkedHashMap<>();
        byTenant.forEach((tenant, indices) -> {
            Map<String, ShardInventory.IndexShards> built = new LinkedHashMap<>();
            indices.forEach((uuid, builder) ->
                built.put(uuid, new ShardInventory.IndexShards(builder.name, builder.configRevision, builder.shards)));
            tenants.put(tenant, new ShardInventory.TenantShards(built));
        });

        return new ShardInventory(nodeId, ephemeralId, clock.millis(), tenants);
    }

    private static final class IndexBuilder {
        private final String name;
        private final long configRevision;
        private final Map<Integer, ShardInventory.ShardCopy> shards = new TreeMap<>();

        IndexBuilder(String name, long configRevision) {
            this.name = name;
            this.configRevision = configRevision;
        }
    }

    /** Deterministic, so a test can assert a digest without pinning a random number. */
    private long simulatedSize(Desired where) {
        Long pinned = shardSizes.get(where.shard());
        return pinned != null
            ? pinned
            : 1_000_000L + Math.abs((where.indexUuid() + where.shard()).hashCode() % 100_000);
    }

    private void heartbeat(ShardInventory current) {
        Heartbeat heartbeat = new Heartbeat(
            nodeId,
            ephemeralId,
            clock.millis(),
            new Heartbeat.Applied(appliedVersion, outcome, failureDetail),
            inventoryDigest,
            current.copyCount(),
            new Heartbeat.Capacity(994_663_170_048L, 856_882_249_728L, 41, 5, 1.2)
        );
        store.put(Keys.nodeHeartbeat(nodeId), NodeJson.write(heartbeat));
    }

    // ---------------------------------------------------------------- for tests

    public NodeId nodeId() {
        return nodeId;
    }

    public String ephemeralId() {
        return ephemeralId;
    }

    public ShardInventory inventory() {
        return inventory;
    }
}
