package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.Assignment;
import org.opensearch.supervisor.model.node.Heartbeat;
import org.opensearch.supervisor.model.node.NodeJson;
import org.opensearch.supervisor.model.node.Registration;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Liveness;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Reads the store into an {@link Observation} for one pool. The only class in this service that turns keys
 * into facts, which is what lets everything downstream be a pure function.
 *
 * <p>Three things it is careful about, all of them about cost or about not lying:
 *
 * <ul>
 *   <li><b>It re-parses only what changed.</b> Revisions compare for equality on the same key, so a
 *       {@link RevisionCache} keyed on them bounds the work per pass without any staleness risk.
 *   <li><b>It reads almost no shard inventories.</b> The heartbeat announces a digest of the inventory's
 *       exact bytes, so an inventory is fetched only when that digest differs from the one this supervisor
 *       computed over the bytes it last read. This is the payoff for the three-document split, and in
 *       steady state a pass reads one small scan and nothing else.
 *   <li><b>One unreadable key does not stop the pass.</b> R2.5: torn or foreign data under our prefixes is
 *       normal and never corruption. A key that cannot be parsed is collected and reported as a finding,
 *       so a single bad document cannot prevent every other tenant from being reconciled.
 * </ul>
 *
 * <p>An observation is <b>not a snapshot</b> — see {@link Observation} and {@code DESIGN.md} §12.
 */
public final class Observer {

    private final KeyValueStore store;
    private final Clock clock;
    private final LivenessTracker liveness;

    private final RevisionCache<Registration> registrations = new RevisionCache<>();
    private final RevisionCache<Heartbeat> heartbeats = new RevisionCache<>();
    private final RevisionCache<IndexConfig> indices = new RevisionCache<>();
    private final RevisionCache<Assignment> assignments = new RevisionCache<>();

    /** Inventories are cached by the digest their bytes hashed to, not by revision — see below. */
    private final Map<NodeId, DigestedInventory> inventories = new HashMap<>();

    private record DigestedInventory(String digest, ShardInventory inventory) {}

    private long pass;

    public Observer(KeyValueStore store, Clock clock, LivenessTracker liveness) {
        this.store = Objects.requireNonNull(store, "store");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.liveness = Objects.requireNonNull(liveness, "liveness");
    }

    /**
     * One pass's view of one pool.
     *
     * @param pools already read, because the caller needs them to decide which pools it owns before it
     *     decides to observe any of them
     */
    public Observation observe(PoolId pool, Pools pools, Set<TenantId> knownTenants) {
        long currentPass = ++pass;
        Instant now = clock.instant();
        List<String> unreadable = new ArrayList<>(pools.unreadable());

        Map<NodeId, Observation.NodeObservation> nodes = observeNodes(pool, pools, now, unreadable);
        Map<TenantId, Observation.DesiredState> tenants = observeTenants(pool, pools, knownTenants, unreadable);

        return new Observation(currentPass, now, pool, tenants, nodes, unreadable);
    }

    // ---------------------------------------------------------------- nodes

    private Map<NodeId, Observation.NodeObservation> observeNodes(
        PoolId pool,
        Pools pools,
        Instant now,
        List<String> unreadable
    ) {
        // Registrations first: they say which pool a node declared, and they are small and rarely change.
        Map<NodeId, Registration> registered = new LinkedHashMap<>();
        var registrationKeys = store.scanPrefix(Keys.NODE_REGISTRATION_PREFIX);
        registrations.retain(registrationKeys.keySet());
        for (var entry : registrationKeys.entrySet()) {
            NodeId nodeId = nodeIdOf(Keys.NODE_REGISTRATION_PREFIX, entry.getKey(), unreadable);
            if (nodeId == null) {
                continue;
            }
            try {
                registered.put(nodeId, registrations.get(
                    entry.getKey(), entry.getValue().revision(),
                    key -> NodeJson.readRegistration(entry.getValue().value())
                ));
            } catch (RuntimeException e) {
                registrations.forget(entry.getKey());
                unreadable.add(entry.getKey());
            }
        }

        // The hot scan. Grouped by kind precisely so this returns heartbeats and not inventories.
        var heartbeatKeys = store.scanPrefix(Keys.NODE_HEARTBEAT_PREFIX);
        heartbeats.retain(heartbeatKeys.keySet());

        // What each node was last told. Rarely changes, so the revision cache keeps this near-free, and it
        // is needed for two distinct reasons: an assignment is a complete desired set so a write has to
        // preserve the rest of it, and its revision is what every write compare-and-sets against.
        var assignmentKeys = store.scanPrefix(Keys.NODE_ASSIGNMENT_PREFIX);
        assignments.retain(assignmentKeys.keySet());

        Map<NodeId, Observation.NodeObservation> observed = new LinkedHashMap<>();
        Set<NodeId> seen = new java.util.LinkedHashSet<>();

        for (var entry : heartbeatKeys.entrySet()) {
            NodeId nodeId = nodeIdOf(Keys.NODE_HEARTBEAT_PREFIX, entry.getKey(), unreadable);
            if (nodeId == null) {
                continue;
            }
            seen.add(nodeId);
            Registration registration = registered.get(nodeId);

            // Only this pool's nodes. A node in another pool is another supervisor's business, and reading
            // its inventory would be work done for someone else.
            PoolId nodePool = pools.poolOf(nodeId, registration == null ? null : registration.pool()).orElse(null);
            boolean unpooled = nodePool == null;
            if (unpooled == false && nodePool.equals(pool) == false) {
                continue;
            }

            Heartbeat heartbeat;
            try {
                heartbeat = heartbeats.get(
                    entry.getKey(), entry.getValue().revision(),
                    key -> NodeJson.readHeartbeat(entry.getValue().value())
                );
            } catch (RuntimeException e) {
                heartbeats.forget(entry.getKey());
                unreadable.add(entry.getKey());
                continue;
            }

            Liveness verdict = liveness.observe(nodeId, entry.getValue().revision(), registration, now);
            ShardInventory inventory = inventoryOf(nodeId, heartbeat, registration, unreadable);

            observed.put(nodeId, new Observation.NodeObservation(
                nodeId, registration, heartbeat, inventory, verdict, unpooled ? null : nodePool,
                assignmentOf(nodeId, assignmentKeys, unreadable),
                revisionOf(nodeId, assignmentKeys)
            ));
        }

        // A node that has registered but never heartbeated is still this pool's problem: it is either
        // starting up or it is broken, and both are worth seeing. Omitting it would make a node that fails
        // before its first heartbeat completely invisible.
        registered.forEach((nodeId, registration) -> {
            if (seen.contains(nodeId)) {
                return;
            }
            PoolId nodePool = pools.poolOf(nodeId, registration.pool()).orElse(null);
            if (nodePool == null || nodePool.equals(pool) == false) {
                return;
            }
            observed.put(nodeId, new Observation.NodeObservation(
                nodeId, registration, null, null,
                liveness.observeAbsent(nodeId, registration, now), nodePool,
                assignmentOf(nodeId, assignmentKeys, unreadable),
                revisionOf(nodeId, assignmentKeys)
            ));
        });

        liveness.retain(seen);
        liveness.retainAllKnown(observed.keySet());
        inventories.keySet().retainAll(seen);
        return observed;
    }

    private Assignment assignmentOf(
        NodeId nodeId,
        java.util.NavigableMap<String, org.opensearch.controlplane.store.KeyValueStore.Versioned> keys,
        List<String> unreadable
    ) {
        String key = Keys.nodeAssignment(nodeId);
        var versioned = keys.get(key);
        if (versioned == null) {
            return null;
        }
        try {
            return assignments.get(key, versioned.revision(), ignored -> NodeJson.readAssignment(versioned.value()));
        } catch (RuntimeException e) {
            // An unreadable assignment must not be read as an empty one: writing over it with a fresh
            // desired set computed as if the node held nothing is how a corrupt key becomes lost shards.
            assignments.forget(key);
            unreadable.add(key);
            return null;
        }
    }

    private long revisionOf(
        NodeId nodeId,
        java.util.NavigableMap<String, org.opensearch.controlplane.store.KeyValueStore.Versioned> keys
    ) {
        var versioned = keys.get(Keys.nodeAssignment(nodeId));
        return versioned == null ? org.opensearch.controlplane.store.KeyValueStore.ABSENT : versioned.revision();
    }

    /**
     * The digest handshake, and the reason a steady-state pass is cheap.
     *
     * <p>The supervisor caches the digest of the bytes it last read for a node. If the heartbeat announces
     * the same one, the inventory has not changed and is not fetched. A mismatch always means the heartbeat
     * is behind or ahead by one write, never that the cached copy is wrong — the node writes its inventory
     * before the heartbeat that announces it, which is the whole point of that ordering rule.
     */
    private ShardInventory inventoryOf(
        NodeId nodeId,
        Heartbeat heartbeat,
        Registration registration,
        List<String> unreadable
    ) {
        DigestedInventory cached = inventories.get(nodeId);
        String announced = heartbeat.shardsDigest();

        if (cached != null && cached.digest().equals(announced)) {
            return cached.inventory();
        }

        String key = Keys.nodeShards(nodeId);
        Optional<KeyValueStore.Versioned> stored = store.get(key);
        if (stored.isEmpty()) {
            inventories.remove(nodeId);
            return null;
        }

        try {
            byte[] bytes = stored.get().value();
            ShardInventory inventory = NodeJson.readInventory(bytes);

            // Straddling a restart: the previous process's heartbeat with the new process's inventory. Both
            // documents carry the ephemeral id, so it is detectable; prefer the inventory, which within a
            // process is never the older of the two.
            if (registration != null
                && inventory.ephemeralId().equals(heartbeat.ephemeralId()) == false) {
                inventories.remove(nodeId);
            } else {
                inventories.put(nodeId, new DigestedInventory(NodeJson.digest(bytes), inventory));
            }
            return inventory;
        } catch (RuntimeException e) {
            inventories.remove(nodeId);
            unreadable.add(key);
            return null;
        }
    }

    // ---------------------------------------------------------------- tenants

    private Map<TenantId, Observation.DesiredState> observeTenants(
        PoolId pool,
        Pools pools,
        Set<TenantId> knownTenants,
        List<String> unreadable
    ) {
        Map<TenantId, Observation.DesiredState> tenants = new LinkedHashMap<>();
        for (TenantId tenant : knownTenants) {
            if (pools.poolFor(tenant).filter(pool::equals).isEmpty()) {
                continue;
            }
            tenants.put(tenant, new Observation.DesiredState(
                indicesOf(tenant, unreadable), tasksOf(tenant, unreadable)
            ));
        }
        return tenants;
    }

    private Map<String, IndexConfig> indicesOf(TenantId tenant, List<String> unreadable) {
        String prefix = Keys.indicesPrefix(tenant);
        Map<String, IndexConfig> byUuid = new LinkedHashMap<>();

        for (var entry : store.scanPrefix(prefix).entrySet()) {
            try {
                IndexConfig config = indices.get(
                    entry.getKey(), entry.getValue().revision(),
                    key -> IndexConfig.parse(entry.getValue().value(), entry.getValue().revision())
                );
                byUuid.put(config.uuid(), config);
            } catch (RuntimeException e) {
                indices.forget(entry.getKey());
                unreadable.add(entry.getKey());
            }
        }
        return byUuid;
    }

    /**
     * The tenant's task records.
     *
     * <p>Not cached by revision, unlike the index documents: these change on almost every pass while the queue
     * is draining, and a cache that misses every time costs more than it saves. They are small and few.
     */
    private List<org.opensearch.supervisor.model.task.TaskRecord> tasksOf(TenantId tenant, List<String> unreadable) {
        List<org.opensearch.supervisor.model.task.TaskRecord> tasks = new ArrayList<>();
        for (var entry : store.scanPrefix(Keys.tasksPrefix(tenant)).entrySet()) {
            try {
                tasks.add(org.opensearch.supervisor.model.task.TaskRecord.from(entry.getValue().value()));
            } catch (RuntimeException e) {
                unreadable.add(entry.getKey());
            }
        }
        return tasks;
    }

    // ---------------------------------------------------------------- helpers

    private NodeId nodeIdOf(String prefix, String key, List<String> unreadable) {
        try {
            return NodeId.of(Keys.idAfter(prefix, key));
        } catch (RuntimeException e) {
            unreadable.add(key);
            return null;
        }
    }

    /** For the convergence document: how much of this pass was served from cache. */
    public int cachedParses() {
        return registrations.hits() + heartbeats.hits() + indices.hits();
    }

    public int freshParses() {
        return registrations.misses() + heartbeats.misses() + indices.misses();
    }

    public long pass() {
        return pass;
    }
}
