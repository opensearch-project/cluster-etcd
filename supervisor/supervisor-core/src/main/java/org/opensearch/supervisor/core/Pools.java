package org.opensearch.supervisor.core;

import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.pool.PoolBinding;
import org.opensearch.supervisor.model.pool.PoolDefinition;
import org.opensearch.supervisor.model.pool.PoolKind;
import org.opensearch.supervisor.model.pool.PoolNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Reads the three pool documents and answers the two questions the partition model rests on: which pool
 * owns a node, and which pool serves a tenant.
 *
 * <p>Every single-writer property in {@code DESIGN.md} §2 follows from those two answers being unique, so
 * this class is where "no key has two writers" is actually decided.
 *
 * <p>Reading all three is a torn read like any other, and the failures it can produce are ordinary: a node
 * briefly attributed to its old pool, or a tenant to its old one. Both are corrected next pass, and the
 * compare-and-set on every write is what stops the transient from doing damage. §12 has the audit.
 */
public final class Pools {

    private final Map<PoolId, PoolDefinition> definitions;
    private final Map<TenantId, PoolBinding> bindings;
    private final Map<NodeId, PoolId> nodeOverrides;
    private final List<String> unreadable;
    private final PoolId defaultPool;
    private final List<String> problems;

    private Pools(
        Map<PoolId, PoolDefinition> definitions,
        Map<TenantId, PoolBinding> bindings,
        Map<NodeId, PoolId> nodeOverrides,
        List<String> unreadable,
        PoolId defaultPool,
        List<String> problems
    ) {
        this.definitions = Map.copyOf(definitions);
        this.bindings = Map.copyOf(bindings);
        this.nodeOverrides = Map.copyOf(nodeOverrides);
        this.unreadable = List.copyOf(unreadable);
        this.defaultPool = defaultPool;
        this.problems = List.copyOf(problems);
    }

    public static Pools read(KeyValueStore store) {
        Objects.requireNonNull(store, "store");
        List<String> unreadable = new ArrayList<>();
        List<String> problems = new ArrayList<>();

        Map<PoolId, PoolDefinition> definitions = new LinkedHashMap<>();
        PoolId defaultPool = null;
        for (var entry : store.scanPrefix(Keys.POOL_DEFINITIONS_PREFIX).entrySet()) {
            String id = Keys.idAfter(Keys.POOL_DEFINITIONS_PREFIX, entry.getKey());
            try {
                PoolDefinition definition = PoolDefinition.from(entry.getValue().value());
                definitions.put(PoolId.of(id), definition);
                if (definition.isDefault()) {
                    if (defaultPool != null) {
                        // Two defaults would make the destination of an unbound tenant depend on scan
                        // order, which is a coin flip dressed up as configuration.
                        problems.add("pools [" + defaultPool + "] and [" + id + "] both claim to be default;"
                            + " ignoring both, because choosing by scan order is worse than not choosing");
                        defaultPool = null;
                    } else {
                        defaultPool = PoolId.of(id);
                    }
                }
            } catch (RuntimeException e) {
                unreadable.add(entry.getKey());
            }
        }

        Map<TenantId, PoolBinding> bindings = new LinkedHashMap<>();
        for (var entry : store.scanPrefix(Keys.POOL_BINDINGS_PREFIX).entrySet()) {
            String id = Keys.idAfter(Keys.POOL_BINDINGS_PREFIX, entry.getKey());
            try {
                bindings.put(TenantId.of(id), PoolBinding.from(entry.getValue().value()));
            } catch (RuntimeException e) {
                unreadable.add(entry.getKey());
            }
        }

        Map<NodeId, PoolId> overrides = new LinkedHashMap<>();
        for (var entry : store.scanPrefix(Keys.POOL_NODES_PREFIX).entrySet()) {
            String id = Keys.idAfter(Keys.POOL_NODES_PREFIX, entry.getKey());
            try {
                overrides.put(NodeId.of(id), PoolNode.from(entry.getValue().value()).pool());
            } catch (RuntimeException e) {
                unreadable.add(entry.getKey());
            }
        }

        return new Pools(definitions, bindings, overrides, unreadable, defaultPool, problems);
    }

    /**
     * Which pool owns a node.
     *
     * <p>Precedence is stated once in {@code NODE_CONTRACT.md} and implemented once here: the operator's
     * {@code pools/nodes/<id>} override wins, then the pool the node declared in its own registration, and
     * a pool with no definition counts as none at all — a typo in a deployment config should strand one
     * node visibly rather than invent a pool.
     */
    public Optional<PoolId> poolOf(NodeId node, PoolId declared) {
        PoolId chosen = nodeOverrides.getOrDefault(node, declared);
        if (chosen == null || definitions.containsKey(chosen) == false) {
            return Optional.empty();
        }
        return Optional.of(chosen);
    }

    /**
     * Which pool serves a tenant.
     *
     * <p>During a rebind the <em>current</em> pool keeps it. The handover is a single compare-and-set
     * performed by the outgoing owner, which is the only ordering with no instant of two owners, so
     * {@code moving_to} is intent and never yet ownership.
     */
    public Optional<PoolId> poolFor(TenantId tenant) {
        PoolBinding binding = bindings.get(tenant);
        if (binding != null) {
            return Optional.of(binding.pool());
        }
        return Optional.ofNullable(defaultPool);
    }

    public Optional<PoolBinding> bindingOf(TenantId tenant) {
        return Optional.ofNullable(bindings.get(tenant));
    }

    /** Tenants served by a pool, in a stable order so that a bounded pass is a fair pass. */
    public Set<TenantId> tenantsOf(PoolId pool) {
        Set<TenantId> tenants = new LinkedHashSet<>();
        bindings.forEach((tenant, binding) -> {
            if (binding.pool().equals(pool)) {
                tenants.add(tenant);
            }
        });
        return tenants;
    }

    /** Tenants that exist but no pool serves — entirely unmanaged, and silent everywhere else. */
    public Set<TenantId> unboundOf(Set<TenantId> known) {
        Set<TenantId> unbound = new LinkedHashSet<>();
        for (TenantId tenant : known) {
            if (poolFor(tenant).isEmpty()) {
                unbound.add(tenant);
            }
        }
        return unbound;
    }

    /** A second tenant bound to a dedicated pool is refused rather than silently making it shared. */
    public List<String> overloadedDedicatedPools() {
        List<String> overloaded = new ArrayList<>();
        definitions.forEach((pool, definition) -> {
            if (definition.kind() == PoolKind.DEDICATED && tenantsOf(pool).size() > 1) {
                overloaded.add(pool.value() + " is dedicated but serves " + tenantsOf(pool).size() + " tenants");
            }
        });
        return overloaded;
    }

    public Set<PoolId> all() {
        return definitions.keySet();
    }

    public Optional<PoolDefinition> definitionOf(PoolId pool) {
        return Optional.ofNullable(definitions.get(pool));
    }

    public Optional<PoolId> defaultPool() {
        return Optional.ofNullable(defaultPool);
    }

    /** Keys under the pool prefixes that could not be parsed; skipped rather than failing the pass. */
    public List<String> unreadable() {
        return unreadable;
    }

    /** Configuration mistakes worth reporting that are not about any one key. */
    public List<String> problems() {
        return problems;
    }
}
