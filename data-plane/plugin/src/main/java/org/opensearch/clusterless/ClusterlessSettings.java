/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless;

import org.opensearch.clusterless.contract.NodeRole;
import org.opensearch.clusterless.contract.PoolId;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * What an operator configures.
 *
 * <p>Named {@code clusterless.*} rather than after a backend, which is the one naming rule the design
 * imposes: {@code cluster-etcd} used {@code cluster.etcd.endpoint}, and a setting that names etcd cannot
 * survive D6.2. The prefix is shared with the control plane and the supervisor so that all three read
 * alike.
 *
 * <p>Every setting here is {@link Setting.Property#NodeScope} and none is dynamic. That is deliberate and
 * is not in tension with D5.5, which is about <em>desired state</em> — an assignment, an index setting, a
 * pool membership — never needing a restart. Which store this node talks to and which pool it was deployed
 * into are deployment facts; changing them under a running node would change who owns it mid-flight.
 */
public final class ClusterlessSettings {

    private ClusterlessSettings() {}

    /**
     * A floor on both intervals.
     *
     * <p>Not a guess at what is sensible — it is a guard against the two ways a very short interval breaks
     * something other than this node. A heartbeat far below this multiplies the store's dominant write
     * load by the size of the fleet, and a staleness threshold derived from it (six times the interval, by
     * default) drops below the length of an ordinary garbage collection pause, so a healthy node is
     * declared lost and has its shards rebuilt elsewhere.
     */
    static final TimeValue MINIMUM_INTERVAL = TimeValue.timeValueMillis(100);

    /**
     * The store URI. One setting names a scheme; the backend is a jar on the classpath found by
     * {@code ServiceLoader}.
     *
     * <p>No default, and required. A default of {@code file:///something} would let a misconfigured node
     * start happily against a private store where it is the only participant, which looks exactly like a
     * healthy node that no supervisor can see.
     */
    public static final Setting<String> STORE = Setting.simpleString("clusterless.store", Setting.Property.NodeScope);

    /**
     * The pool this node was deployed into.
     *
     * <p>Optional, because {@code pools/nodes/<node-id>} overrides it and an operator may prefer to place
     * nodes entirely from the store. Absent from both is {@code NODE_UNPOOLED}: the node registers, gets no
     * shards, and is visible as a finding — which is the right answer to a typo in a deployment config,
     * and much better than inventing a pool.
     */
    public static final Setting<String> POOL = Setting.simpleString("clusterless.node.pool", Setting.Property.NodeScope);

    /**
     * What this node may be given, overriding the derivation from OpenSearch's own {@code node.roles}.
     *
     * <p>The derivation is in {@link #rolesFor}. It exists because the two role vocabularies do not line
     * up: OpenSearch has no {@code coordinator} role — a coordinating-only node is one with no roles at
     * all — while the contract needs to say "this node takes routing and no copies" explicitly.
     */
    public static final Setting<List<String>> ROLES = Setting.listSetting(
        "clusterless.node.roles",
        List.of(),
        Function.identity(),
        Setting.Property.NodeScope
    );

    /**
     * How often the heartbeat is written.
     *
     * <p>D3.7 makes this an obligation rather than a preference: the value is published in the
     * registration, and the supervisor derives this node's staleness threshold from it. A node declaring
     * 1s and writing every 30s will be declared lost and have its shards rebuilt elsewhere.
     */
    public static final Setting<TimeValue> HEARTBEAT_INTERVAL = Setting.timeSetting(
        "clusterless.heartbeat.interval",
        TimeValue.timeValueSeconds(5),
        MINIMUM_INTERVAL,
        Setting.Property.NodeScope
    );

    /**
     * How often the assignment is re-read.
     *
     * <p>A separate number from the heartbeat's, and separately configurable, because the two costs are
     * different: the heartbeat is a write by every node in the fleet and the poll is a read of one small
     * key. Defaulting to the same value keeps the common case simple, and a deployment that wants faster
     * convergence can shorten the poll without tripling the store's write load.
     *
     * <p>Nothing reads it yet — the assignment reader is the next milestone — but it is declared here so
     * that the two intervals are visibly independent from the start rather than accidentally coupled by
     * one being introduced as "the interval".
     */
    public static final Setting<TimeValue> POLL_INTERVAL = Setting.timeSetting(
        "clusterless.poll.interval",
        HEARTBEAT_INTERVAL,
        atLeast("clusterless.poll.interval", MINIMUM_INTERVAL),
        Setting.Property.NodeScope
    );

    private static Setting.Validator<TimeValue> atLeast(String key, TimeValue minimum) {
        return value -> {
            if (value.compareTo(minimum) < 0) {
                throw new IllegalArgumentException("[" + key + "] must be at least " + minimum + ", was " + value);
            }
        };
    }

    public static List<Setting<?>> all() {
        return List.of(STORE, POOL, ROLES, HEARTBEAT_INTERVAL, POLL_INTERVAL);
    }

    public static String storeUri(Settings settings) {
        String uri = STORE.get(settings);
        if (uri == null || uri.isEmpty()) {
            throw new IllegalArgumentException(
                "["
                    + STORE.getKey()
                    + "] is required: this plugin has no cluster manager to learn desired state from, so a node "
                    + "without a store has nowhere to read it and nowhere to publish what it holds"
            );
        }
        return uri;
    }

    /** {@code null} when the node declares no pool, which is a legal and visible state, not an error. */
    public static PoolId pool(Settings settings) {
        String pool = POOL.get(settings);
        return pool == null || pool.isEmpty() ? null : PoolId.of(pool);
    }

    /**
     * What this node may be given.
     *
     * <p>Configured if {@code clusterless.node.roles} is set; otherwise derived from what OpenSearch
     * thinks the node is:
     *
     * <ul>
     *   <li>a node with the {@code data} role takes shard copies, so it is {@code data};
     *   <li>a node with <em>no</em> OpenSearch roles is a coordinating-only node, so it is
     *       {@code coordinator}.
     * </ul>
     *
     * <p>Note what the derivation deliberately does not do: it does not make every node a coordinator.
     * Every OpenSearch node can coordinate a search, so a derivation based on capability would return
     * {@code [data, coordinator]} for the whole fleet and the supervisor could place coordinator duty for
     * any tenant anywhere. The contract's roles are about what a node should be <em>given</em>, and giving
     * a data node a tenant's routing table as well is a decision an operator makes, by setting the roles.
     */
    public static Set<NodeRole> rolesFor(Settings settings, boolean isDataNode, boolean hasAnyOpenSearchRole) {
        List<String> configured = ROLES.get(settings);
        if (configured != null && configured.isEmpty() == false) {
            Set<NodeRole> roles = EnumSet.noneOf(NodeRole.class);
            for (String role : configured) {
                roles.add(parseRole(role));
            }
            return roles;
        }

        Set<NodeRole> derived = EnumSet.noneOf(NodeRole.class);
        if (isDataNode) {
            derived.add(NodeRole.DATA);
        }
        if (hasAnyOpenSearchRole == false) {
            derived.add(NodeRole.COORDINATOR);
        }
        return derived;
    }

    private static NodeRole parseRole(String role) {
        try {
            return NodeRole.valueOf(role.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "["
                    + ROLES.getKey()
                    + "] has an unknown role ["
                    + role
                    + "]; the contract defines data and coordinator"
            );
        }
    }
}
