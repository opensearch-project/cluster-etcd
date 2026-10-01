package org.opensearch.supervisor.model.node;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.PoolId;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * {@code nodes/registration/<node-id>} — written by the node when it starts, and rewritten only if
 * something in it changes.
 *
 * <p>The slowest-changing of the three documents a node writes. {@code NODE_CONTRACT.md} splits them by
 * change frequency so that the document written every few seconds is not also the document that grows with
 * shard count.
 *
 * @param pool the pool this node was deployed into, from its own configuration. May be {@code null}: a node
 *     is allowed not to declare one, and {@code pools/nodes/<node-id>} overrides it in any case. Absent
 *     from both is {@code NODE_UNPOOLED} — a finding, never a default.
 * @param ephemeralId changes on every process start, which is how a restart is detected without comparing
 *     anything else
 * @param contractFeatures the optional parts of {@code NODE_CONTRACT.md} this node acts on. The supervisor uses it to
 *     decide what it may safely ask, because {@code applied} cannot: a node that ignores an unknown assignment field
 *     still reports {@code applied}. Unknown strings are kept and ignored, so a newer node is not misread.
 */
public record Registration(
    NodeId nodeId,
    String ephemeralId,
    String name,
    PoolId pool,
    Set<NodeRole> roles,
    String address,
    int port,
    String version,
    Map<String, String> attributes,
    long heartbeatIntervalMillis,
    long registeredAt,
    Set<String> contractFeatures
) {

    /** The contract's bound on awareness attributes, because the control plane renders them in a response. */
    public static final int MAX_ATTRIBUTES = 32;

    public static final int MAX_ATTRIBUTE_LENGTH = 128;

    /** Without contract features: a node that implements none of the optional parts of the contract. */
    public Registration(
        NodeId nodeId,
        String ephemeralId,
        String name,
        PoolId pool,
        Set<NodeRole> roles,
        String address,
        int port,
        String version,
        Map<String, String> attributes,
        long heartbeatIntervalMillis,
        long registeredAt
    ) {
        this(nodeId, ephemeralId, name, pool, roles, address, port, version, attributes, heartbeatIntervalMillis,
            registeredAt, Set.of());
    }

    public Registration {
        Objects.requireNonNull(nodeId, "nodeId");
        contractFeatures = contractFeatures == null ? Set.of() : Set.copyOf(new java.util.TreeSet<>(contractFeatures));
        Objects.requireNonNull(ephemeralId, "ephemeralId");
        roles = roles == null || roles.isEmpty() ? EnumSet.noneOf(NodeRole.class) : EnumSet.copyOf(roles);
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);

        if (attributes.size() > MAX_ATTRIBUTES) {
            throw new IllegalArgumentException(
                "node [" + nodeId + "] declares " + attributes.size() + " attributes, over the limit of " + MAX_ATTRIBUTES
            );
        }
        attributes.forEach((key, value) -> {
            if (key.length() > MAX_ATTRIBUTE_LENGTH || value.length() > MAX_ATTRIBUTE_LENGTH) {
                throw new IllegalArgumentException(
                    "node [" + nodeId + "] attribute [" + key + "] exceeds " + MAX_ATTRIBUTE_LENGTH + " characters"
                );
            }
        });
        if (heartbeatIntervalMillis <= 0) {
            throw new IllegalArgumentException(
                "node [" + nodeId + "] declares a heartbeat interval of " + heartbeatIntervalMillis
                    + "ms; the supervisor derives this node's staleness threshold from it, so it must be positive"
            );
        }
    }

    public Optional<PoolId> declaredPool() {
        return Optional.ofNullable(pool);
    }

    public boolean hasRole(NodeRole role) {
        return roles.contains(role);
    }

    public boolean declares(String contractFeature) {
        return contractFeatures.contains(contractFeature);
    }

    /** The same registration declaring these features, for a node that implements them. */
    public Registration withContractFeatures(Set<String> features) {
        return new Registration(nodeId, ephemeralId, name, pool, roles, address, port, version, attributes,
            heartbeatIntervalMillis, registeredAt, features);
    }
}
