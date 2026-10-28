/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * {@code nodes/registration/<node-id>} — written when the node starts, and rewritten only if something in
 * it changes.
 *
 * <p>The slowest-changing of the three documents this plugin writes. The split by change frequency is what
 * keeps the document that grows with shard count off the every-few-seconds write path.
 *
 * @param ephemeralId fresh per process, which is how the supervisor tells a restart from a continuous run
 * @param pool the pool this node was deployed into, from its own configuration. May be {@code null}: a
 *     node is allowed not to declare one, and {@code pools/nodes/<node-id>} overrides it in any case.
 *     Absent from both is {@code NODE_UNPOOLED} — a finding, never a default.
 * @param heartbeatIntervalMillis the node declaring its own cadence. The supervisor derives this node's
 *     staleness threshold from it rather than from a global constant, so D3.7 makes it an obligation: a
 *     node declaring 1s and writing every 30s will be declared lost and have its shards rebuilt elsewhere.
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

    /** Bounded because the control plane renders these in an API response. */
    /**
     * What this build implements of the optional contract, and nothing it does not.
     *
     * <p>The supervisor reads this to decide what it may safely ask, and the reason it has to exist is that
     * {@code applied} cannot answer the question: a node that ignores an assignment field it does not understand
     * applies everything else and reports success, so a term it never read looks exactly like a term it honoured.
     * For a promotion that difference is two copies believing they are primary at one term.
     *
     * <p><b>So a feature goes in here only once this node acts on it.</b> Listing one early is not optimism, it is
     * an instruction the supervisor will issue and nothing will carry out.
     */
    public static final Set<String> IMPLEMENTED_FEATURES = Set.of("primary_terms");

    /**
     * A registration that declares no optional features, which is what absence means on the wire.
     *
     * <p>Kept so that a caller with nothing to say about features does not have to say something, and because the
     * honest default is none: a build that has not been asked declares nothing rather than guessing.
     */
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
        this(nodeId, ephemeralId, name, pool, roles, address, port, version, attributes, heartbeatIntervalMillis, registeredAt, Set.of());
    }

    public static final int MAX_ATTRIBUTES = 32;

    public static final int MAX_ATTRIBUTE_LENGTH = 128;

    public Registration {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(ephemeralId, "ephemeralId");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(version, "version");
        roles = roles == null || roles.isEmpty()
            ? Collections.unmodifiableSet(EnumSet.noneOf(NodeRole.class))
            : Collections.unmodifiableSet(EnumSet.copyOf(roles));
        // Sorted, because these bytes are compared against the last ones written to decide whether the
        // document changed at all. An attribute map that iterated differently between two passes would
        // rewrite a document nothing had changed in.
        attributes = attributes == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(attributes));
        // Sorted for the same reason as the attributes, and it matters more here: this set is what the supervisor
        // reads to decide what it may safely ask of this node, so a document rewritten because a set iterated
        // differently would look like the node's capabilities had changed.
        contractFeatures = contractFeatures == null
            ? Set.of()
            : Collections.unmodifiableSet(new java.util.TreeSet<>(contractFeatures));

        if (attributes.size() > MAX_ATTRIBUTES) {
            throw new IllegalArgumentException(
                "node ["
                    + nodeId
                    + "] declares "
                    + attributes.size()
                    + " attributes, over the contract's limit of "
                    + MAX_ATTRIBUTES
                    + "; these are awareness values the control plane renders in an API response, so the bound is theirs"
            );
        }
        for (Map.Entry<String, String> attribute : attributes.entrySet()) {
            if (attribute.getKey().length() > MAX_ATTRIBUTE_LENGTH || attribute.getValue().length() > MAX_ATTRIBUTE_LENGTH) {
                throw new IllegalArgumentException(
                    "node [" + nodeId + "] attribute [" + attribute.getKey() + "] exceeds " + MAX_ATTRIBUTE_LENGTH + " characters"
                );
            }
        }
        if (heartbeatIntervalMillis <= 0) {
            throw new IllegalArgumentException(
                "node ["
                    + nodeId
                    + "] declares a heartbeat interval of "
                    + heartbeatIntervalMillis
                    + "ms; the supervisor derives this node's staleness threshold from it, so it must be positive"
            );
        }
    }

    /**
     * Whether the supervisor would see a different document — everything except when it was written.
     *
     * <p>{@code registeredAt} is excluded deliberately. Including it would make every comparison unequal
     * and rewrite the registration on every pass, which is the same mistake {@code generated_at} on the
     * inventory exists to prevent.
     */
    public boolean sameAs(Registration other) {
        return other != null
            && nodeId.equals(other.nodeId)
            && ephemeralId.equals(other.ephemeralId)
            && name.equals(other.name)
            && Objects.equals(pool, other.pool)
            && roles.equals(other.roles)
            && address.equals(other.address)
            && port == other.port
            && version.equals(other.version)
            && attributes.equals(other.attributes)
            && heartbeatIntervalMillis == other.heartbeatIntervalMillis
            // Included, or an upgrade that gains a feature would not rewrite the document that announces it -- and
            // the supervisor would go on believing this node cannot be promoted onto until something else changed.
            && contractFeatures.equals(other.contractFeatures);
    }
}
