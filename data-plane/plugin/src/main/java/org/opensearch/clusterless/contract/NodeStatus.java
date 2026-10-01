/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * {@code tenant/<t>/status/nodes/<node-id>} — where a peer is, and enough about it to build a
 * {@code DiscoveryNode} the transport will accept.
 *
 * <p>Written by the supervisor from what a node registered, and read here rather than reading that node's own
 * registration. The contract argues the difference and it is not a stylistic one: reading each peer's document
 * would make every node a reader of every other node's key, couple this plugin to the heartbeat schema, and give a
 * single-writer document a class of reader its author never considers.
 *
 * <p><b>{@code ephemeralId} is why this document can be used at all, and it was missing.</b> Found by trying:
 * {@code DiscoveryNode.equals} compares <em>only</em> the ephemeral id, and {@code TransportService}'s connection
 * validator handshakes a new connection and then refuses it unless the node it was given equals the node the
 * remote reports itself to be. So address and port are necessary and not sufficient — a peer built without the
 * right ephemeral id is unequal to the real one by construction, and every connection to it fails. There is no
 * workaround available to a reader, which is why the supervisor now projects it.
 *
 * <p>It changes on every restart, by design: it identifies an <em>incarnation</em>, which is exactly what the
 * transport wants to pin. A reader that has a stale one cannot connect until the projection catches up, and should
 * report that rather than retry silently.
 */
public record NodeStatus(
    NodeId nodeId,
    String ephemeralId,
    String name,
    Set<NodeRole> roles,
    String address,
    int port,
    String version,
    Map<String, String> attributes,
    Liveness liveness
) {

    public NodeStatus {
        Objects.requireNonNull(nodeId, "nodeId");
        roles = roles == null || roles.isEmpty() ? EnumSet.noneOf(NodeRole.class) : EnumSet.copyOf(roles);
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        liveness = liveness == null ? Liveness.UNKNOWN : liveness;
    }

    /**
     * How alive the supervisor last thought this node was.
     *
     * <p>{@code STALE} is reachable on purpose: a node whose heartbeat is late is usually still serving, and
     * refusing to route to it would turn a slow store into an outage. {@code LOST} is not — the supervisor has
     * given up on it and its copies are being placed elsewhere.
     */
    public enum Liveness {
        LIVE,
        STALE,
        LOST,
        UNKNOWN;

        public static Liveness from(String value) {
            if (value == null) {
                return UNKNOWN;
            }
            return switch (value.trim().toUpperCase(Locale.ROOT)) {
                case "LIVE" -> LIVE;
                case "STALE" -> STALE;
                case "LOST" -> LOST;
                default -> UNKNOWN;
            };
        }
    }

    /**
     * Whether this node is worth sending anything to.
     *
     * <p>{@code UNKNOWN} counts as reachable, which is the same tolerance every other enum in this package gets: a
     * liveness this build does not recognise comes from a supervisor newer than it, and treating that as "do not
     * route" would make an upgrade look like an outage.
     */
    public boolean isReachable() {
        return liveness != Liveness.LOST;
    }

    public boolean holdsData() {
        return roles.contains(NodeRole.DATA);
    }

    /** Whether this carries enough to build a peer the transport will connect to. */
    public boolean isAddressable() {
        return ephemeralId != null
            && ephemeralId.isEmpty() == false
            && address != null
            && address.isEmpty() == false
            && port > 0;
    }

    public static NodeStatus read(byte[] bytes) {
        String what = "node status";
        Map<String, Object> document = Documents.parse(bytes, what);

        Set<NodeRole> roles = EnumSet.noneOf(NodeRole.class);
        Object declared = document.get("roles");
        if (declared instanceof Iterable<?> values) {
            for (Object value : values) {
                roles.add(NodeRole.from(value == null ? null : String.valueOf(value)));
            }
        }

        return new NodeStatus(
            NodeId.of(Documents.requireString(document, "node_id", what)),
            Documents.string(document, "ephemeral_id", null),
            Documents.string(document, "name", null),
            roles,
            Documents.string(document, "address", null),
            (int) Documents.number(document, "port", 0, what),
            Documents.string(document, "version", null),
            Documents.stringMap(document, "attributes"),
            Liveness.from(Documents.string(document, "liveness", null))
        );
    }
}
