/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.core.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;

/**
 * Represents a remote node in the cluster. For now, we can probably get by
 * with just a transport ID and unique IDs.
 * <p>
 * TODO: We'll assume that remote node names, IDs, and ephemeral IDs are all the same.
 * Hopefully that won't cause any problems.
 *
 * @param nodeId  the name, ID, and ephemeral ID for the remote node
 * @param address the IP address (IPv4 or IPv6) of the remote node
 * @param port    the port to connect to on the remote node (usually 9300)
 */
public record RemoteNode(String nodeName, String nodeId, String ephemeralId, String address, int port) {

    public DiscoveryNode toDiscoveryNode() {
        try {
            return new DiscoveryNode(
                nodeName(),
                nodeId(),
                ephemeralId(),
                address(),
                address(),
                new TransportAddress(new InetSocketAddress(InetAddress.getByAddress(InetAddresses.ipStringToBytes(address())), port())),
                Collections.emptyMap(),
                Set.of(DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid address for remote node: " + address, e);
        }

    }
}
