/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.etcd.changeapplier;

/**
 * Represents a remote node in the cluster. For now, we can probably get by
 * with just a transport ID and unique IDs.
 * <p>
 * TODO: We'll assume that remote node names, IDs, and ephemeral IDs are all the same.
 * Hopefully that won't cause any problems.
 *
 * @param nodeId     the unique identifier for the remote node
 * @param ephemeralId the ephemeral ID for the remote node
 * @param address    the IP address (IPv4 or IPv6) of the remote node
 * @param port       the port to connect to on the remote node (usually 9300)
 */
public record RemoteNode(String nodeId, String ephemeralId, String address, int port) {
}
