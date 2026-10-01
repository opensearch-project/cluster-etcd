/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

import org.opensearch.clusterless.contract.Heartbeat;

/**
 * What allocation needs to know about this node's headroom.
 *
 * <p>A seam mostly so that the publisher can be tested without a running node, but also because the probes
 * behind it are the one part of a heartbeat that can fail on its own: reading filesystem statistics is a
 * syscall that can block or throw while everything else about the node is fine. A failed probe must not
 * cost a heartbeat, because a missing heartbeat is what liveness is computed from — so an implementation
 * reports {@link Heartbeat.Capacity#UNKNOWN} rather than throwing.
 */
@FunctionalInterface
public interface CapacityProbe {

    Heartbeat.Capacity capacity();

    CapacityProbe UNKNOWN = () -> Heartbeat.Capacity.UNKNOWN;
}
