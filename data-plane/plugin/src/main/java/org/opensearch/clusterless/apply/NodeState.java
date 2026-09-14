/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.cluster.ClusterState;

/**
 * Something that can say what this node's {@link ClusterState} should look like.
 *
 * <p>One implementation per node role — {@link DataNodeState} now, a coordinator's at milestone 8 — and the
 * shape is lifted from {@code cluster-etcd}, where it earned its keep: it makes the fabrication a pure
 * function that a test can drive without a cluster, which is the only way the interesting parts of it are
 * checkable at all.
 *
 * <p>The contract with callers is that this <b>reads nothing and decides nothing</b>. Every store read and
 * every "should this copy be built" question is settled upstream, in {@link DesiredStateResolver}, so that
 * what remains is a translation between two data structures. That separation is what lets the awkward parts —
 * carrying metadata versions forward, keeping a started shard started — be tested against hand-built
 * previous states rather than against a running node.
 */
public interface NodeState {

    /**
     * @param previous the state currently applied, which is {@link ClusterState#EMPTY_STATE} on a fresh
     *     process. It is an input rather than a starting point: several things must be carried forward from it
     *     or the server sees change where there is none.
     */
    ClusterState buildClusterState(ClusterState previous);
}
