/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * What this node should hold, after joining its assignment with the index documents it names.
 *
 * <p>The assignment alone is not enough to build anything: it says which shards of which index uuid, and the
 * settings and mappings live in a document the node reads separately. This is the join, resolved and
 * validated — so that {@link NodeState#buildClusterState} can be a pure function of it and the previous state,
 * with no reads and no decisions left in it.
 *
 * <p><b>{@code deferred} is as important as {@code indices}.</b> Everything the node declined to build is
 * recorded with a reason, rather than silently missing. Most of those reasons are ordinary and temporary — an
 * index document that has not landed yet, a uuid that does not match — and the contract is explicit that torn
 * state is normal. But "ordinary" is not the same as "invisible": a shard that never appears and no line
 * saying why is the failure mode an operator cannot debug, and the supervisor will meanwhile report the shard
 * as unassigned with no idea that this node made a decision about it.
 *
 * @param assignmentVersion the version this was resolved from, echoed on the heartbeat
 */
public record DesiredState(
    long assignmentVersion,
    Map<IndexKey, DesiredIndex> indices,
    List<TenantId> coordinatorTenants,
    List<String> deferred
) {

    public DesiredState {
        indices = indices == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(indices));
        coordinatorTenants = coordinatorTenants == null ? List.of() : List.copyOf(coordinatorTenants);
        deferred = deferred == null ? List.of() : List.copyOf(deferred);
    }

    /** Nothing wanted, because nothing has been read. Distinct from "an assignment that names nothing". */
    public static DesiredState nothing() {
        return new DesiredState(0, Map.of(), List.of(), List.of());
    }

    /**
     * The identity of an index, and the only thing anything here is keyed on.
     *
     * <p>{@code (tenant, uuid)} and never the name: names are reused, and are only unique within a tenant.
     * Comparable so that the desired state has a stable order, which matters because the fabricated cluster
     * state is compared with its predecessor to decide whether anything changed.
     */
    public record IndexKey(TenantId tenant, String uuid) implements Comparable<IndexKey> {

        public IndexKey {
            Objects.requireNonNull(tenant, "tenant");
            Objects.requireNonNull(uuid, "uuid");
        }

        @Override
        public int compareTo(IndexKey other) {
            int byTenant = tenant.compareTo(other.tenant);
            return byTenant != 0 ? byTenant : uuid.compareTo(other.uuid);
        }

        @Override
        public String toString() {
            return tenant + "/" + uuid;
        }
    }

    /**
     * One index this node holds copies of, with the configuration to build them from.
     *
     * @param config read from the store, already checked against the uuid the assignment named
     * @param shards the copies this node should hold, and what each should be
     */
    public record DesiredIndex(
        IndexKey key,
        IndexConfig config,
        Map<Integer, ShardRole> shards,
        Map<Integer, Long> primaryTerms
    ) {

        public DesiredIndex {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(config, "config");
            shards = shards == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(shards));
            primaryTerms = primaryTerms == null ? Map.of() : Collections.unmodifiableMap(new TreeMap<>(primaryTerms));
        }

        /** An index whose terms the supervisor has not stated, which is every one until it issues them. */
        public DesiredIndex(IndexKey key, IndexConfig config, Map<Integer, ShardRole> shards) {
            this(key, config, shards, Map.of());
        }

        /**
         * The same index holding only {@code kept}, with the terms of those shards.
         *
         * <p>How a step that declines some copies must rebuild the index. The three-argument constructor states
         * no terms, which is a claim that the supervisor issued none, and a promoted primary rebuilt that way went
         * into the fabrication at term 1.
         */
        public DesiredIndex withShards(Map<Integer, ShardRole> kept) {
            Map<Integer, Long> terms = new TreeMap<>(primaryTerms);
            terms.keySet().retainAll(kept.keySet());
            return new DesiredIndex(key, config, kept, terms);
        }

        /** The term this node's cluster state should name for a shard, or 1 where the supervisor said nothing. */
        public long primaryTermOf(int shard) {
            return primaryTerms.getOrDefault(shard, org.opensearch.clusterless.contract.Assignment.IndexAssignment.TERM_WHEN_UNSTATED);
        }

        public TenantId tenant() {
            return key.tenant();
        }

        public String uuid() {
            return key.uuid();
        }
    }

    public int copyCount() {
        int total = 0;
        for (DesiredIndex index : indices.values()) {
            total += index.shards().size();
        }
        return total;
    }

    public boolean isEmpty() {
        return indices.isEmpty() && coordinatorTenants.isEmpty();
    }
}
