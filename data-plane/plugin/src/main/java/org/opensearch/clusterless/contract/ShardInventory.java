/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * {@code nodes/shards/<node-id>} — ground truth for actual state: what this node <b>has</b>, not what it
 * was told.
 *
 * <p>This is the document the whole design leans on. The supervisor writes assignments but does not know
 * what any node holds; it finds out by reading this. So the inventory has to be the truth even when the
 * truth is embarrassing, and especially when it disagrees with the assignment — a node that reported what
 * it was told to hold rather than what it holds would make the system blind in a way nothing downstream
 * could detect.
 *
 * <p><b>Held flat rather than nested,</b> although the wire form is nested by tenant and index. The flat
 * form is what the two things this record exists for actually need: comparing this pass's copies with the
 * last pass's, which decides whether the document is rewritten at all (D3.4), and counting copies for the
 * heartbeat. {@link org.opensearch.clusterless.contract.NodeDocuments} does the nesting on the way out,
 * once, where the tenant and index grouping is a serialisation concern and nothing else.
 *
 * @param generatedAt <b>when this inventory last changed, not when it was last computed.</b> A node that
 *     stamps the clock on every recomputation produces new bytes and a new digest every interval, which
 *     has the supervisor re-read the document that scales with shard count on every pass and defeats the
 *     three-document split entirely. Found by implementing it once already.
 */
public record ShardInventory(NodeId nodeId, String ephemeralId, long generatedAt, List<HeldCopy> copies) {

    public ShardInventory {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(ephemeralId, "ephemeralId");
        copies = copies == null ? List.of() : List.copyOf(sorted(copies));
    }

    /**
     * An empty inventory is still a document. "I have nothing" and "I have not yet said" are different
     * facts and only one of them is safe to place shards against, so a node with no copies publishes
     * {@code {"tenants":{}}} rather than leaving the key absent.
     */
    public static ShardInventory empty(NodeId nodeId, String ephemeralId, long generatedAt) {
        return new ShardInventory(nodeId, ephemeralId, generatedAt, List.of());
    }

    /** What the heartbeat reports as {@code shard_copies}. */
    public int copyCount() {
        return copies.size();
    }

    /** Whether the supervisor would see different copies — everything the document says except when. */
    public boolean sameCopiesAs(ShardInventory other) {
        return other != null && ephemeralId.equals(other.ephemeralId) && copies.equals(other.copies);
    }

    private static List<HeldCopy> sorted(List<HeldCopy> copies) {
        List<HeldCopy> ordered = new ArrayList<>(copies);
        ordered.sort(HeldCopy.ORDER);
        for (int i = 1; i < ordered.size(); i++) {
            HeldCopy previous = ordered.get(i - 1);
            HeldCopy current = ordered.get(i);
            if (previous.isSameShardAs(current)) {
                throw new IllegalArgumentException(
                    "two copies of the same shard on one node: " + previous + " and " + current
                );
            }
            if (previous.tenant().equals(current.tenant())
                && previous.indexUuid().equals(current.indexUuid())
                && (previous.indexName().equals(current.indexName()) == false
                    || previous.configRevision() != current.configRevision())) {
                // The wire form carries name and config_revision once per index, so two copies of one
                // index that disagree cannot both be written. It should be impossible — both come from
                // the same index document — and if it happens, saying so beats silently picking one.
                throw new IllegalArgumentException(
                    "copies of index [" + current.indexUuid() + "] disagree about name or config revision: "
                        + previous + " and " + current
                );
            }
        }
        return ordered;
    }

    /**
     * One shard copy on this node.
     *
     * @param indexName the index name, for humans. The uuid is the identity: names are reused, and a
     *     reconciler keyed on the name will eventually confuse an index with its deleted predecessor.
     * @param configRevision the store revision of the {@code tenant/<t>/indices/<name>} document this copy
     *     was built from. Revisions are <b>opaque and comparable only for equality</b> — never ordered,
     *     never arithmetic.
     * @param sizeBytes the balancer's only size signal, and <b>omitted rather than zero</b> while a copy
     *     is initializing: zero is a real size and would tell a disk-aware decider the copy costs nothing.
     *     The supervisor fills the gap from the largest copy of the same shard elsewhere in the pool, so a
     *     node need not invent a figure it does not have.
     * @param reason why a {@code FAILED} copy failed, truncated because an unbounded per-copy field is how
     *     a size budget gets exceeded during exactly the incident that produced the failures (D7.2)
     * @param allocationId the id of this copy's routing entry, published only for a document-replication copy: peer
     *     recovery and the primary's tracker are keyed on it, and only this node knows it (finding 12)
     * @param inSync on a document-replication primary, the copies its own tracker holds in sync, itself included,
     *     sorted so the bytes do not change while the set does not. Null where it is not known.
     */
    public record HeldCopy(
        TenantId tenant,
        String indexUuid,
        String indexName,
        long configRevision,
        int shard,
        ShardRole role,
        ShardState state,
        Long sizeBytes,
        String reason,
        String allocationId,
        List<String> inSync
    ) {

        public static final int MAX_REASON_LENGTH = 512;

        static final Comparator<HeldCopy> ORDER = Comparator.comparing(HeldCopy::tenant)
            .thenComparing(HeldCopy::indexUuid)
            .thenComparingInt(HeldCopy::shard);

        public HeldCopy {
            Objects.requireNonNull(tenant, "tenant");
            Objects.requireNonNull(indexUuid, "indexUuid");
            Objects.requireNonNull(indexName, "indexName");
            Objects.requireNonNull(role, "role");
            Objects.requireNonNull(state, "state");
            if (shard < 0) {
                throw new IllegalArgumentException("shard number must not be negative, was " + shard);
            }
            if (sizeBytes != null && sizeBytes < 0) {
                throw new IllegalArgumentException("size must not be negative, was " + sizeBytes);
            }
            if (state == ShardState.INITIALIZING && sizeBytes != null) {
                throw new IllegalArgumentException(
                    "an initializing copy has no size to report; omit it rather than sending a figure a "
                        + "disk-aware decider would believe"
                );
            }
            if (reason != null && reason.length() > MAX_REASON_LENGTH) {
                reason = reason.substring(0, MAX_REASON_LENGTH);
            }
            if (inSync != null) {
                if (role != ShardRole.PRIMARY) {
                    throw new IllegalArgumentException("only a primary has an in-sync set, and this is a " + role);
                }
                if (inSync.isEmpty()) {
                    throw new IllegalArgumentException("a primary is always in sync with itself, so the set is never empty");
                }
                inSync = inSync.stream().sorted().toList();
            }
        }

        /** A copy that publishes neither replication field, which is every copy but a document-replication one. */
        public HeldCopy(
            TenantId tenant,
            String indexUuid,
            String indexName,
            long configRevision,
            int shard,
            ShardRole role,
            ShardState state,
            Long sizeBytes,
            String reason
        ) {
            this(tenant, indexUuid, indexName, configRevision, shard, role, state, sizeBytes, reason, null, null);
        }

        public static HeldCopy initializing(TenantId tenant, String indexUuid, String indexName, long configRevision, int shard,
            ShardRole role) {
            return new HeldCopy(tenant, indexUuid, indexName, configRevision, shard, role, ShardState.INITIALIZING, null, null);
        }

        public static HeldCopy started(TenantId tenant, String indexUuid, String indexName, long configRevision, int shard,
            ShardRole role, Long sizeBytes) {
            return new HeldCopy(tenant, indexUuid, indexName, configRevision, shard, role, ShardState.STARTED, sizeBytes, null);
        }

        public boolean isSameShardAs(HeldCopy other) {
            return tenant.equals(other.tenant) && indexUuid.equals(other.indexUuid) && shard == other.shard;
        }

        @Override
        public String toString() {
            return tenant + "/" + indexName + "[" + indexUuid + "][" + shard + "] " + role + " " + state;
        }
    }
}
