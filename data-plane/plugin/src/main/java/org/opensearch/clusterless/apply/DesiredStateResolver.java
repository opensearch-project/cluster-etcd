/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.apply;

import org.opensearch.clusterless.contract.Assignment;
import org.opensearch.clusterless.contract.IndexConfig;
import org.opensearch.clusterless.contract.MalformedDocumentException;
import org.opensearch.clusterless.contract.ShardRole;
import org.opensearch.clusterless.contract.TenantId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Turns an assignment into a {@link DesiredState}, reading the index documents it names.
 *
 * <p>Everything that can decline to build a copy happens here, so that the fabricator downstream has nothing
 * to decide and nothing to read. Each reason below is a deliberate answer to a case with a plausible wrong one,
 * and the wrong answers are all the same shape: build the shard anyway, from something that is not quite what
 * was asked for.
 */
public final class DesiredStateResolver {

    /**
     * How many declined copies to describe before summarising.
     *
     * <p>The reasons go onto the heartbeat's failure detail, which the contract caps at 512 characters, and
     * onto a log line. A node whose index documents have all gone missing would otherwise produce a reason per
     * shard — thousands of them, during exactly the incident when the log needs to stay readable.
     */
    static final int MAX_REASONS = 20;

    private final IndexConfigSource indexConfigs;
    private final RemoteStore remoteStore;

    public DesiredStateResolver(IndexConfigSource indexConfigs) {
        this(indexConfigs, RemoteStore.NONE);
    }

    /**
     * @param remoteStore this node's remote store, which decides whether a search replica can work here at all. A
     *     node with none declines every search replica, which is the honest answer rather than a limitation: see
     *     {@link RemoteStore} for why a fabricated cluster state leaves peer-based segment replication no peer to
     *     find.
     */
    public DesiredStateResolver(IndexConfigSource indexConfigs, RemoteStore remoteStore) {
        this.indexConfigs = Objects.requireNonNull(indexConfigs, "indexConfigs");
        this.remoteStore = Objects.requireNonNull(remoteStore, "remoteStore");
    }

    public DesiredState resolve(Assignment assignment) {
        Map<DesiredState.IndexKey, DesiredState.DesiredIndex> indices = new TreeMap<>();
        List<String> deferred = new ArrayList<>();

        assignment.tenants().forEach((tenant, tenantAssignment) -> {
            // Not a switch any more, because the roles are a set and both at once is the interesting case: a node
            // that holds copies of a tenant's indices and serves queries for it does both, from one routing table.
            // Where the coordinating half gets that table is tenant/<t>/status/indices/<uuid>.
            if (tenantAssignment.holdsData()) {
                tenantAssignment.indices().forEach((uuid, index) -> resolveIndex(tenant, uuid, index, indices, deferred));
            } else if (tenantAssignment.coordinates() && tenantAssignment.indices().isEmpty() == false) {
                // Copies named for a tenant this node was not told to hold data for. Declined rather than built,
                // because the two halves of the entry contradict each other and only one of them can be obeyed.
                defer(
                    deferred,
                    tenant + ": " + tenantAssignment.indices().size() + " index(es) named without the data role, so"
                        + " the copies were ignored"
                );
            }
            if (tenantAssignment.understood() == false) {
                defer(
                    deferred,
                    tenant + ": roles " + tenantAssignment.roles() + " are not ones this node understands, so nothing"
                        + " was done for this tenant"
                );
            }
        });

        return new DesiredState(assignment.version(), indices, assignment.coordinatorTenants(), deferred);
    }

    private void resolveIndex(
        TenantId tenant,
        String uuid,
        Assignment.IndexAssignment assigned,
        Map<DesiredState.IndexKey, DesiredState.DesiredIndex> indices,
        List<String> deferred
    ) {
        DesiredState.IndexKey key = new DesiredState.IndexKey(tenant, uuid);

        Optional<IndexConfig> read;
        try {
            read = indexConfigs.read(tenant, assigned.name());
        } catch (MalformedDocumentException e) {
            // One index's document being unreadable costs that index a pass, and nothing else. The assignment
            // stands and every other index in it is still resolved.
            defer(deferred, key + " [" + assigned.name() + "]: " + e.getMessage());
            return;
        }

        if (read.isEmpty()) {
            // Ordinary and temporary: the supervisor may have assigned a shard before the control plane's
            // index document landed, and the two write to different keys with no transaction between them.
            defer(deferred, key + ": no index document at [" + assigned.name() + "] yet");
            return;
        }

        IndexConfig config = read.get();
        if (config.matches(uuid) == false) {
            // The torn read a later pass would not correct. An index deleted and recreated under the same name
            // gives a valid document describing a different index, and building against it would produce a
            // shard with the right uuid in its inventory and the wrong index's mappings inside it.
            defer(
                deferred,
                key + ": the document at [" + assigned.name() + "] is index [" + config.uuid()
                    + "], so the assigned index is not present yet"
            );
            return;
        }

        if (config.open() == false) {
            // The contract maps a closed index to "no copies at all" rather than to a state of its own, which
            // matches what the supervisor projects: a closed index is green with no shards.
            //
            // Declining every copy is right; what matters is what the fabricator does next. An index that
            // disappears from the cluster state's metadata is read by the server as *deleted*, and a deleted
            // index has its shard files removed -- so a node that simply dropped a closed index would destroy the
            // data on close and have nothing to reopen. DataNodeState keeps the metadata of an index it holds no
            // copy of for exactly this reason; see retainIndicesNoLongerWanted.
            defer(deferred, key + " [" + config.name() + "]: the index is closed, so it holds no copies");
            return;
        }

        int numberOfShards;
        try {
            numberOfShards = config.numberOfShards();
            config.numberOfReplicas();
        } catch (MalformedDocumentException e) {
            defer(deferred, key + ": " + e.getMessage());
            return;
        }

        Map<Integer, ShardRole> buildable = new LinkedHashMap<>();
        assigned.shards().forEach((shard, role) -> {
            if (shard >= numberOfShards) {
                // The assignment and the index document disagree about how many shards there are. Building it
                // would fail inside IndexMetadata; declining says which two documents to compare.
                defer(
                    deferred,
                    key + "/" + shard + ": the index document declares " + numberOfShards + " shards, so there is no shard " + shard
                );
                return;
            }
            switch (role) {
                case PRIMARY -> buildable.put(shard, role);
                case SEARCH_REPLICA -> {
                    if (remoteStore.backs(config)) {
                        buildable.put(shard, role);
                    } else if (remoteStore.configured() == false) {
                        // Declined rather than built, because a search replica built here would fail in a way that
                        // points nowhere near the cause. The server would hand it PrimaryShardReplicationSource,
                        // which resolves the primary through the local routing table -- and this node's fabricated
                        // state contains only its own copies, so there is no primary in it to find. The copy would
                        // start, report itself started, and never receive a segment.
                        defer(
                            deferred,
                            key + "/" + shard + ": a search replica reads its segments from a remote store, and this "
                                + "node has none configured (set the node.attr.remote_store.* attributes)"
                        );
                    } else {
                        defer(
                            deferred,
                            key + "/" + shard + ": a search replica needs the index to use segment replication, and ["
                                + config.name() + "] does not (index.replication.type)"
                        );
                    }
                }
                case REPLICA -> {
                    if ("DOCUMENT".equals(config.replication())) {
                        // Buildable as far as this step can tell. Whether its primary is actually reachable is
                        // decided later, once the peer view has been read -- see PeerRecovery, which declines it
                        // with a reason rather than building a routing table RoutingNodes will not accept.
                        buildable.put(shard, role);
                    } else {
                        // The mirror of the search-replica case above. The supervisor maps SEGMENT to
                        // SEARCH_REPLICA and DOCUMENT to REPLICA, so this is either a supervisor newer than this
                        // node or a mapping that has drifted; either way, building a peer-recovering copy of an
                        // index whose segments come from a repository would recover from a primary that is not
                        // publishing operations to it.
                        defer(
                            deferred,
                            key + "/" + shard + ": a document-replication replica needs the index to use document "
                                + "replication, and [" + config.name() + "] does not (index.replication.type)"
                        );
                    }
                }
                case UNKNOWN -> defer(
                    deferred,
                    key + "/" + shard + ": the role is not one this node understands, so the copy was not built"
                );
            }
        });

        if (buildable.isEmpty()) {
            // Every copy of this index was declined, so there is nothing to put in the cluster state. The
            // reasons are already recorded; adding the index with no shards would fabricate metadata for
            // something this node holds nothing of.
            return;
        }
        indices.put(key, new DesiredState.DesiredIndex(key, config, buildable));
    }

    /**
     * Drops search replicas of an index this node also holds a writeable copy of.
     *
     * <p>Not a scheduling preference — there is no state that describes the combination. The block that lets a
     * search-only copy exist without a primary in the routing table, {@code index.blocks.search_only}, is an
     * <em>index-level</em> setting, and so is {@code index.number_of_search_replicas}; but the assignment names a
     * role per shard. So an index whose shard 0 is a primary here and whose shard 1 is a search replica here has
     * two shards wanting opposite index settings, and both answers are wrong:
     *
     * <ul>
     *   <li>block off — {@code RoutingNodes} asserts {@code "Primary shard routing can't be null for
     *       non-search-only indices"} on shard 1, and the server runs with assertions enabled;
     *   <li>block on — shard 0's primary is on a node that has declared the whole index read-only, so writes to it
     *       fail.
     * </ul>
     *
     * <p>The search replicas are the ones dropped, because reads survive losing a copy and writes do not survive
     * losing their primary. Each declined copy is reported absent in the usual way, which is what lets the
     * supervisor place it on a node that is not also holding a primary of the same index. That is a constraint on
     * placement the contract does not currently state, and it has been raised with the supervisor rather than
     * quietly worked around here.
     */
    private static void declineSearchReplicasMixedWithOtherRoles(
        DesiredState.IndexKey key,
        Map<Integer, ShardRole> buildable,
        List<String> deferred
    ) {
        boolean holdsOther = buildable.values().stream().anyMatch(role -> role != ShardRole.SEARCH_REPLICA);
        if (holdsOther == false) {
            return;
        }
        buildable.entrySet().removeIf(entry -> {
            if (entry.getValue() != ShardRole.SEARCH_REPLICA) {
                return false;
            }
            defer(
                deferred,
                key + "/" + entry.getKey() + ": this node also holds a writeable copy of this index, and the "
                    + "search-only block that a search replica needs is index-level, so the two cannot share a node"
            );
            return true;
        });
    }

    private static void defer(List<String> deferred, String reason) {
        if (deferred.size() < MAX_REASONS) {
            deferred.add(reason);
        } else if (deferred.size() == MAX_REASONS) {
            deferred.add("... and more; see the node's log");
        }
    }
}
