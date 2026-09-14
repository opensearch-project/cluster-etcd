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

    public DesiredStateResolver(IndexConfigSource indexConfigs) {
        this.indexConfigs = Objects.requireNonNull(indexConfigs, "indexConfigs");
    }

    public DesiredState resolve(Assignment assignment) {
        Map<DesiredState.IndexKey, DesiredState.DesiredIndex> indices = new TreeMap<>();
        List<String> deferred = new ArrayList<>();

        assignment.tenants().forEach((tenant, tenantAssignment) -> {
            switch (tenantAssignment.role()) {
                case DATA -> tenantAssignment.indices()
                    .forEach((uuid, index) -> resolveIndex(tenant, uuid, index, indices, deferred));
                case COORDINATOR -> {
                    // Holds no data, so there is nothing to resolve. Where it gets the routing table it needs
                    // is tenant/<t>/status/indices/<uuid>, which is milestone 8.
                    if (tenantAssignment.indices().isEmpty() == false) {
                        defer(
                            deferred,
                            tenant + ": assigned the coordinator role and " + tenantAssignment.indices().size()
                                + " indices; a coordinator holds no copies, so the indices were ignored"
                        );
                    }
                }
                case UNKNOWN -> defer(
                    deferred,
                    tenant + ": role is not one this node understands, so nothing was done for this tenant"
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
                case PRIMARY, SEARCH_REPLICA -> buildable.put(shard, role);
                case REPLICA -> defer(
                    deferred,
                    key + "/" + shard + ": a document-replication replica needs peer resolution, which this "
                        + "node does not implement yet"
                );
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

    private static void defer(List<String> deferred, String reason) {
        if (deferred.size() < MAX_REASONS) {
            deferred.add(reason);
        } else if (deferred.size() == MAX_REASONS) {
            deferred.add("... and more; see the node's log");
        }
    }
}
