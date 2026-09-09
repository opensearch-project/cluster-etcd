package org.opensearch.supervisor.model.diff;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Liveness;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Turns an observation into the list of things that are wrong. A pure function, and the piece of this
 * service that carries the most risk per line.
 *
 * <p>One method per concern, concatenated. Each is independently testable and independently disableable,
 * which matters when one of them turns out to oscillate in production.
 *
 * <p><b>This function never decides anything is gone.</b> It reports what it sees; whether a finding is old
 * enough to act on is {@link FindingAges}' job, and acting at all is the planner's. That separation is what
 * lets the whole thing be a pure function of one observation despite the destructive decisions downstream
 * needing history.
 */
public final class Differ {

    private Differ() {}

    public static Diff diff(Observation observation, FindingAges ages) {
        List<Finding> findings = new ArrayList<>();

        nodeFindings(observation, findings);
        shardFindings(observation, findings);
        unreadableFindings(observation, findings);

        return new Diff(observation.pass(), observation.pool(), ages.age(findings, observation.pass()));
    }

    // ---------------------------------------------------------------- nodes

    private static void nodeFindings(Observation observation, List<Finding> findings) {
        observation.nodes().forEach((nodeId, node) -> {
            if (node.registration() == null && node.heartbeat() != null) {
                findings.add(Finding.of(
                    FindingKind.NODE_UNREGISTERED, Subject.ofNode(nodeId), observation.pass(),
                    "heartbeat present with no registration document; keeping its copies but placing nothing"
                        + " new, since this is more often a torn read than a broken node"
                ));
            }
            if (node.pool() == null && node.registration() != null) {
                findings.add(Finding.of(
                    FindingKind.NODE_UNPOOLED, Subject.ofNode(nodeId), observation.pass(),
                    "node declares no pool and has no pools/nodes/ override, so no supervisor owns it"
                ));
            }
            if (node.liveness() == Liveness.STALE) {
                findings.add(Finding.of(
                    FindingKind.NODE_STALE, Subject.ofNode(nodeId), observation.pass(),
                    "heartbeat key has not changed revision within this node's staleness threshold; its"
                        + " copies still count until the second threshold"
                ));
            } else if (node.liveness() == Liveness.LOST) {
                findings.add(Finding.of(
                    FindingKind.NODE_LOST, Subject.ofNode(nodeId), observation.pass(),
                    "heartbeat key silent past the lost threshold; treating its " + copyCount(node) + " copies"
                        + " as gone"
                ));
            }
        });
    }

    private static int copyCount(Observation.NodeObservation node) {
        return node.inventory() == null ? 0 : node.inventory().copyCount();
    }

    // ---------------------------------------------------------------- shards

    /** One shard of one index: what is desired, and what is actually there. */
    private record ShardKey(TenantId tenant, String indexUuid, int shard) implements Comparable<ShardKey> {
        @Override
        public int compareTo(ShardKey other) {
            int byTenant = tenant.compareTo(other.tenant);
            if (byTenant != 0) {
                return byTenant;
            }
            int byIndex = indexUuid.compareTo(other.indexUuid);
            return byIndex != 0 ? byIndex : Integer.compare(shard, other.shard);
        }
    }

    private record Copy(NodeId node, ShardRole role, ShardState state) {}

    private static void shardFindings(Observation observation, List<Finding> findings) {
        Map<ShardKey, List<Copy>> actual = actualCopies(observation);
        Map<String, IndexConfig> allIndices = new LinkedHashMap<>();
        observation.tenants().values().forEach(desired -> allIndices.putAll(desired.indicesByUuid()));

        // Desired, shard by shard.
        observation.tenants().forEach((tenant, desired) -> desired.indicesByUuid().forEach((uuid, index) -> {
            if (index.open() == false) {
                // A closed index still exists and still has settings; it simply should not be serving, and
                // OpenSearch reports it green with no shards. Nothing about it is a finding.
                return;
            }
            for (int shard = 0; shard < index.numberOfShards(); shard++) {
                ShardKey key = new ShardKey(tenant, uuid, shard);
                List<Copy> copies = actual.getOrDefault(key, List.of());
                shardFinding(observation, index, key, copies, findings);
            }
        }));

        // Actual copies with no desired index behind them.
        actual.forEach((key, copies) -> {
            if (allIndices.containsKey(key.indexUuid())) {
                return;
            }
            for (Copy copy : copies) {
                findings.add(Finding.of(
                    FindingKind.ORPHANED_COPY,
                    Subject.ofShardCopy(key.tenant(), key.indexUuid(), key.shard(), copy.node()),
                    observation.pass(),
                    "node holds a copy of index uuid " + key.indexUuid() + " which is absent from desired"
                        + " state; this is a deleted index or an index document that has not landed yet, and"
                        + " the two are indistinguishable, so nothing acts on it until it has held"
                ));
            }
        });
    }

    private static void shardFinding(
        Observation observation,
        IndexConfig index,
        ShardKey key,
        List<Copy> copies,
        List<Finding> findings
    ) {
        Subject subject = Subject.ofShard(key.tenant(), key.indexUuid(), key.shard());
        long primaries = copies.stream().filter(copy -> copy.role() == ShardRole.PRIMARY).count();
        long servingPrimaries = copies.stream()
            .filter(copy -> copy.role() == ShardRole.PRIMARY && copy.state() == ShardState.STARTED)
            .count();
        long nonPrimaries = copies.size() - primaries;

        if (primaries == 0) {
            findings.add(Finding.of(
                FindingKind.UNASSIGNED_PRIMARY, subject, observation.pass(),
                "index [" + index.name() + "] shard " + key.shard() + " has no primary on any node in this"
                    + " pool" + placementHint(observation)
            ));
        }

        if (nonPrimaries < index.numberOfReplicas()) {
            findings.add(Finding.of(
                FindingKind.UNASSIGNED_REPLICA, subject, observation.pass(),
                "index [" + index.name() + "] shard " + key.shard() + " has " + nonPrimaries + " of "
                    + index.numberOfReplicas() + " replicas" + placementHint(observation)
            ));
        } else if (nonPrimaries > index.numberOfReplicas()) {
            findings.add(Finding.of(
                FindingKind.SURPLUS_COPY, subject, observation.pass(),
                "index [" + index.name() + "] shard " + key.shard() + " has " + nonPrimaries + " replicas"
                    + " where " + index.numberOfReplicas() + " are wanted; likely a move that has added"
                    + " before removing, which is the intended order"
            ));
        }

        // A docrep replica cannot converge until a primary is serving, so a replica placed first is a
        // planning mistake rather than a transient -- but it is only reportable, never actionable here.
        if (index.replication() == org.opensearch.supervisor.model.status.Replication.DOCUMENT
            && servingPrimaries == 0
            && nonPrimaries > 0) {
            findings.add(Finding.of(
                FindingKind.PRIMARY_BEFORE_REPLICA, subject, observation.pass(),
                "index [" + index.name() + "] shard " + key.shard() + " has " + nonPrimaries + " replicas"
                    + " and no serving primary; under document replication they cannot recover until one"
                    + " exists, so the plan placed them in the wrong order"
            ));
        }

        for (Copy copy : copies) {
            if (copy.role() == ShardRole.UNKNOWN || copy.state() == ShardState.UNKNOWN) {
                findings.add(Finding.of(
                    FindingKind.UNRECOGNISED_STATE,
                    Subject.ofShardCopy(key.tenant(), key.indexUuid(), key.shard(), copy.node()),
                    observation.pass(),
                    "node reports role " + copy.role() + " state " + copy.state() + ", which this build does"
                        + " not recognise; counted as present but never as serving"
                ));
            }
        }
    }

    private static String placementHint(Observation observation) {
        int placeable = observation.placeableNodes().size();
        if (placeable == 0) {
            return "; no node in this pool is currently placeable, so nothing can be done about it";
        }
        return "; " + placeable + " node(s) in this pool could take it";
    }

    private static Map<ShardKey, List<Copy>> actualCopies(Observation observation) {
        Map<ShardKey, List<Copy>> actual = new TreeMap<>();
        observation.nodes().forEach((nodeId, node) -> {
            if (node.copiesCount() == false || node.inventory() == null) {
                return;
            }
            node.inventory().tenants().forEach((tenant, tenantShards) ->
                tenantShards.indices().forEach((uuid, indexShards) ->
                    indexShards.shards().forEach((shard, copy) ->
                        actual.computeIfAbsent(new ShardKey(tenant, uuid, shard), key -> new ArrayList<>())
                            .add(new Copy(nodeId, copy.role(), copy.state())))));
        });
        return actual;
    }

    // ---------------------------------------------------------------- unreadable documents

    private static void unreadableFindings(Observation observation, List<Finding> findings) {
        for (String key : observation.unreadable()) {
            findings.add(Finding.of(
                FindingKind.UNREADABLE_DOCUMENT,
                Subject.ofPool(observation.pool()),
                observation.pass(),
                "could not parse [" + key + "]; skipped rather than failing the pass, because one bad key"
                    + " must not stop every other tenant from being reconciled"
            ));
        }
    }

    /** Convenience for a differ with no history, which is what most tests want. */
    public static Diff diff(Observation observation) {
        return diff(observation, new FindingAges());
    }
}
