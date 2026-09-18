package org.opensearch.supervisor.model.diff;

import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.allocate.RemoteStoreDecider;
import org.opensearch.supervisor.model.TenantId;
import org.opensearch.supervisor.model.index.IndexConfig;
import org.opensearch.supervisor.model.node.ShardInventory;
import org.opensearch.supervisor.model.node.ShardRole;
import org.opensearch.supervisor.model.node.ShardState;
import org.opensearch.supervisor.model.observe.Observation;
import org.opensearch.supervisor.model.status.Liveness;
import org.opensearch.supervisor.model.task.TaskRecord;
import org.opensearch.supervisor.model.task.TaskState;
import org.opensearch.supervisor.model.task.TaskType;

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
        poolFindings(observation, findings);
        shardFindings(observation, findings);
        taskFindings(observation, findings);
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

        // Copies with no desired index behind them, from both places a copy can be recorded.
        //
        // Held and merely assigned are one finding kind on purpose. The data plane defers an assignment entry
        // naming an index it cannot resolve and -- per the contract -- reports anything deferred as a failed
        // apply, so an entry left behind by a deleted index is not a harmless leftover: it stops that node ever
        // reporting an assignment as applied again, which is the acknowledgement a consumer-pointer reset is
        // completed on. An entry is also the only thing a removal can actually withdraw. Raising it as its own
        // kind would mean two kinds, two ages and two rules for one question.
        Map<Subject, String> orphans = new LinkedHashMap<>();

        actual.forEach((key, copies) -> {
            if (allIndices.containsKey(key.indexUuid())) {
                return;
            }
            for (Copy copy : copies) {
                orphans.put(
                    Subject.ofShardCopy(key.tenant(), key.indexUuid(), key.shard(), copy.node()),
                    "node holds a copy of index uuid " + key.indexUuid() + " which is absent from desired"
                        + " state; this is a deleted index or an index document that has not landed yet, and"
                        + " the two are indistinguishable, so it is left to age before anything is deleted"
                );
            }
        });

        observation.nodes().forEach((nodeId, node) -> {
            if (node.assignment() == null) {
                return;
            }
            node.assignment().tenants().forEach((tenant, assigned) ->
                assigned.indices().forEach((uuid, index) -> {
                    if (allIndices.containsKey(uuid)) {
                        return;
                    }
                    index.shards().keySet().forEach(shard ->
                        // Held wins where a node both holds and is assigned the copy: it is the stronger
                        // statement, and the age must not depend on which of the two the differ saw first.
                        orphans.putIfAbsent(
                            Subject.ofShardCopy(tenant, uuid, shard, nodeId),
                            "node is assigned a copy of index uuid " + uuid + " which is absent from desired"
                                + " state; this is a deleted index or an index document that has not landed"
                                + " yet, and while the entry stands the node cannot resolve its assignment"
                        )
                    );
                })
            );
        });

        orphans.forEach((subject, because) ->
            findings.add(Finding.of(FindingKind.ORPHANED_COPY, subject, observation.pass(), because)));
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

        // Per role, not in total. Two counts that happen to sum correctly can still be individually wrong -- a
        // shard with two search replicas and no ordinary replica satisfies a total of two and is not what was
        // asked for -- and the planner needs to know which role a missing copy wants in any case.
        for (ShardRole role : List.of(ShardRole.REPLICA, ShardRole.SEARCH_REPLICA)) {
            long held = copies.stream().filter(copy -> copy.role() == role).count();
            int wanted = index.desiredCopiesOf(role);
            String plural = role == ShardRole.SEARCH_REPLICA ? "search replicas" : "replicas";

            if (held < wanted) {
                findings.add(Finding.of(
                    role == ShardRole.SEARCH_REPLICA
                        ? FindingKind.UNASSIGNED_SEARCH_REPLICA
                        : FindingKind.UNASSIGNED_REPLICA,
                    subject, observation.pass(),
                    "index [" + index.name() + "] shard " + key.shard() + " has " + held + " of " + wanted + " "
                        + plural + placementHint(observation)
                ));
            } else if (held > wanted) {
                findings.add(Finding.of(
                    FindingKind.SURPLUS_COPY, subject, observation.pass(),
                    "index [" + index.name() + "] shard " + key.shard() + " has " + held + " " + plural
                        + " where " + wanted + " are wanted; likely a move that has added before removing,"
                        + " which is the intended order"
                ));
            }
        }

        if (index.asksForImpossibleSearchReplicas()) {
            findings.add(Finding.of(
                FindingKind.DESIRED_COPY_IGNORED, subject, observation.pass(),
                "index [" + index.name() + "] asks for " + index.numberOfSearchReplicas() + " search replica(s),"
                    + " which are not planned because the index uses document replication: a search-only copy"
                    + " recovers from a remote store, and document replication does not imply one. Either set"
                    + " index.replication.type to SEGMENT or use index.number_of_replicas instead"
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

    // ---------------------------------------------------------------- tasks

    /**
     * Tasks worth telling an operator about, decided from the records themselves.
     *
     * <p>Computed here rather than fed back from the queue on purpose. The queue runs <em>after</em> the diff
     * — a task is a hint, so draining it must not decide what gets reconciled — so anything it reported would
     * be a pass out of date. Both conditions below are plain facts about a record, needing no policy and no
     * memory: it has been given up on, or its holder has stopped renewing.
     */
    private static void taskFindings(Observation observation, List<Finding> findings) {
        long now = observation.observedAt().toEpochMilli();

        observation.tenants().forEach((tenant, desired) -> {
            for (TaskRecord task : desired.tasks()) {
                if (task.state() == TaskState.FAILED) {
                    findings.add(Finding.of(
                        FindingKind.TASK_STUCK, Subject.ofTask(tenant, task.taskId()), observation.pass(),
                        "task [" + task.type() + "] for index [" + task.index() + "] was given up on after "
                            + task.attempts() + " attempts: " + task.lastError()
                            + "; it is kept rather than reaped so the failure stays visible"
                    ));
                } else if (task.state() == TaskState.CLAIMED && task.isClaimExpired(now)) {
                    findings.add(Finding.of(
                        FindingKind.TASK_STUCK, Subject.ofTask(tenant, task.taskId()), observation.pass(),
                        "task [" + task.type() + "] for index [" + task.index() + "] is claimed by ["
                            + task.holder() + "] whose lease has expired; any supervisor may now take it up,"
                            + " and this will be its attempt " + (task.attempts() + 1)
                    ));
                } else if (task.type() == TaskType.UNKNOWN) {
                    findings.add(Finding.of(
                        FindingKind.TASK_STUCK, Subject.ofTask(tenant, task.taskId()), observation.pass(),
                        "task type is not recognised by this build, so it can be neither completed nor failed"
                            + " here; a newer supervisor is needed to drain it"
                    ));
                }
            }
        });
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
    /**
     * Findings about the pool itself rather than about any node or shard in it.
     *
     * <p>Only one so far, and it is here because <b>only the supervisor can see it</b>. Every other party in the
     * system sees one node's configuration; this sees all of them at once.
     */
    private static void poolFindings(Observation observation, List<Finding> findings) {
        // Only nodes that could hold a copy, and only those declaring a repository. A node with none is
        // RemoteStoreDecider's business, not a disagreement.
        java.util.Map<String, java.util.List<NodeId>> byRepository = new java.util.TreeMap<>();
        observation.placeableNodes().forEach((nodeId, node) -> {
            if (node.registration() == null) {
                return;
            }
            String repository = node.registration().attributes()
                .get(RemoteStoreDecider.SEGMENT_REPOSITORY_ATTRIBUTE);
            if (repository != null && repository.isBlank() == false) {
                byRepository.computeIfAbsent(repository, key -> new ArrayList<>()).add(nodeId);
            }
        });

        if (byRepository.size() > 1) {
            StringBuilder detail = new StringBuilder();
            byRepository.forEach((repository, nodes) -> {
                if (detail.length() > 0) {
                    detail.append("; ");
                }
                // Bounded: one example node per repository rather than every node, for the same reason the
                // allocator groups its refusals -- a large pool must not produce a large document.
                detail.append('[').append(repository).append("] on ").append(nodes.size())
                    .append(nodes.size() == 1 ? " node, e.g. [" : " nodes, e.g. [")
                    .append(nodes.get(0)).append(']');
            });
            findings.add(Finding.of(
                FindingKind.POOL_REPOSITORY_MISMATCH, Subject.ofPool(observation.pool()), observation.pass(),
                "nodes in this pool write segments to " + byRepository.size() + " different remote repositories: "
                    + detail + ". Copies of one shard share a remote path, so a search-only copy placed on the"
                    + " wrong node reads an empty store and reports itself healthy. No node can detect this --"
                    + " each sees only its own attributes -- so nothing else will report it"
            ));
        }
    }

    public static Diff diff(Observation observation) {
        return diff(observation, new FindingAges());
    }
}
