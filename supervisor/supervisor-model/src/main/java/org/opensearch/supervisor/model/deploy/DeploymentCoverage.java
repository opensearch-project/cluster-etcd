package org.opensearch.supervisor.model.deploy;

import org.opensearch.supervisor.model.Keys;
import org.opensearch.supervisor.model.NodeId;
import org.opensearch.supervisor.model.observe.Observation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Which deployment covers each node of the pool, and what cannot be acted on ({@code DESIGN.md} §29.2). A pure
 * function of an observation, like the differ, so that the decider, the planner and the status that will read it all
 * agree.
 *
 * <p><b>Whatever cannot be acted on as written freezes, and never releases.</b> A deployment that holds a node is
 * protecting something: a drain's nodes take no new primaries and are not reaped. Reading a document that cannot be
 * understood as "no deployment" would release exactly that, the mistake an unreadable cluster settings document taught
 * (§29.2). So:
 *
 * <ul>
 *   <li>a document that <b>cannot be parsed</b> freezes the whole pool, since which nodes it selects is unknown;
 *   <li>one that parses and is <b>invalid</b>, including an unrecognised state, freezes the nodes it selects;
 *   <li>a node selected by <b>two deployments</b> is frozen, rather than given to either. Upstream refuses the overlap
 *       when the second is written; with no single path to refuse it on, it is caught here.
 * </ul>
 *
 * <p>A frozen node takes no new primaries and is not reaped, and is otherwise left as it is. An <b>expired</b> drain is
 * not frozen: it reads as absent, so its nodes return to the ordinary liveness ladder, and is reported.
 *
 * @param byNode the deployment covering each covered node of the pool, valid or not
 * @param poolFrozen whether some deployment could not be parsed, which freezes every node of the pool
 * @param unreadable the ids of deployments that could not be parsed
 * @param invalid each invalid deployment's problems
 * @param conflicts each node selected by more than one deployment, with their ids
 * @param expired the ids of returning drains past their {@code until}
 */
public record DeploymentCoverage(
    Map<NodeId, Covered> byNode,
    boolean poolFrozen,
    Set<String> unreadable,
    Map<String, List<String>> invalid,
    Map<NodeId, Set<String>> conflicts,
    Set<String> expired
) {

    /**
     * One node's deployment.
     *
     * @param frozen covered by an invalid deployment, by two, or in a pool with an unreadable one
     * @param expired a returning drain past its {@code until}, which reads as absent
     */
    public record Covered(String id, Deployment deployment, boolean frozen, boolean expired) {

        /** Whether the deployment is in force for this node: neither frozen nor expired. */
        public boolean active() {
            return frozen == false && expired == false;
        }
    }

    public static final DeploymentCoverage NONE =
        new DeploymentCoverage(Map.of(), false, Set.of(), Map.of(), Map.of(), Set.of());

    public DeploymentCoverage {
        byNode = Map.copyOf(byNode);
        unreadable = Set.copyOf(unreadable);
        invalid = Map.copyOf(invalid);
        conflicts = Map.copyOf(conflicts);
        expired = Set.copyOf(expired);
    }

    /** The deployment covering this node, if any. */
    public java.util.Optional<Covered> of(NodeId node) {
        return java.util.Optional.ofNullable(byNode.get(node));
    }

    /** Whether this node is frozen: no new primaries placed on it, and not reaped. */
    public boolean frozen(NodeId node) {
        if (poolFrozen) {
            return true;
        }
        Covered covered = byNode.get(node);
        return covered != null && covered.frozen();
    }

    public static DeploymentCoverage of(Observation observation) {
        Set<String> unreadable = new TreeSet<>();
        for (String key : observation.unreadable()) {
            if (key.startsWith(Keys.DEPLOYMENTS_PREFIX)) {
                unreadable.add(Keys.idAfter(Keys.DEPLOYMENTS_PREFIX, key));
            }
        }
        if (observation.deployments().isEmpty() && unreadable.isEmpty()) {
            return NONE;
        }

        long now = observation.observedAt().toEpochMilli();
        Map<String, List<String>> invalid = new TreeMap<>();
        Set<String> expired = new TreeSet<>();
        observation.deployments().forEach((id, deployment) -> {
            List<String> problems = deployment.problems();
            if (problems.isEmpty() == false) {
                invalid.put(id, List.copyOf(problems));
            } else if (deployment.expiredAt(now)) {
                expired.add(id);
            }
        });

        Map<NodeId, Covered> byNode = new LinkedHashMap<>();
        Map<NodeId, Set<String>> conflicts = new TreeMap<>(java.util.Comparator.comparing(NodeId::value));
        observation.nodes().forEach((nodeId, node) -> {
            if (node.registration() == null) {
                return;
            }
            List<String> selecting = new ArrayList<>();
            observation.deployments().forEach((id, deployment) -> {
                if (deployment.selects(node.registration().attributes())) {
                    selecting.add(id);
                }
            });
            selecting.sort(String::compareTo);
            if (selecting.isEmpty()) {
                return;
            }
            if (selecting.size() > 1) {
                conflicts.put(nodeId, new TreeSet<>(selecting));
            }
            String id = selecting.get(0);
            boolean frozen = unreadable.isEmpty() == false || selecting.size() > 1 || invalid.containsKey(id);
            byNode.put(nodeId, new Covered(id, observation.deployments().get(id), frozen,
                frozen == false && expired.contains(id)));
        });

        return new DeploymentCoverage(byNode, unreadable.isEmpty() == false, unreadable, invalid, conflicts, expired);
    }
}
