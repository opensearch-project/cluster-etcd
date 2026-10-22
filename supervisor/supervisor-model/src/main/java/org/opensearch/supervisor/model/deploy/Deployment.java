package org.opensearch.supervisor.model.deploy;

import org.opensearch.supervisor.model.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@code deployments/<deployment-id>}: one deployment, as the deployment system that drives it last wrote it
 * ({@code DESIGN.md} §29.2). Operator-owned and cluster-wide. Every supervisor reads it each pass and applies it to the
 * nodes of the pools it owns; none writes it.
 *
 * <p>Desired state, never an event: the document says which state the deployment is in, and the supervisor re-derives
 * everything from that and from what it observes. It cannot see the transition that led here, so the transition table
 * is enforced by whatever writes the document, not by this reader.
 *
 * @param attributes the nodes under deployment, every one of which must match, as upstream selects them. Given when the
 *     deployment starts with {@code DRAIN}, and the same for every later state.
 * @param intent for a {@code DRAIN}: whether the nodes come back. Absent reads as {@code return}, the ordinary case.
 * @param until for a {@code DRAIN} that returns: epoch millis after which it reads as expired. Required, because a
 *     drain left behind by a deployment system that crashed must not hold its nodes for ever.
 * @param dial for {@code DIAL_UP}: the fraction of each shard's search replicas following the new writer, 0 to 1.
 * @param concurrentRecoveries for {@code ROLLBACK}: overrides the supervisor's default for how many copies a node may
 *     recover at once while the rollback rebuilds them. Absent means the default.
 */
public record Deployment(
    DeploymentState state,
    Map<String, String> attributes,
    DrainIntent intent,
    Long until,
    Double dial,
    Integer concurrentRecoveries
) {

    public Deployment {
        state = state == null ? DeploymentState.UNKNOWN : state;
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    /** The intent, reading absent as {@code return}. Meaningful for a {@code DRAIN} only. */
    public DrainIntent effectiveIntent() {
        return intent == null ? DrainIntent.RETURN : intent;
    }

    /**
     * Why this document cannot be acted on as written, or empty if it can. An invalid deployment acts on nothing and
     * releases nothing: its selected nodes are frozen until it is corrected ({@link DeploymentCoverage}).
     */
    public List<String> problems() {
        List<String> problems = new ArrayList<>();
        if (attributes.isEmpty()) {
            problems.add("it selects no nodes: attributes are required, and every one must match");
        }
        attributes.forEach((key, value) -> {
            if (value == null || value.isEmpty()) {
                problems.add("attribute [" + key + "] has no value");
            }
        });
        switch (state) {
            case UNKNOWN -> problems.add("its state is not one this build recognises");
            case FINISH -> problems.add("FINISH removes a deployment rather than being a state one is left in; delete the"
                + " key instead");
            case DRAIN -> {
                if (effectiveIntent() == DrainIntent.UNKNOWN) {
                    problems.add("its intent is not one this build recognises");
                }
                if (effectiveIntent() == DrainIntent.RETURN && until == null) {
                    problems.add("a drain whose nodes return needs an until, so that one left behind expires");
                }
            }
            case DIAL_UP -> {
                if (dial == null || dial.isNaN() || dial < 0 || dial > 1) {
                    problems.add("DIAL_UP needs a dial between 0 and 1, was " + dial);
                }
            }
            case ROLLBACK -> {
                if (concurrentRecoveries != null && concurrentRecoveries < 1) {
                    problems.add("concurrent_recoveries must allow at least one recovery, was " + concurrentRecoveries);
                }
            }
            case SHADOW -> {
            }
        }
        return problems;
    }

    /** Whether the selected nodes are this deployment's: every attribute it names is one the node has, with that value. */
    public boolean selects(Map<String, String> nodeAttributes) {
        if (attributes.isEmpty() || nodeAttributes == null) {
            return false;
        }
        for (Map.Entry<String, String> wanted : attributes.entrySet()) {
            if (Objects.equals(wanted.getValue(), nodeAttributes.get(wanted.getKey())) == false) {
                return false;
            }
        }
        return true;
    }

    /** Whether a returning drain's deadline has passed, at this instant. */
    public boolean expiredAt(long epochMillis) {
        return state == DeploymentState.DRAIN && effectiveIntent() == DrainIntent.RETURN && until != null
            && epochMillis >= until;
    }

    public byte[] toBytes() {
        return Json.write(this, "deployment");
    }

    public static Deployment from(byte[] bytes) {
        return Json.read(bytes, Deployment.class, "deployment");
    }
}
