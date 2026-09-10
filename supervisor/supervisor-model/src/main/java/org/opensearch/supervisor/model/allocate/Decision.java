package org.opensearch.supervisor.model.allocate;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * One decider's answer about one candidate node, and the reason for it.
 *
 * <p>The reason is not decoration. Every other decision in this service carries a {@code because} — R7.4 — and
 * placement is where an unexplained answer costs the most: "shard 3 is unassigned" with no further detail is
 * the single most common unanswerable question an operator has about a cluster. A rejected candidate should be
 * able to say which rule rejected it and what the numbers were.
 */
public record Decision(Type type, String decider, String because) {

    /** Ordered by severity, so combining a set of decisions is a maximum. */
    public enum Type {
        /** Allowed. */
        YES,
        /**
         * Allowed, but not now.
         *
         * <p>Distinct from {@code NO} because it means "try again shortly" rather than "this will never work",
         * and an operator reading the diff needs to tell a rule that is pacing the work from one that is
         * blocking it. A throttled placement is not a finding to act on; a rejected one is.
         */
        THROTTLE,
        /** Not allowed. */
        NO
    }

    public Decision {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(decider, "decider");
        if (because == null || because.isBlank()) {
            throw new IllegalArgumentException(decider + " returned " + type + " with no reason");
        }
    }

    public static Decision yes(String decider, String because) {
        return new Decision(Type.YES, decider, because);
    }

    public static Decision no(String decider, String because) {
        return new Decision(Type.NO, decider, because);
    }

    public static Decision throttle(String decider, String because) {
        return new Decision(Type.THROTTLE, decider, because);
    }

    public boolean isAllowed() {
        return type == Type.YES;
    }

    /** Every decider's answer for one candidate, so a rejection can name all of its causes rather than one. */
    public record Combined(Type type, List<Decision> decisions) {

        public Combined {
            decisions = List.copyOf(decisions);
        }

        public boolean isAllowed() {
            return type == Type.YES;
        }

        /** Only the decisions that explain the outcome — the blockers, or everything if it was allowed. */
        public List<Decision> deciding() {
            if (type == Type.YES) {
                return decisions;
            }
            List<Decision> deciding = new ArrayList<>();
            for (Decision decision : decisions) {
                if (decision.type() == type) {
                    deciding.add(decision);
                }
            }
            return deciding;
        }

        /** A single sentence naming what stopped this candidate, for a finding or an instruction. */
        public String summary() {
            List<Decision> deciding = deciding();
            if (deciding.isEmpty()) {
                return "no decider had anything to say";
            }
            StringBuilder summary = new StringBuilder();
            for (int i = 0; i < deciding.size(); i++) {
                if (i > 0) {
                    summary.append("; ");
                }
                summary.append('[').append(deciding.get(i).decider()).append("] ")
                    .append(deciding.get(i).because());
            }
            return summary.toString();
        }
    }
}
