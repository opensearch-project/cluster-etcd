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
public record Decision(Type type, String decider, String code, String because) {

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
        Objects.requireNonNull(code, "code");
        if (because == null || because.isBlank()) {
            throw new IllegalArgumentException(decider + " returned " + type + " with no reason");
        }
    }

    /**
     * A {@code YES} needs no code, because nothing ever groups approvals.
     *
     * <p>Kept as a two-argument form deliberately: a refusal has no such shortcut, so the compiler is what makes
     * sure every new rule that can block a placement says <em>which</em> rule it is.
     */
    public static Decision yes(String decider, String because) {
        return new Decision(Type.YES, decider, decider, because);
    }

    /**
     * @param code which rule inside this decider fired, as a short stable slug — {@code "shared_host"}, not the
     *     sentence. It exists so that refusals can be <b>grouped</b>: an operator wants one example of each
     *     distinct reason a placement was refused, and grouping on the prose cannot work, because most reasons
     *     name the node ("node [A] holds…") or a measurement ("91% full") and so are textually distinct for every
     *     candidate. Grouping on the decider name alone is too coarse the other way — the regression that
     *     prompted this had one decider giving two genuinely different answers, and showing one of them hid the
     *     one that mattered. So the decider says what varies and what does not, because only it knows.
     */
    public static Decision no(String decider, String code, String because) {
        return new Decision(Type.NO, decider, code, because);
    }

    /** @param code as {@link #no}. */
    public static Decision throttle(String decider, String code, String because) {
        return new Decision(Type.THROTTLE, decider, code, because);
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

        /**
         * What distinguishes this candidate's refusal from another's — the deciding rules, not their prose.
         *
         * <p>Two candidates share a key when the same rules refused them for the same reasons, whatever the
         * sentences say about which node it was or how full its disk is.
         */
        public List<String> reasonKey() {
            return deciding().stream()
                .map(decision -> decision.decider() + "/" + decision.code())
                .sorted()
                .toList();
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
