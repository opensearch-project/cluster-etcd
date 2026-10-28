package org.opensearch.supervisor.model.monitor;

import java.util.Comparator;
import java.util.Objects;

/**
 * One thing a human should be told about, and why.
 *
 * @param because <b>the supervisor's own sentence, verbatim.</b> Not a paraphrase, and this is a rule rather than
 *     a preference: an alerter reading projections is one step removed from the reasoning that produced them, so
 *     the moment it starts writing its own explanations there are two accounts of the same event and the more
 *     confident one is the one further from the evidence. Where the sentence is missing or vague, the fix belongs
 *     in the supervisor.
 * @param subject what it is about, in the supervisor's own rendering — a tenant/index/shard key, a pool id, a node
 *     id. Never reformatted, for the same reason a subject has one representation in the diff: anything that
 *     groups alerts across samples keys on it.
 */
public record Alert(Severity severity, String rule, String subject, String because) {

    public Alert {
        Objects.requireNonNull(severity, "severity");
        Objects.requireNonNull(rule, "rule");
        if (because == null || because.isBlank()) {
            throw new IllegalArgumentException(
                "alert " + rule + " on " + subject + " has no reason; an alert a human cannot act on is noise,"
                    + " and every rule here has a sentence available to it"
            );
        }
        subject = subject == null ? "-" : subject;
    }

    /** What an operator wants: the worst first, then grouped by rule so repeats read together. */
    public static final Comparator<Alert> BY_URGENCY = Comparator
        .comparingInt((Alert alert) -> alert.severity().ordinal()).reversed()
        .thenComparing(Alert::rule)
        .thenComparing(Alert::subject);

    /** The exit status this alert justifies. {@code 2} for critical, {@code 1} for warning, {@code 0} for none. */
    public int exitStatus() {
        return severity == Severity.CRITICAL ? 2 : 1;
    }

    public enum Severity {
        /** Worth knowing, never worth waking anyone. */
        INFO,
        /** Someone should look today. */
        WARNING,
        /** Someone should look now. */
        CRITICAL
    }
}
