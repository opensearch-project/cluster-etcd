package org.opensearch.supervisor.model.diff;

import java.util.Comparator;
import java.util.Objects;

/**
 * One thing the supervisor believes is wrong, and why.
 *
 * @param because <b>required, deliberately.</b> R7.4 asks for the decision rather than the action:
 *     "moving shard X to node Y because node Z has been silent for 40s" is debuggable and the same sentence
 *     without its clause is not. A required constructor argument is the only version of that rule which
 *     survives a deadline — an optional field would be empty within a month.
 * @param firstObservedPass when this finding first appeared, which is what makes "stable and non-empty"
 *     expressible. {@code REQUIREMENTS.md} names that as the alarm.
 * @param passes how many passes it has held for
 */
public record Finding(
    FindingKind kind,
    Subject subject,
    String because,
    long firstObservedPass,
    long passes
) {

    public Finding {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(subject, "subject");
        if (because == null || because.isBlank()) {
            throw new IllegalArgumentException(
                "finding " + kind + " on " + subject + " has no reason; a finding without a because is not"
                    + " debuggable and should not pass review"
            );
        }
    }

    /** A finding seen for the first time on this pass. */
    /**
     * The key a refusal about <em>this</em> finding is filed under.
     *
     * <p>Includes the kind, and that is the whole point. Three kinds of unassigned copy share one subject — a
     * shard — so filing a refusal by subject alone attributes it to every finding about that shard. It produced a
     * genuinely misleading report: on a one-node pool the primary was placed, the search replica was then
     * correctly refused because the only node had just been given the primary, and the refusal was rendered
     * against the primary's finding. Whoever read it went looking for a node that "already holds" an index it had
     * never been assigned, and the stored documents did not support the claim because the claim was about a
     * different copy.
     */
    public String refusalKey() {
        return kind.name() + "@" + subject.key();
    }

    public static Finding of(FindingKind kind, Subject subject, long pass, String because) {
        return new Finding(kind, subject, because, pass, 1);
    }

    public Severity severity() {
        return kind.severityAfter(passes);
    }

    /** Most severe first, then oldest first, so truncating a diff document drops the least interesting. */
    public static final Comparator<Finding> BY_INTEREST = Comparator
        .comparing((Finding finding) -> finding.severity().ordinal()).reversed()
        .thenComparing(Comparator.comparingLong(Finding::passes).reversed())
        .thenComparing(finding -> finding.kind().name())
        .thenComparing(finding -> finding.subject().key());
}
