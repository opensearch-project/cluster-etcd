package org.opensearch.supervisor.model.diff;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * How long each finding has been true — the only cross-pass memory the diff has, and the thing that makes
 * "stable and non-empty" expressible.
 *
 * <p>{@code REQUIREMENTS.md} names that as the alarm, and it cannot be computed from one observation: a
 * reconciler that is quietly failing looks exactly like one with nothing to do, and both produce no output.
 * Twelve findings shrinking is a recovery; twelve findings flat is the thing to wake someone for.
 *
 * <p>It is also what keeps expected torn state from crying wolf. R2.5 promises the supervisor <em>will</em>
 * observe an index without its alias, so the alarm has to be that a finding is <b>old</b> rather than that
 * it exists.
 *
 * <p>Held in memory and bounded by the number of distinct findings. Losing it on restart is safe and
 * required to be: R2.3 forbids depending on memory, and the cost of losing this is that every finding looks
 * new for one pass — conservative in the right direction, since nothing acts on a young finding.
 */
public final class FindingAges {

    private record Key(FindingKind kind, String subject) {}

    private final Map<Key, Long> firstSeen = new HashMap<>();

    /**
     * Stamps each finding with when it first appeared, and forgets findings that have cleared.
     *
     * @return the same findings, with {@code firstObservedPass} and {@code passes} filled in
     */
    public List<Finding> age(List<Finding> findings, long pass) {
        Set<Key> present = new HashSet<>();
        List<Finding> aged = new ArrayList<>(findings.size());

        for (Finding finding : findings) {
            Key key = new Key(finding.kind(), finding.subject().key());
            present.add(key);
            long first = firstSeen.computeIfAbsent(key, ignored -> pass);
            aged.add(new Finding(finding.kind(), finding.subject(), finding.because(), first, pass - first + 1));
        }

        // A finding that has cleared loses its history, so that the same problem recurring later is a new
        // problem rather than an apparently ancient one. Also what bounds this map.
        firstSeen.keySet().retainAll(present);
        return aged;
    }

    public int size() {
        return firstSeen.size();
    }
}
