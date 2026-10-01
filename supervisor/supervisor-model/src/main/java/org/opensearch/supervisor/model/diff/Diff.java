package org.opensearch.supervisor.model.diff;

import org.opensearch.supervisor.model.PoolId;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Everything wrong with one pool on one pass, ordered so that truncation drops the least interesting. */
public record Diff(long pass, PoolId pool, List<Finding> findings) {

    public Diff {
        Objects.requireNonNull(pool, "pool");
        List<Finding> sorted = new ArrayList<>(findings == null ? List.of() : findings);
        sorted.sort(Finding.BY_INTEREST);
        findings = List.copyOf(sorted);
    }

    public boolean isEmpty() {
        return findings.isEmpty();
    }

    public Map<FindingKind, Integer> countsByKind() {
        Map<FindingKind, Integer> counts = new EnumMap<>(FindingKind.class);
        for (Finding finding : findings) {
            counts.merge(finding.kind(), 1, Integer::sum);
        }
        return counts;
    }

    public List<Finding> of(FindingKind kind) {
        return findings.stream().filter(finding -> finding.kind() == kind).toList();
    }
}
