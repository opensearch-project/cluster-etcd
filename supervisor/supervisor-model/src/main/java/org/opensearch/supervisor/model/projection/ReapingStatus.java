package org.opensearch.supervisor.model.projection;

import org.opensearch.supervisor.model.Json;

import java.util.List;

/**
 * {@code supervisor/status/pools/<pool-id>/reaping} — which nodes this pool deleted, and which it would not.
 *
 * <p><b>The most destructive thing this service does, made reviewable after the fact.</b> {@code NodeReaper}
 * returned its report into a field and nothing else, so the reason behind every document the supervisor deleted —
 * and every one it declined to delete — existed only in the memory of the process that decided it. A reap could
 * not be reviewed afterwards at all, which is a poor property for the one action here that removes a node's four
 * documents.
 *
 * <p>The {@code kept} half is the more useful of the two in practice, and is the reason this is not just an audit
 * log. A node that keeps being declined tells an operator something is pinned that should not be — most often a
 * shard whose only started copy is on a machine nobody can reach — and that state is otherwise silent, because
 * declining to act produces no finding.
 *
 * <p>Bounded like every other document here. Reaps are rare, so the cap is generous and the count is exact even
 * when the list is cut.
 */
public record ReapingStatus(
    long pass,
    String holder,
    long observedAt,
    int reapedTotal,
    int keptTotal,
    List<Reaped> reaped,
    List<Kept> kept
) {

    /** Entries kept per list. Exact counts sit beside them, so truncation never makes the summary wrong. */
    public static final int MAX_SHOWN = 20;

    public record Reaped(String node, List<String> keys, String because) {}

    public record Kept(String node, String because) {}

    public byte[] toBytes() {
        return Json.write(this, "reaping");
    }

    public static ReapingStatus from(byte[] bytes) {
        return Json.read(bytes, ReapingStatus.class, "reaping");
    }
}
