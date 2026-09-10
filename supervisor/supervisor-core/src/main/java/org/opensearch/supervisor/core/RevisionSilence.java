package org.opensearch.supervisor.core;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * How long a key's revision has been unchanged, as observed by <b>this</b> process.
 *
 * <p>The one idea underneath both node liveness and lease expiry, extracted because getting it right twice
 * independently is how the two would come to disagree. Neither is judged from a timestamp written by the
 * party being watched: two clocks that disagree by a minute would let a supervisor evict a healthy node or
 * steal a healthy lease, and skew is not rare.
 *
 * <p>It works because both watched documents change on every write — a heartbeat carries a fresh timestamp,
 * a renewed lease a fresh expiry — so the revision always advances. That is why those fields are required
 * rather than optional, and it is the only coupling between this class and any document format.
 *
 * <p><b>Silence is never measured from before this process started.</b> The map lives in memory, and R2.3
 * forbids depending on memory, so a restarted supervisor must not read its own youth as the watched party's
 * silence. Without that clamp a restart evicts every node and steals every lease at once.
 */
final class RevisionSilence {

    private record Seen(long revision, Instant at) {}

    private final Map<String, Seen> seen = new HashMap<>();
    private final Instant startedAt;

    RevisionSilence(Instant startedAt) {
        this.startedAt = Objects.requireNonNull(startedAt, "startedAt");
    }

    /**
     * Records an observation and answers how long the revision has been unchanged.
     *
     * @return {@link Duration#ZERO} if this is the first sighting or the revision has just moved
     */
    Duration observe(String key, long revision, Instant now) {
        Seen previous = seen.get(key);
        if (previous == null || previous.revision() != revision) {
            seen.put(key, new Seen(revision, now));
            return Duration.ZERO;
        }
        Instant since = previous.at().isBefore(startedAt) ? startedAt : previous.at();
        Duration silent = Duration.between(since, now);
        return silent.isNegative() ? Duration.ZERO : silent;
    }

    /** How long this process has been running, which bounds every answer above. */
    Duration sinceStart(Instant now) {
        Duration running = Duration.between(startedAt, now);
        return running.isNegative() ? Duration.ZERO : running;
    }

    void forget(String key) {
        seen.remove(key);
    }

    void retain(Set<String> keys) {
        seen.keySet().retainAll(keys);
    }

    int tracked() {
        return seen.size();
    }
}
