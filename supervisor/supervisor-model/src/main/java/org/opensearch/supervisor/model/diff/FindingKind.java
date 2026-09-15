package org.opensearch.supervisor.model.diff;

import static org.opensearch.supervisor.model.diff.Severity.CRITICAL;
import static org.opensearch.supervisor.model.diff.Severity.INFO;
import static org.opensearch.supervisor.model.diff.Severity.WARNING;

/**
 * Everything the supervisor can find wrong. {@code DESIGN.md} §6 is the table this mirrors.
 *
 * <p>The severity here is the <em>base</em>. Two kinds escalate with age rather than being born serious,
 * which is the rule that keeps torn state from crying wolf: R2.5 promises the supervisor <em>will</em> see
 * an index without its alias, so alarming the first time is how a correct system earns a reputation for
 * false alarms. The alarm is that a finding is <b>old</b>, not that it exists.
 */
public enum FindingKind {

    /** An open index's shard has no primary anywhere in the pool. */
    UNASSIGNED_PRIMARY(CRITICAL),

    /** Fewer {@code REPLICA} copies than desired state asks for. */
    UNASSIGNED_REPLICA(WARNING),

    /**
     * Fewer {@code SEARCH_REPLICA} copies than desired state asks for.
     *
     * <p>Its own kind rather than a field on {@link Finding}, for two reasons. The planner has to know which role
     * a missing copy wants — the roles have different placement rules, since only a document-replication replica
     * has to wait for a serving primary — and it dispatches on kind. And an operator reading the counts by kind
     * gets "two search replicas unplaced, no replicas unplaced", which is the distinction they would otherwise
     * have to reconstruct from the reasons.
     */
    UNASSIGNED_SEARCH_REPLICA(WARNING),

    /**
     * Desired state asks for copies this supervisor will not plan, and says why.
     *
     * <p>The general form of a specific embarrassment: {@code index.number_of_search_replicas} was read by
     * nothing for as long as it existed, so a tenant who set it got no copy, no complaint, and a status claiming
     * everything asked for had been delivered. The count <em>defined</em> what was wanted, which made anything
     * outside it invisible by construction.
     *
     * <p>So a request this supervisor declines has to leave a reason behind rather than being dropped from the
     * arithmetic. Its copies are deliberately <b>not</b> counted as unassigned: unassigned means "intended and
     * not yet placed", and reporting a permanently unplaceable request that way would leave the index yellow for
     * ever with nothing an operator could do about it.
     */
    DESIRED_COPY_IGNORED(WARNING),

    /** More copies than desired. */
    SURPLUS_COPY(WARNING),

    /** A copy whose index uuid is absent from desired state. Acts only after K consecutive passes. */
    ORPHANED_COPY(WARNING),

    /** A copy of a tenant's shard on a node outside that tenant's pool. */
    FOREIGN_COPY(CRITICAL),

    /** A copy built against a different revision of the index document than the current one. */
    STALE_CONFIG(WARNING),

    /** A node's heartbeat key has not changed revision within its staleness threshold. */
    NODE_STALE(WARNING),

    /** Past the second, longer threshold. Its copies are presumed gone. */
    NODE_LOST(CRITICAL),

    /** A heartbeat with no registration: the node cannot be placed on, but keeps what it has. */
    NODE_UNREGISTERED(WARNING),

    /**
     * Nodes in one pool write their segments to <em>different</em> remote repositories.
     *
     * <p>{@code CRITICAL}, and the only finding here whose failure mode is a lie rather than an absence. Copies of
     * a shard share one remote path, so every node that might hold one has to be configured with the same
     * repository. A node cannot check this — it sees only its own attributes — and the supervisor sees all of them,
     * which makes it the only party in the system that can. If nobody checks, a search replica placed on the odd
     * node out reads an empty store, finds nothing, and reports itself perfectly healthy.
     *
     * <p>Nodes declaring <em>no</em> repository are not a disagreement and are not counted: a coordinator has none,
     * and a data node without one is refused a search-only copy by {@code RemoteStoreDecider} with its own reason.
     * Absence is somebody else's finding. Disagreement is this one.
     */
    POOL_REPOSITORY_MISMATCH(CRITICAL),

    /** A node in no pool, or naming a pool with no definition. */
    NODE_UNPOOLED(WARNING),

    /** A registered tenant with no pool binding — entirely unmanaged, and silent everywhere else. */
    TENANT_UNPOOLED(CRITICAL),

    /** No live member holds a pool's lease. */
    POOL_UNOWNED(CRITICAL),

    /** An alias naming an absent index, or a data stream naming an absent backing index. */
    TORN_DESIRED_STATE(INFO, true),

    /** A task claimed past its lease, or at its attempt limit. */
    TASK_STUCK(WARNING),

    /**
     * A {@code status/} document older than N passes.
     *
     * <p><b>Never raised, and that is the honest state of it.</b> Only a pool's owner writes projections, and
     * it rewrites every one whose content has changed on the pass it takes the pool over — so a stale
     * projection would mean the owner failed to write, which it would already know about. Kept because a
     * second writer or a partial write would make it meaningful, and removing it would lose the reason it was
     * expected to be needed.
     */
    PROJECTION_STALE(WARNING),

    /** Desired copies that no live node in the pool can satisfy. */
    UNPLACEABLE(CRITICAL),

    /** A docrep replica placed for a shard with no primary yet. */
    PRIMARY_BEFORE_REPLICA(WARNING),

    /** A node reporting a shard state or role this build does not recognise. */
    UNRECOGNISED_STATE(WARNING),

    /** A document under a prefix we own that could not be parsed at all. */
    UNREADABLE_DOCUMENT(WARNING);

    /** Findings older than this many passes escalate, if their kind escalates at all. */
    public static final long ESCALATE_AFTER_PASSES = 10;

    private final Severity base;
    private final boolean escalatesWithAge;

    FindingKind(Severity base) {
        this(base, false);
    }

    FindingKind(Severity base, boolean escalatesWithAge) {
        this.base = base;
        this.escalatesWithAge = escalatesWithAge;
    }

    public Severity base() {
        return base;
    }

    /**
     * Severity given how long this has been true.
     *
     * <p>An index without its alias for one pass is normal and must not raise an alarm; the same finding at
     * three hundred passes is a stuck reconciler.
     */
    public Severity severityAfter(long passes) {
        if (escalatesWithAge && passes >= ESCALATE_AFTER_PASSES) {
            return WARNING;
        }
        return base;
    }
}
