package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;

/**
 * One placement rule.
 *
 * <p>A rule per class, following OpenSearch's own structure, and worth it for the same reasons: each one is
 * small enough to read in full, independently testable against a hand-built situation, independently
 * disableable when it turns out to be the one causing trouble in production, and able to explain its own
 * refusals without any other rule's help.
 *
 * <p>Each is a <b>pure function</b> of the context and the candidate. No decider reads the store, holds state
 * between calls, or knows what any other decider decided.
 */
public interface AllocationDecider {

    /** Short, stable name, used as the label on a decision and in the diff. */
    String name();

    /** Whether a copy may be placed on this node. */
    Decision canAllocate(AllocationContext context, NodeId candidate);
}
