package org.opensearch.supervisor.model.status;

import org.opensearch.supervisor.model.Json;

import java.util.List;
import java.util.Map;

/**
 * {@code tenant/<t>/status/awareness} — the distinct awareness attribute values actually present on nodes.
 *
 * <p>This is the half of weighted-routing validation the control plane cannot do today: it can check that a
 * request names a known attribute, but not that the values it weights exist anywhere. R5.4.
 *
 * <p>Derivable from the per-node documents, and kept anyway because it is one tiny document that saves a
 * prefix scan on the validation path of every weighted-routing request.
 *
 * <p><b>One hazard worth stating rather than discovering.</b> A value disappears from here when no reachable
 * node reports it — so a zone whose nodes are all down would silently stop being a valid weighted-routing
 * target, and a request naming it would be rejected for a reason that reads like a typo. {@code STALE} nodes
 * are therefore included and only {@code LOST} ones excluded, which keeps a brief network problem from
 * rewriting what the control plane will accept.
 */
public record AwarenessStatus(Map<String, List<String>> attributes, long observedAt) {

    public AwarenessStatus {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    public byte[] toBytes() {
        return Json.write(this, "awareness status");
    }

    public static AwarenessStatus from(byte[] bytes) {
        return Json.read(bytes, AwarenessStatus.class, "awareness status");
    }
}
