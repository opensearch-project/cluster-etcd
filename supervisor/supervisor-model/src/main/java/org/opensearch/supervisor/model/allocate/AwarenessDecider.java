package org.opensearch.supervisor.model.allocate;

import org.opensearch.supervisor.model.NodeId;

import java.util.Map;
import java.util.Set;

/**
 * Spreads copies of a shard evenly across the values of an awareness attribute — zones, racks, whatever the
 * operator names.
 *
 * <p>The rule that makes redundancy mean something at cloud scale. Three copies in one availability zone
 * survive a disk, not a zone; and the whole reason a tenant runs three copies is usually the zone.
 *
 * <p>The bound is OpenSearch's, and worth stating in full because it is less obvious than it looks:
 * <b>at most {@code ceil(copies / values)} copies of a shard in any one attribute value.</b> Three copies
 * across three zones allows one each; three copies across two zones allows two in one and one in the other,
 * because forbidding that would make the shard unplaceable rather than imperfectly placed. Ceiling division is
 * what keeps the rule from being stricter than the cluster can satisfy.
 *
 * <p>The values counted are those <em>discovered on nodes</em>, not a configured list. That has a consequence
 * worth knowing: if every node in a zone goes away, the zone stops existing as far as this rule is concerned,
 * and the copies that were there become placeable elsewhere. That is the behaviour you want during an outage
 * and the opposite of what a configured list would give you.
 */
public final class AwarenessDecider implements AllocationDecider {

    public static final String NAME = "awareness";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canAllocate(AllocationContext context, NodeId candidate) {
        if (context.awarenessAttributes().isEmpty()) {
            return Decision.yes(NAME, "no awareness attributes are configured for this pool");
        }

        for (String attribute : context.awarenessAttributes()) {
            var candidateValue = context.attributeOf(candidate, attribute);
            if (candidateValue.isEmpty()) {
                return Decision.no(NAME, "node [" + candidate + "] declares no [" + attribute + "] attribute,"
                    + " and this pool spreads copies across " + attribute + " values; a node that cannot say"
                    + " where it is cannot be placed into a spread");
            }

            Set<String> values = context.attributeValues(attribute);
            if (values.isEmpty()) {
                continue;
            }

            int copies = context.desiredCopiesOfThisShard();
            int maximumPerValue = (copies + values.size() - 1) / values.size();
            Map<String, Integer> perValue = context.copiesPerAttributeValue(attribute);
            int alreadyHere = perValue.getOrDefault(candidateValue.get(), 0);

            if (alreadyHere >= maximumPerValue) {
                return Decision.no(NAME, "[" + attribute + "=" + candidateValue.get() + "] already holds "
                    + alreadyHere + " of the " + copies + " copies of this shard, and with " + values.size()
                    + " known " + attribute + " values no value may hold more than " + maximumPerValue);
            }
        }
        return Decision.yes(NAME, "node [" + candidate + "] keeps this shard's copies spread across "
            + context.awarenessAttributes());
    }
}
