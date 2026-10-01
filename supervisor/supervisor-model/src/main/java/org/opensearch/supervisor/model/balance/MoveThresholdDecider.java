package org.opensearch.supervisor.model.balance;

import org.opensearch.supervisor.model.allocate.Decision;

/**
 * Refuses moves that do not make things better, which is what keeps the balancer from running forever.
 *
 * <p>The arithmetic is the reason this is a rule and not a heuristic. Moving a copy of normalised weight
 * {@code w} from a node at load {@code A} to one at load {@code B} leaves them at {@code A - w} and
 * {@code B + w}. Since {@link LoadModel#spread()} is the gap between the busiest and idlest node, the move
 * reduces it only if the target does not end up worse than the source began:
 *
 * <pre>
 *     B + w &lt; A     i.e.     A - B &gt; w
 * </pre>
 *
 * <p>If {@code A - B == w} the move exactly swaps the two nodes' positions and achieves nothing; if
 * {@code A - B &lt; w} it overshoots and the pool is <em>more</em> lopsided than before — and the same rule
 * applied to the new state will propose moving it straight back. That is precisely the shape of an oscillating
 * balancer: every individual move looks locally sensible, and the cluster moves data forever without ever
 * converging. Requiring strict improvement is what makes the loop terminate, because {@code spread()} is a
 * non-negative quantity that every accepted move strictly decreases.
 *
 * <p>The threshold is a margin on top: {@code A - B > w * (1 + threshold)}. It buys hysteresis, which matters
 * because loads are measured from an observation that is always slightly out of date — sizes change as segments
 * merge, and a move justified by a gap of half a percent may not have been justified by the time it lands. It
 * also means a pool is allowed to be a bit uneven, which is the right answer: an operator would rather have a
 * cluster that is five percent lopsided than one that is permanently moving data to avoid being.
 *
 * <p>A refusal here is a {@code NO} rather than a {@code THROTTLE}, unlike most of the other rebalance rules.
 * Nothing about waiting changes this answer — it is a statement about the shape of the pool, and the honest
 * report is "this is as balanced as it is going to get", not "try again shortly".
 */
public final class MoveThresholdDecider implements RebalanceDecider {

    public static final String NAME = "move_threshold";

    /**
     * Ten percent of the copy's own weight.
     *
     * <p>Low enough that a genuinely lopsided pool still gets corrected, high enough that a pool jittering
     * around its balance point is left alone. The units make this scale sensibly on its own: because the margin
     * is a fraction of the moving copy's weight rather than an absolute figure, moving a large shard has to
     * justify itself more than moving a small one, which is the right bias.
     */
    public static final double DEFAULT_THRESHOLD = 0.10;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision canRebalance(RebalanceContext context) {
        double weight = context.copyWeight();
        double gap = context.sourceLoad() - context.targetLoad();
        double required = weight * (1.0 + context.threshold());

        if (weight <= 0.0) {
            return Decision.no(NAME, "weightless", "this copy weighs nothing under the current signals, so moving it would"
                + " change no node's load and cost bytes for nothing");
        }
        if (gap > required) {
            return Decision.yes(NAME, String.format(java.util.Locale.ROOT,
                "moving this copy shifts %.3f of load from a node at %.3f to one at %.3f, a gap of %.3f, which"
                    + " clears the %.3f needed to leave the pool strictly less lopsided than it is now",
                weight, context.sourceLoad(), context.targetLoad(), gap, required));
        }
        if (gap <= weight) {
            return Decision.no(NAME, "overshoot", String.format(java.util.Locale.ROOT,
                "the load gap between these nodes is %.3f but the copy itself weighs %.3f, so moving it would"
                    + " overshoot: the target would end up at %.3f against the source's current %.3f, and the"
                    + " same reasoning would then propose moving it back",
                gap, weight, context.targetLoad() + weight, context.sourceLoad()));
        }
        return Decision.no(NAME, "below_margin", String.format(java.util.Locale.ROOT,
            "moving this copy would improve the spread, but only by %.3f against the %.3f margin required;"
                + " the pool is close enough to balanced that moving %d MiB is not worth it",
            gap - weight, required - weight, context.copy().sizeBytes() / (1024 * 1024)));
    }
}
