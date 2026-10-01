package org.opensearch.supervisor.core;

import org.opensearch.supervisor.model.PoolId;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Decides which pools this supervisor should hold. A pure function, and deliberately a simple one.
 *
 * <p><b>The balancer is advisory; the lease is authoritative.</b> That sentence is what makes "minimal
 * locking" true. Every member runs this independently over data that may be stale, and <em>they do not have
 * to agree</em>: if two decide to claim the same pool, the compare-and-set serialises them and one loses.
 * Disagreement costs a wasted write, never correctness. So this can be approximate, and can be replaced
 * without touching anything that matters.
 */
public final class Balancer {

    /**
     * @param hysteresis how far over fair share is tolerated before releasing. Rebalancing throws away a
     *     warm revision cache and costs a full re-observation, so the bias is toward stability rather than
     *     toward a perfectly even split.
     * @param releaseAfterPasses sustained passes over share before giving a pool up. With hysteresis, this
     *     is what stops the characteristic failure of every rebalancer — two members handing one pool back
     *     and forth for ever.
     * @param defaultPoolWeight assumed weight of a pool nobody owns, since its weight is published by its
     *     owner and an unowned pool has none.
     */
    public record Config(double hysteresis, int releaseAfterPasses, double defaultPoolWeight) {
        public static Config defaults() {
            return new Config(0.25, 20, 1.0);
        }
    }

    /**
     * Everything the decision depends on, so that the decision itself can be a function of nothing else.
     *
     * @param poolWeights <b>must contain every pool that exists</b>, not only those that have published a
     *     weight. Fair share is the sum of this map, so a partial map shrinks the total while a member's
     *     holdings are still counted in full — which made every member permanently over share and the fleet
     *     churn for ever. {@code PoolWeights} keeps it total; the fallback below is belt and braces.
     */
    public record View(
        Map<PoolId, Double> poolWeights,
        Set<PoolId> claimable,
        Map<String, Double> memberCapacity,
        Map<String, Double> memberWeightHeld,
        String me,
        Set<PoolId> held
    ) {
        public View {
            poolWeights = Map.copyOf(poolWeights);
            claimable = Set.copyOf(claimable);
            memberCapacity = Map.copyOf(memberCapacity);
            memberWeightHeld = Map.copyOf(memberWeightHeld);
            held = Set.copyOf(held);
            Objects.requireNonNull(me, "me");
        }
    }

    public sealed interface Decision {
        record Claim(PoolId pool, String because) implements Decision {}

        record Release(PoolId pool, String because) implements Decision {}

        record Hold(String because) implements Decision {}
    }

    private Balancer() {}

    /**
     * At most one claim or one release per pass, never both.
     *
     * <p>One change at a time is what keeps the fleet's movement observable: an operator watching the
     * convergence documents sees a sequence of single decisions rather than a reshuffle whose intermediate
     * states nobody recorded.
     */
    public static Decision decide(View view, int passesOverShare, Config config) {
        double myWeight = weightOf(view.held(), view.poolWeights(), config);
        double fairShare = fairShareOf(view, config);

        // An unowned pool is worse than an unbalanced fleet: nothing is reconciling it at all, and no
        // amount of balance makes up for that. So claiming comes first, and the last clause below makes
        // sure a pool heavier than anyone's fair share still gets taken.
        Optional<PoolId> toClaim = chooseClaim(view, config, myWeight, fairShare);
        if (toClaim.isPresent()) {
            return new Decision.Claim(toClaim.get(), claimReason(view, config, myWeight, fairShare, toClaim.get()));
        }

        if (view.memberCapacity().size() <= 1) {
            return new Decision.Hold("sole live member, so releasing anything would leave it unowned");
        }
        if (myWeight <= fairShare * (1 + config.hysteresis())) {
            return new Decision.Hold(String.format(
                "holding %.1f of a fair share of %.1f, inside the %.0f%% hysteresis band",
                myWeight, fairShare, config.hysteresis() * 100
            ));
        }
        if (passesOverShare < config.releaseAfterPasses()) {
            return new Decision.Hold(String.format(
                "holding %.1f against a fair share of %.1f, but only for %d of %d passes; releasing sooner"
                    + " is how a rebalancer starts oscillating",
                myWeight, fairShare, passesOverShare, config.releaseAfterPasses()
            ));
        }

        return lightestOf(view.held(), view.poolWeights(), config)
            .<Decision>map(pool -> new Decision.Release(pool, String.format(
                "holding %.1f against a fair share of %.1f for %d passes; giving up the lightest pool [%s]"
                    + " to move toward it without a large jump",
                myWeight, fairShare, passesOverShare, pool
            )))
            .orElseGet(() -> new Decision.Hold("over share but holding nothing to give up"));
    }

    private static Optional<PoolId> chooseClaim(View view, Config config, double myWeight, double fairShare) {
        if (view.claimable().isEmpty()) {
            return Optional.empty();
        }

        // Prefer the heaviest pool that still fits under fair share: it converges in fewer passes than
        // taking the lightest each time, and each claim costs a cold cache.
        Optional<PoolId> fits = view.claimable()
            .stream()
            .filter(pool -> myWeight + weight(pool, view.poolWeights(), config) <= fairShare)
            .max(Comparator.comparingDouble((PoolId pool) -> weight(pool, view.poolWeights(), config))
                .thenComparing(PoolId::value));
        if (fits.isPresent()) {
            return fits;
        }

        // Nothing fits. Somebody still has to take it, so the least-loaded member does — decided the same
        // way by every member, from published data, with the id as a tiebreak. No coordination needed, and
        // if two disagree because their views differ, the compare-and-set sorts it out.
        return amLeastLoaded(view) ? lightestOf(view.claimable(), view.poolWeights(), config) : Optional.empty();
    }

    private static String claimReason(View view, Config config, double myWeight, double fairShare, PoolId pool) {
        double poolWeight = weight(pool, view.poolWeights(), config);
        if (myWeight + poolWeight <= fairShare) {
            return String.format(
                "pool [%s] is unowned and weighs %.1f, which fits under a fair share of %.1f while holding %.1f",
                pool, poolWeight, fairShare, myWeight
            );
        }
        return String.format(
            "pool [%s] is unowned and fits nobody's fair share, and this is the least loaded member at %.1f;"
                + " an unowned pool is reconciled by nobody, which is worse than an unbalanced fleet",
            pool, myWeight
        );
    }

    /**
     * Whether this member is the one that should take a pool nobody can fit.
     *
     * <p>Every member computes this from the same published figures, so they normally agree without talking.
     * The id tiebreak is what makes it a total order rather than a coin flip.
     */
    private static boolean amLeastLoaded(View view) {
        String leader = view.memberCapacity()
            .keySet()
            .stream()
            .min(Comparator.<String>comparingDouble(member -> loadFactor(view, member)).thenComparing(member -> member))
            .orElse(view.me());
        return view.me().equals(leader);
    }

    /** Weight held relative to capacity, so a big member and a small one are compared fairly. */
    private static double loadFactor(View view, String member) {
        double capacity = view.memberCapacity().getOrDefault(member, 1.0);
        return view.memberWeightHeld().getOrDefault(member, 0.0) / (capacity <= 0 ? 1.0 : capacity);
    }

    /** Total pool weight divided in proportion to configured capacity. */
    public static double fairShareOf(View view, Config config) {
        double totalWeight = view.poolWeights()
            .keySet()
            .stream()
            .mapToDouble(pool -> weight(pool, view.poolWeights(), config))
            .sum();
        double totalCapacity = view.memberCapacity().values().stream().mapToDouble(Double::doubleValue).sum();
        double myCapacity = view.memberCapacity().getOrDefault(view.me(), 1.0);
        if (totalCapacity <= 0) {
            return totalWeight;
        }
        return totalWeight * (myCapacity / totalCapacity);
    }

    private static double weightOf(Set<PoolId> pools, Map<PoolId, Double> weights, Config config) {
        return pools.stream().mapToDouble(pool -> weight(pool, weights, config)).sum();
    }

    private static double weight(PoolId pool, Map<PoolId, Double> weights, Config config) {
        Double published = weights.get(pool);
        return published == null || published <= 0 ? config.defaultPoolWeight() : published;
    }

    private static Optional<PoolId> lightestOf(Set<PoolId> pools, Map<PoolId, Double> weights, Config config) {
        return pools.stream()
            .min(Comparator.comparingDouble((PoolId pool) -> weight(pool, weights, config))
                .thenComparing(PoolId::value));
    }

    /** Convenience for building a view's held-weight map from member records. */
    public static Map<String, Double> weightsHeldFrom(Map<String, Double> published, String me, double mine) {
        Map<String, Double> weights = new LinkedHashMap<>(published);
        weights.put(me, mine);
        return weights;
    }

    /** Pools that exist but nobody holds, for the housekeeping document. */
    public static Set<PoolId> unowned(Set<PoolId> all, Map<String, Set<PoolId>> heldByMember) {
        Set<PoolId> unowned = new LinkedHashSet<>(all);
        heldByMember.values().forEach(unowned::removeAll);
        return unowned;
    }
}
