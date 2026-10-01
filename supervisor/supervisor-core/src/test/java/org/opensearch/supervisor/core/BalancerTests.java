package org.opensearch.supervisor.core;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.PoolId;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The balancer, which is a pure function and therefore testable to death.
 *
 * <p>Worth remembering while reading these: the balancer is advisory and the lease is authoritative. None of
 * these decisions has to be right, only convergent — a wrong one costs a wasted compare-and-set. That is
 * what licenses a heuristic this simple.
 */
class BalancerTests {

    private static final PoolId A = PoolId.of("pool-a");
    private static final PoolId B = PoolId.of("pool-b");
    private static final PoolId C = PoolId.of("pool-c");
    private static final Balancer.Config CONFIG = Balancer.Config.defaults();

    private static Map<PoolId, Double> weights(Object... pairs) {
        Map<PoolId, Double> weights = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            weights.put((PoolId) pairs[i], (Double) pairs[i + 1]);
        }
        return weights;
    }

    private static Map<String, Double> members(Object... pairs) {
        Map<String, Double> members = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            members.put((String) pairs[i], (Double) pairs[i + 1]);
        }
        return members;
    }

    private static Balancer.View view(
        Map<PoolId, Double> poolWeights,
        Set<PoolId> claimable,
        Map<String, Double> capacities,
        Map<String, Double> heldWeights,
        String me,
        Set<PoolId> held
    ) {
        return new Balancer.View(poolWeights, claimable, capacities, heldWeights, me, held);
    }

    // ---------------------------------------------------------------- claiming

    @Test
    void aLoneSupervisorTakesEverythingOnePoolAtATime() {
        // One change per pass keeps the fleet's movement observable: a sequence of single decisions rather
        // than a reshuffle whose intermediate states nobody recorded.
        var decision = Balancer.decide(view(
            weights(A, 10.0, B, 10.0), Set.of(A, B), members("s1", 1.0), members("s1", 0.0), "s1", Set.of()
        ), 0, CONFIG);

        assertInstanceOf(Balancer.Decision.Claim.class, decision);
    }

    @Test
    void claimsTheHeaviestPoolThatStillFitsUnderFairShare() {
        // Converges in fewer passes than taking the lightest each time, and each claim costs a cold cache.
        var decision = Balancer.decide(view(
            weights(A, 2.0, B, 8.0, C, 40.0),
            Set.of(A, B, C),
            members("s1", 1.0, "s2", 1.0),
            members("s1", 0.0, "s2", 0.0),
            "s1",
            Set.of()
        ), 0, CONFIG);

        // Fair share is 25 of a total of 50. C is 40 and does not fit; B at 8 is the heaviest that does.
        assertEquals(B, ((Balancer.Decision.Claim) decision).pool());
    }

    @Test
    void anUnownedPoolTooHeavyForAnybodyIsStillTakenByTheLeastLoadedMember() {
        // The rule that matters most here: an unowned pool is reconciled by nobody, which is worse than an
        // unbalanced fleet. Fair share must not be allowed to leave one stranded.
        var s1 = view(
            weights(A, 1000.0), Set.of(A),
            members("s1", 1.0, "s2", 1.0), members("s1", 0.0, "s2", 0.0), "s1", Set.of()
        );
        var s2 = view(
            weights(A, 1000.0), Set.of(A),
            members("s1", 1.0, "s2", 1.0), members("s1", 0.0, "s2", 0.0), "s2", Set.of()
        );

        assertInstanceOf(Balancer.Decision.Claim.class, Balancer.decide(s1, 0, CONFIG));
        // And exactly one of them decides to, without any coordination: same figures, id as tiebreak.
        assertInstanceOf(Balancer.Decision.Hold.class, Balancer.decide(s2, 0, CONFIG));
    }

    @Test
    void theLeastLoadedMemberIsChosenByLoadFactorNotRawWeight() {
        // A big member holding 10 is less loaded than a small one holding 6. Comparing raw weight would
        // pile work onto the machine least able to take it.
        var small = view(
            weights(A, 100.0, B, 10.0, C, 6.0), Set.of(A),
            members("big", 4.0, "small", 1.0),
            members("big", 10.0, "small", 6.0),
            "small", Set.of(C)
        );
        var big = view(
            weights(A, 100.0, B, 10.0, C, 6.0), Set.of(A),
            members("big", 4.0, "small", 1.0),
            members("big", 10.0, "small", 6.0),
            "big", Set.of(B)
        );

        assertInstanceOf(Balancer.Decision.Hold.class, Balancer.decide(small, 0, CONFIG));
        assertInstanceOf(Balancer.Decision.Claim.class, Balancer.decide(big, 0, CONFIG));
    }

    @Test
    void aMemberAtItsFairShareClaimsNothingMore() {
        var decision = Balancer.decide(view(
            weights(A, 10.0, B, 10.0), Set.of(B),
            members("s1", 1.0, "s2", 1.0), members("s1", 10.0, "s2", 0.0), "s1", Set.of(A)
        ), 0, CONFIG);

        // s2 exists and is idle, so s1 leaves B for it rather than hoarding.
        assertInstanceOf(Balancer.Decision.Hold.class, decision);
    }

    @Test
    void capacityWeightGivesABiggerMachineMorePools() {
        // Configured rather than measured, so nothing has to trust a measurement loop's output.
        var view = view(
            weights(A, 10.0, B, 10.0, C, 10.0), Set.of(C),
            members("big", 3.0, "small", 1.0), members("big", 20.0, "small", 10.0), "big", Set.of(A, B)
        );

        // Fair share for big is 30 x 3/4 = 22.5, and it holds 20, so C fits.
        assertInstanceOf(Balancer.Decision.Claim.class, Balancer.decide(view, 0, CONFIG));
    }

    // ---------------------------------------------------------------- releasing

    @Test
    void aMemberOverShareHoldsUntilItHasBeenOverShareForALongTime() {
        // Hysteresis plus a sustained count is what stops the characteristic failure of every rebalancer:
        // two members handing one pool back and forth for ever.
        var over = view(
            weights(A, 10.0, B, 10.0, C, 10.0), Set.of(),
            members("s1", 1.0, "s2", 1.0), members("s1", 30.0, "s2", 0.0), "s1", Set.of(A, B, C)
        );

        assertInstanceOf(Balancer.Decision.Hold.class, Balancer.decide(over, 0, CONFIG));
        assertInstanceOf(Balancer.Decision.Hold.class, Balancer.decide(over, CONFIG.releaseAfterPasses() - 1, CONFIG));
        assertInstanceOf(Balancer.Decision.Release.class, Balancer.decide(over, CONFIG.releaseAfterPasses(), CONFIG));
    }

    @Test
    void releasesTheLightestPoolSoTheMoveIsSmall() {
        var decision = Balancer.decide(view(
            weights(A, 30.0, B, 3.0), Set.of(),
            members("s1", 1.0, "s2", 1.0), members("s1", 33.0, "s2", 0.0), "s1", Set.of(A, B)
        ), CONFIG.releaseAfterPasses(), CONFIG);

        assertEquals(B, ((Balancer.Decision.Release) decision).pool());
    }

    @Test
    void justOverFairShareIsInsideTheHysteresisBandAndIsNotWorthMoving() {
        // Rebalancing throws away a warm revision cache and costs a full re-observation, so a small
        // imbalance is cheaper to keep than to fix.
        var decision = Balancer.decide(view(
            weights(A, 11.0, B, 9.0), Set.of(),
            members("s1", 1.0, "s2", 1.0), members("s1", 11.0, "s2", 9.0), "s1", Set.of(A)
        ), 1000, CONFIG);

        assertInstanceOf(Balancer.Decision.Hold.class, decision);
    }

    @Test
    void theSoleMemberNeverReleasesAnything() {
        // Releasing would leave the pool unowned, which is strictly worse than one member being overloaded.
        var decision = Balancer.decide(view(
            weights(A, 100.0, B, 100.0), Set.of(),
            members("s1", 1.0), members("s1", 200.0), "s1", Set.of(A, B)
        ), 10_000, CONFIG);

        assertInstanceOf(Balancer.Decision.Hold.class, decision);
        assertTrue(((Balancer.Decision.Hold) decision).because().contains("sole live member"));
    }

    // ---------------------------------------------------------------- properties

    @Test
    void anOverloadedMemberLeavesAnUnownedPoolToAnIdlePeerAndTheFleetFixesBothAtOnce() {
        // Worth reading twice, because the obvious expectation is wrong. An overloaded member does *not*
        // grab the unowned pool "because unowned is worse than unbalanced": an idle peer exists and will
        // take it, so grabbing it would be hoarding. Each member makes a different decision from the same
        // figures, and one pass fixes both problems.
        var overloaded = view(
            weights(A, 10.0, B, 10.0, C, 1.0), Set.of(C),
            members("s1", 1.0, "s2", 1.0), members("s1", 20.0, "s2", 0.0), "s1", Set.of(A, B)
        );
        var idle = view(
            weights(A, 10.0, B, 10.0, C, 1.0), Set.of(C),
            members("s1", 1.0, "s2", 1.0), members("s1", 20.0, "s2", 0.0), "s2", Set.of()
        );

        assertInstanceOf(Balancer.Decision.Release.class, Balancer.decide(overloaded, 10_000, CONFIG));
        assertEquals(C, ((Balancer.Decision.Claim) Balancer.decide(idle, 0, CONFIG)).pool());
    }

    @Test
    void beingOverShareAndBeingLeastLoadedAreMutuallyExclusive() {
        // Which means the claim-before-release ordering in decide() can never actually fire, and is
        // defensive rather than load-bearing. Recorded because it is not obvious and because someone will
        // otherwise try to write a test for that precedence and conclude the code is wrong.
        //
        // The algebra: over share means weightHeld/capacity > 1.25 x total/totalCapacity, while least
        // loaded means weightHeld/capacity is the minimum. The capacity-weighted mean of weightHeld/capacity
        // is totalHeld/totalCapacity, which is at most total/totalCapacity -- so a minimum cannot exceed
        // 1.25 times it. Checked here over a spread of shapes rather than only argued.
        for (double myCapacity : new double[] { 0.5, 1.0, 4.0 }) {
            for (double myWeight : new double[] { 0.0, 5.0, 50.0 }) {
                for (double peerWeight : new double[] { 0.0, 5.0, 50.0 }) {
                    var view = view(
                        weights(A, myWeight <= 0 ? 1.0 : myWeight, B, peerWeight <= 0 ? 1.0 : peerWeight),
                        Set.of(),
                        members("me", myCapacity, "peer", 1.0),
                        members("me", myWeight, "peer", peerWeight),
                        "me",
                        myWeight > 0 ? Set.of(A) : Set.of()
                    );
                    boolean overShare = myWeight > Balancer.fairShareOf(view, CONFIG) * (1 + CONFIG.hysteresis());
                    boolean leastLoaded = myWeight / myCapacity <= peerWeight;
                    assertFalse(
                        overShare && leastLoaded,
                        "capacity " + myCapacity + " holding " + myWeight + " against peer " + peerWeight
                    );
                }
            }
        }
    }

    @Test
    void everyDecisionExplainsItself() {
        // R7.4 applies here as much as to findings: "released pool-b" is not debuggable, and the reason is
        // the only thing that tells an operator whether the balancer is behaving.
        var decisions = new java.util.ArrayList<Balancer.Decision>();
        decisions.add(Balancer.decide(view(weights(A, 1.0), Set.of(A), members("s1", 1.0), members("s1", 0.0), "s1", Set.of()), 0, CONFIG));
        decisions.add(Balancer.decide(view(weights(A, 1.0), Set.of(), members("s1", 1.0), members("s1", 1.0), "s1", Set.of(A)), 0, CONFIG));
        decisions.add(Balancer.decide(view(
            weights(A, 10.0, B, 10.0), Set.of(), members("s1", 1.0, "s2", 1.0),
            members("s1", 20.0, "s2", 0.0), "s1", Set.of(A, B)
        ), 10_000, CONFIG));

        for (var decision : decisions) {
            String because = switch (decision) {
                case Balancer.Decision.Claim claim -> claim.because();
                case Balancer.Decision.Release release -> release.because();
                case Balancer.Decision.Hold hold -> hold.because();
            };
            assertFalse(because.isBlank(), decision + " has no reason");
        }
    }

    @Test
    void aPoolWithNoPublishedWeightGetsTheDefault() {
        // An unowned pool publishes nothing, because its weight comes from its owner's convergence document.
        // Underestimating it is the right direction to be wrong in: it makes someone more likely to claim it.
        var decision = Balancer.decide(view(
            weights(), Set.of(A), members("s1", 1.0), members("s1", 0.0), "s1", Set.of()
        ), 0, CONFIG);

        assertInstanceOf(Balancer.Decision.Claim.class, decision);
    }

    @Test
    void decidingIsDeterministicAndFreeOfIterationOrder() {
        // Every member runs this over its own view. If the answer depended on hash order, two members with
        // identical data could disagree for ever rather than converging.
        Set<PoolId> forward = new LinkedHashSet<>(java.util.List.of(A, B, C));
        Set<PoolId> reverse = new LinkedHashSet<>(java.util.List.of(C, B, A));
        var weights = weights(A, 5.0, B, 5.0, C, 5.0);

        var first = Balancer.decide(view(weights, forward, members("s1", 1.0), members("s1", 0.0), "s1", Set.of()), 0, CONFIG);
        var second = Balancer.decide(view(weights, reverse, members("s1", 1.0), members("s1", 0.0), "s1", Set.of()), 0, CONFIG);

        assertEquals(((Balancer.Decision.Claim) first).pool(), ((Balancer.Decision.Claim) second).pool());
    }

    @Test
    void fairShareOfNothingIsNothing() {
        var view = view(weights(), Set.of(), members("s1", 1.0), members("s1", 0.0), "s1", Set.of());
        assertEquals(0.0, Balancer.fairShareOf(view, CONFIG), 0.0001);
        assertInstanceOf(Balancer.Decision.Hold.class, Balancer.decide(view, 0, CONFIG));
    }

    @Test
    void unownedPoolsAreThoseNobodyClaimsToHold() {
        assertEquals(
            Set.of(C),
            Balancer.unowned(Set.of(A, B, C), Map.of("s1", Set.of(A), "s2", Set.of(B)))
        );
    }
}
