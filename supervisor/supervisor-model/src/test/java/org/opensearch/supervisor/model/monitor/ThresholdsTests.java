package org.opensearch.supervisor.model.monitor;

import org.junit.jupiter.api.Test;
import org.opensearch.supervisor.model.diff.FindingKind;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The threshold table, which is the artifact rather than the code around it. */
class ThresholdsTests {

    @Test
    void everyFindingKindHasADecision() {
        // The test that stops a new finding kind going unjudged. Adding one to FindingKind without deciding
        // whether it is worth waking someone for is the easy mistake -- the supervisor would raise it, this
        // monitor would say UNKNOWN_FINDING_KIND at INFO, and nobody would notice for a year. NEVER is a
        // perfectly good answer here; silence is not.
        List<FindingKind> undecided = new ArrayList<>();
        for (FindingKind kind : FindingKind.values()) {
            if (Thresholds.declared().containsKey(kind) == false) {
                undecided.add(kind);
            }
        }
        assertEquals(List.of(), undecided,
            "each of these needs a row in Thresholds -- a threshold, or NEVER with the reason why not");
    }

    @Test
    void aFindingYoungerThanItsThresholdIsNotAnAlert() {
        assertTrue(Thresholds.firesAt("UNASSIGNED_PRIMARY", 29).isEmpty());
        assertEquals(Alert.Severity.CRITICAL, Thresholds.firesAt("UNASSIGNED_PRIMARY", 30).orElseThrow());
    }

    @Test
    void tornDesiredStateNeverFiresHoweverOldItIs() {
        // R2.5 promises the supervisor will see an index without its alias. Alerting on it is the classic
        // mistake, and the differ already escalates the kind by age on its own.
        assertTrue(Thresholds.firesAt("TORN_DESIRED_STATE", 100_000).isEmpty());
        assertTrue(Thresholds.knows("TORN_DESIRED_STATE"), "known, and deliberately silent, are different things");
    }

    @Test
    void aKindThisBuildHasNeverHeardOfIsNotGuessedAt() {
        // A supervisor newer than this binary. Inventing a threshold for a finding whose definition this build
        // does not have would be alerting on a guess; Rules reports the skew instead.
        assertTrue(Thresholds.firesAt("SOMETHING_FROM_THE_FUTURE", 500).isEmpty());
        assertTrue(Thresholds.knows("SOMETHING_FROM_THE_FUTURE") == false);
    }

    @Test
    void anAlertsSeverityIsNotNecessarilyItsFindingsSeverity() {
        // NODE_LOST is CRITICAL as a finding -- a node's copies have stopped counting -- and a WARNING as an
        // alert, because the supervisor is already replacing them and only its persistence needs a human.
        // NODE_UNREGISTERED is the reverse. Conflating the two scales is how an alerter earns its noise.
        assertEquals(org.opensearch.supervisor.model.diff.Severity.CRITICAL, FindingKind.NODE_LOST.base());
        assertEquals(Alert.Severity.WARNING, Thresholds.firesAt("NODE_LOST", 10).orElseThrow());

        assertEquals(org.opensearch.supervisor.model.diff.Severity.WARNING, FindingKind.NODE_UNREGISTERED.base());
        assertEquals(Alert.Severity.CRITICAL, Thresholds.firesAt("NODE_UNREGISTERED", 2).orElseThrow());
    }
}
